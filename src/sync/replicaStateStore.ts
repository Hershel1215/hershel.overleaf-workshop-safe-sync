import { createHash, randomUUID } from 'crypto';
import { mkdir, open, readFile, rename, rm, stat } from 'fs/promises';
import * as path from 'path';
import { ProjectionReceipt } from './projectionTracker';
import { SerializedAiWriterLeaseState } from './aiWriterLease';
import { DurableWal } from './durableWal';

export type BaselineFileType = 'doc' | 'file' | 'folder';

export interface BaselineEntry {
    entityId: string;
    fileType: BaselineFileType;
    hash: string;
    text?: string;
    treeHash?: string;
    localIdentity?: string;
}

export interface ConflictRecord {
    path: string;
    artifactDirectory: string;
    reason: string;
    createdAt: string;
    localFile?: string;
    remoteFile?: string;
    baseFile?: string;
    baseHash?: string;
    localHash?: string;
    remoteHash?: string;
    baselineEntityId?: string;
    remoteEntityId?: string;
    remoteRevision?: number;
}

export type ReplicaActionKind =
    | 'pull'
    | 'push'
    | 'delete-local'
    | 'delete-remote'
    | 'move-local'
    | 'move-remote'
    | 'converge-local'
    | 'resolve-conflict';

export interface PendingReplicaAction {
    id: string;
    kind: ReplicaActionKind;
    path: string;
    targetPath?: string;
    startedAt: string;
    expectedLocalHash?: string;
    expectedRemoteHash?: string;
    targetHash?: string;
    entityId?: string;
}

export interface ReplicaState {
    schemaVersion: 1;
    replicaId: string;
    initialized: boolean;
    updatedAt: string;
    entries: Record<string, BaselineEntry>;
    conflicts: Record<string, ConflictRecord>;
    projectionReceipts?: ProjectionReceipt[];
    aiLeaseState?: SerializedAiWriterLeaseState;
    pendingAction?: PendingReplicaAction;
}

/** Durable baseline and conflict index for one local replica. */
export class ReplicaStateStore {
    readonly statePath: string;
    private readonly walPath: string;
    private readonly baselineDirectory: string;
    private wal?: DurableWal<ReplicaState>;
    private lastStablePayload?: string;
    private saveChain: Promise<void> = Promise.resolve();

    constructor(rootDirectory: string, serverName: string, projectId: string, basePath: string) {
        const key = createHash('sha256')
            .update(`${serverName}\0${projectId}\0${path.resolve(basePath)}`)
            .digest('hex');
        this.statePath = path.join(rootDirectory, 'safe-sync', 'replicas', `${key}.json`);
        this.walPath = path.join(rootDirectory, 'safe-sync', 'replicas', `${key}.wal`);
        this.baselineDirectory = path.join(rootDirectory, 'safe-sync', 'baselines', key);
    }

    async load(): Promise<ReplicaState> {
        this.wal ??= await DurableWal.open<ReplicaState>(this.walPath);
        const walState = this.wal.records.at(-1)?.payload;
        if (walState) {
            this.validate(walState);
            this.lastStablePayload = stablePayload(walState);
            return walState;
        }
        try {
            const parsed = JSON.parse(await readFile(this.statePath, 'utf8')) as ReplicaState;
            this.validate(parsed);
            this.lastStablePayload = stablePayload(parsed);
            return parsed;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                throw error;
            }
            const initial: ReplicaState = {
                schemaVersion: 1,
                replicaId: randomUUID(),
                initialized: false,
                updatedAt: new Date().toISOString(),
                entries: {},
                conflicts: {},
            };
            this.lastStablePayload = stablePayload(initial);
            return initial;
        }
    }

    async save(state: ReplicaState): Promise<void> {
        // Checkpoint replacement and WAL append must share one ordering. A
        // projection receipt from one document must not overwrite a receipt
        // or conflict record committed by another document concurrently.
        const snapshot = JSON.parse(JSON.stringify(state)) as ReplicaState;
        const write = this.saveChain.then(
            () => this.saveInternal(snapshot),
            () => this.saveInternal(snapshot),
        );
        this.saveChain = write.then(() => undefined, () => undefined);
        return write;
    }

    private async saveInternal(state: ReplicaState): Promise<void> {
        this.wal ??= await DurableWal.open<ReplicaState>(this.walPath);
        const directory = path.dirname(this.statePath);
        await mkdir(directory, { recursive: true, mode: 0o700 });
        const stable = stablePayload(state);
        if (stable === this.lastStablePayload) {
            return;
        }
        const temporary = `${this.statePath}.${randomUUID()}.tmp`;
        const payload: ReplicaState = {
            ...state,
            updatedAt: new Date().toISOString(),
        };
        await this.wal.append(payload, true);
        const handle = await open(temporary, 'w', 0o600);
        try {
            await handle.writeFile(JSON.stringify(payload));
            await handle.sync();
        } finally {
            await handle.close();
        }
        try {
            await rename(temporary, this.statePath);
        } catch (error) {
            const code = (error as NodeJS.ErrnoException).code;
            if (code !== 'EEXIST' && code !== 'EPERM' && code !== 'ENOTEMPTY') {
                throw error;
            }
            // Windows does not consistently replace an existing destination.
            await rm(this.statePath, { force: true });
            await rename(temporary, this.statePath);
        }
        try {
            const directoryHandle = await open(directory, 'r');
            try {
                await directoryHandle.sync();
            } finally {
                await directoryHandle.close();
            }
        } catch (error) {
            if (process.platform !== 'win32') {
                throw error;
            }
        }
        this.lastStablePayload = stablePayload(payload);
    }

    async close(): Promise<void> {
        await this.saveChain;
        await this.wal?.close();
        this.wal = undefined;
    }

    /** Persist one content-addressed common-base snapshot outside the project tree. */
    async saveBaseline(content: Uint8Array): Promise<string> {
        const hash = createHash('sha256').update(content).digest('hex');
        await mkdir(this.baselineDirectory, { recursive: true, mode: 0o700 });
        const destination = path.join(this.baselineDirectory, hash);
        try {
            await stat(destination);
            return hash;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                throw error;
            }
        }
        const temporary = `${destination}.${randomUUID()}.tmp`;
        const handle = await open(temporary, 'wx', 0o600);
        try {
            await handle.writeFile(content);
            await handle.sync();
        } finally {
            await handle.close();
        }
        try {
            await rename(temporary, destination);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
                throw error;
            }
            await rm(temporary, { force: true });
        }
        const directoryHandle = await open(this.baselineDirectory, 'r');
        try {
            await directoryHandle.sync();
        } finally {
            await directoryHandle.close();
        }
        return hash;
    }

    async readBaseline(hash: string): Promise<Uint8Array | undefined> {
        if (!/^[a-f0-9]{64}$/.test(hash)) {
            throw new Error(`Invalid safe-sync baseline hash: ${hash}`);
        }
        try {
            const content = await readFile(path.join(this.baselineDirectory, hash));
            if (createHash('sha256').update(content).digest('hex') !== hash) {
                throw new Error(`Stored safe-sync baseline is corrupt: ${hash}`);
            }
            return content;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                return undefined;
            }
            throw error;
        }
    }

    private validate(state: ReplicaState): void {
        if (state.schemaVersion !== 1) {
            throw new Error(`Unsupported replica state schema: ${state.schemaVersion}`);
        }
        if (!state.replicaId || typeof state.entries !== 'object' || typeof state.conflicts !== 'object') {
            throw new Error('Replica state shape is invalid');
        }
    }
}

function stablePayload(state: ReplicaState): string {
    return JSON.stringify({...state, updatedAt: ''});
}
