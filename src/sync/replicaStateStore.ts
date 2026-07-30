import { createHash, randomUUID } from 'crypto';
import { mkdir, open, readFile, rename, rm } from 'fs/promises';
import * as path from 'path';

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
    localHash?: string;
    remoteHash?: string;
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
    pendingAction?: PendingReplicaAction;
}

/** Durable baseline and conflict index for one local replica. */
export class ReplicaStateStore {
    readonly statePath: string;

    constructor(rootDirectory: string, serverName: string, projectId: string, basePath: string) {
        const key = createHash('sha256')
            .update(`${serverName}\0${projectId}\0${path.resolve(basePath)}`)
            .digest('hex');
        this.statePath = path.join(rootDirectory, 'safe-sync', 'replicas', `${key}.json`);
    }

    async load(): Promise<ReplicaState> {
        try {
            const parsed = JSON.parse(await readFile(this.statePath, 'utf8')) as ReplicaState;
            if (parsed.schemaVersion !== 1) {
                throw new Error(`Unsupported replica state schema: ${parsed.schemaVersion}`);
            }
            return parsed;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                throw error;
            }
            return {
                schemaVersion: 1,
                replicaId: randomUUID(),
                initialized: false,
                updatedAt: new Date().toISOString(),
                entries: {},
                conflicts: {},
            };
        }
    }

    async save(state: ReplicaState): Promise<void> {
        const directory = path.dirname(this.statePath);
        await mkdir(directory, { recursive: true, mode: 0o700 });
        const temporary = `${this.statePath}.${randomUUID()}.tmp`;
        const payload: ReplicaState = {
            ...state,
            updatedAt: new Date().toISOString(),
        };
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
    }
}
