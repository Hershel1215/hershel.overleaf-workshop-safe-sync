import { createHash, randomUUID } from 'crypto';
import { lstat, open, readFile, readdir, rename, rm, stat, unlink } from 'fs/promises';
import * as path from 'path';
import { ProjectionReceipt } from './projectionTracker';
import { SerializedAiWriterLeaseState } from './aiWriterLease';
import { DurableWal, WalCorruptionError, WalRecord } from './durableWal';
import { createDirectoryDurably, syncDirectoryDurably, writeFileDurably } from './durableFile';

export type BaselineFileType = 'doc' | 'file' | 'folder';

export interface BaselineEntry {
    entityId: string;
    fileType: BaselineFileType;
    hash: string;
    text?: string;
    treeHash?: string;
    localIdentity?: string;
}

export interface AiCommitReceipt {
    opId?: string;
    path: string;
    content: string;
    journaled: true;
    remoteAcknowledged: false;
}

export interface CompletedAiCommit {
    leaseId: string;
    fencingToken: number;
    path: string;
    entityId: string;
    targetHash: string;
    receiptContentHash: string;
    receipt: Omit<AiCommitReceipt, 'content'>;
    completedAt: string;
}

interface LegacyCompletedAiCommit {
    leaseId: string;
    fencingToken: number;
    path: string;
    entityId: string;
    targetContent: string;
    receipt: AiCommitReceipt;
    completedAt: string;
}

type PersistedCompletedAiCommit = CompletedAiCommit | LegacyCompletedAiCommit;

/** Durable admission record spanning AI commit acceptance and receipt publication. */
export interface AiCommitIntent {
    leaseId: string;
    fencingToken: number;
    path: string;
    entityId: string;
    baseHash: string;
    targetHash: string;
    operationId: string;
    createdAt: string;
}

export const MAX_COMPLETED_AI_COMMITS = 128;
export const MAX_REPLICA_STATE_WAL_RECORDS = 32;
export const MAX_REPLICA_STATE_WAL_BYTES = 4 * 1024 * 1024;

export interface ConflictDocumentRecovery {
    entityId: string;
    visibleFile?: string;
    acknowledgedFile?: string;
    visibleHash: string;
    acknowledgedHash: string;
    revision: number;
    pendingOperationIds: string[];
    epoch: number;
    snapshotHash: string;
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
    baseType?: BaselineFileType;
    localType?: BaselineFileType;
    remoteType?: BaselineFileType;
    baselineEntityId?: string;
    remoteEntityId?: string;
    remoteRevision?: number;
    projectionTargetHash?: string;
    projectionRecoveryTransactionId?: string;
    projectionRecoveryArchivedTransactionId?: string;
    projectionRecoveryArchivedHash?: string;
    retainedRemoteFile?: string;
    retainedRemoteHash?: string;
    retainedRemoteEntityId?: string;
    retainedRemoteType?: BaselineFileType;
    documentRecoveries?: ConflictDocumentRecovery[];
}

export interface DetachedDocumentRecovery {
    documentId: string;
    artifactDirectory: string;
    reason: string;
    createdAt: string;
    visibleFile: string;
    acknowledgedFile: string;
    visibleHash: string;
    acknowledgedHash: string;
    revision: number;
    pendingOperationIds: string[];
    epoch: number;
    snapshotHash: string;
    walRetired: boolean;
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
    expectedLocalTreeHash?: string;
    expectedLocalIdentity?: string;
    expectedRemoteHash?: string;
    expectedRemoteTreeHash?: string;
    expectedRemoteStructureHash?: string;
    targetHash?: string;
    entityId?: string;
    /** Fixed id and replay inputs for a document operation spanning a crash. */
    documentOperationId?: string;
    documentOperationPhase?: 'prepared' | 'accepted' | 'settled';
    documentAncestorHash?: string;
    documentRequestedTargetHash?: string;
}

export interface ReplicaState {
    schemaVersion: 1;
    replicaId: string;
    initialized: boolean;
    updatedAt: string;
    entries: Record<string, BaselineEntry>;
    conflicts: Record<string, ConflictRecord>;
    detachedDocumentRecoveries?: Record<string, DetachedDocumentRecovery>;
    projectionReceipts?: ProjectionReceipt[];
    aiLeaseState?: SerializedAiWriterLeaseState;
    aiCommitIntents?: Record<string, AiCommitIntent>;
    aiCompletedCommits?: Record<string, PersistedCompletedAiCommit>;
    pendingAction?: PendingReplicaAction;
}

const BASELINE_HASH_PATTERN = /^[a-f0-9]{64}$/;
const BASELINE_TEMP_PATTERN = /^[a-f0-9]{64}\.[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.tmp$/;

interface ProjectionReceiptEnvelope {
    schemaVersion: 1;
    receipts: ProjectionReceipt[];
    hash: string;
}

/** Content-addressed blobs required to recover the supplied durable state. */
export function referencedBaselineHashes(state: ReplicaState): Set<string> {
    const referenced = new Set<string>();
    for (const entry of Object.values(state.entries)) {
        if (entry.fileType === 'file' && BASELINE_HASH_PATTERN.test(entry.hash)) {
            referenced.add(entry.hash);
        }
    }
    const pending = state.pendingAction;
    for (const hash of [
        pending?.documentAncestorHash,
        pending?.documentRequestedTargetHash,
    ]) {
        if (hash !== undefined && BASELINE_HASH_PATTERN.test(hash)) {
            referenced.add(hash);
        }
    }
    for (const intent of Object.values(state.aiCommitIntents ?? {})) {
        referenced.add(intent.baseHash);
        referenced.add(intent.targetHash);
    }
    for (const completed of Object.values(state.aiCompletedCommits ?? {})) {
        if (!isLegacyCompletedAiCommit(completed)) {
            referenced.add(completed.targetHash);
            referenced.add(completed.receiptContentHash);
        }
    }
    return referenced;
}

/** Retain enough durable idempotency keys for delayed AI gateway retries. */
export function recordCompletedAiCommit(
    state: ReplicaState,
    completed: CompletedAiCommit,
): void {
    const retained = {...(state.aiCompletedCommits ?? {})};
    retained[completed.leaseId] = JSON.parse(JSON.stringify(completed)) as CompletedAiCommit;
    const expired = Object.values(retained)
        // The receipt being published is the only retry proof after its
        // intent is removed. Clock rollback must not make that new proof look
        // old enough to prune in the same atomic transition.
        .filter(entry => entry.leaseId !== completed.leaseId)
        .sort((left, right) => (
            left.completedAt.localeCompare(right.completedAt)
            || left.leaseId.localeCompare(right.leaseId)
        ))
        .slice(0, Math.max(0, Object.keys(retained).length - MAX_COMPLETED_AI_COMMITS));
    for (const entry of expired) {
        delete retained[entry.leaseId];
    }
    state.aiCompletedCommits = retained;
}

/** Durable baseline and conflict index for one local replica. */
export class ReplicaStateStore {
    readonly statePath: string;
    readonly projectionStatePath: string;
    private readonly walPath: string;
    private readonly baselineDirectory: string;
    private wal?: DurableWal<ReplicaState>;
    private lastStablePayload?: string;
    private lastStableProjectionPayload?: string;
    private lastDurableState?: ReplicaState;
    private saveChain: Promise<void> = Promise.resolve();
    private baselineMutationChain: Promise<void> = Promise.resolve();
    private readonly uncommittedBaselineReservations = new Map<string, number>();

    constructor(rootDirectory: string, serverName: string, projectId: string, basePath: string) {
        const key = createHash('sha256')
            .update(`${serverName}\0${projectId}\0${path.resolve(basePath)}`)
            .digest('hex');
        this.statePath = path.join(rootDirectory, 'safe-sync', 'replicas', `${key}.json`);
        this.projectionStatePath = path.join(
            rootDirectory,
            'safe-sync',
            'replicas',
            `${key}.projections.json`,
        );
        this.walPath = path.join(rootDirectory, 'safe-sync', 'replicas', `${key}.wal`);
        this.baselineDirectory = path.join(rootDirectory, 'safe-sync', 'baselines', key);
    }

    async load(): Promise<ReplicaState> {
        try {
            this.wal ??= await DurableWal.open<ReplicaState>(this.walPath);
        } catch (error) {
            if (!(error instanceof WalCorruptionError)) {
                throw error;
            }
            return this.recoverPublishedFullSnapshot(error);
        }
        const walState = this.wal.records.at(-1)?.payload;
        if (walState) {
            return this.acceptLoadedState(walState);
        }
        try {
            const parsed = JSON.parse(await readFile(this.statePath, 'utf8')) as ReplicaState;
            return this.acceptLoadedState(parsed);
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
            this.lastDurableState = cloneReplicaState(initial);
            return initial;
        }
    }

    /** Read the atomically published snapshot without opening or repairing the live WAL. */
    async reload(): Promise<ReplicaState> {
        await this.saveChain;
        const parsed = JSON.parse(await readFile(this.statePath, 'utf8')) as ReplicaState;
        return this.mergeProjectionReceipts(parsed);
    }

    /**
     * A full-snapshot WAL can safely be retired after chain damage only when
     * its independently checksummed final record is byte-for-byte represented
     * by the already durable published snapshot. The caller owns the replica
     * lock; any mismatch remains a fail-closed corruption error.
     */
    private async recoverPublishedFullSnapshot(
        corruption: WalCorruptionError,
    ): Promise<ReplicaState> {
        let published: ReplicaState;
        let encodedWal: string;
        try {
            const [encodedState, wal] = await Promise.all([
                readFile(this.statePath, 'utf8'),
                readFile(this.walPath, 'utf8'),
            ]);
            published = JSON.parse(encodedState) as ReplicaState;
            this.validate(published);
            encodedWal = wal;
        } catch {
            throw corruption;
        }
        const tail = validateStandaloneWalTail(encodedWal);
        if (!tail || stablePayload(tail.payload) !== stablePayload(published)) {
            throw corruption;
        }
        // Refuse a repair if the path advanced after the proof was collected.
        if (await readFile(this.walPath, 'utf8') !== encodedWal) {
            throw new WalCorruptionError('Replica state WAL changed during recovery proof');
        }
        await DurableWal.replaceWithEmpty(this.walPath);
        this.wal = await DurableWal.open<ReplicaState>(this.walPath);
        return this.acceptLoadedState(published);
    }

    private async acceptLoadedState(state: ReplicaState): Promise<ReplicaState> {
        const merged = await this.mergeProjectionReceipts(state);
        const durableState = withoutProjectionReceipts(state);
        this.lastStablePayload = stablePayload(durableState);
        this.lastDurableState = cloneReplicaState(durableState);
        return merged;
    }

    private async mergeProjectionReceipts(state: ReplicaState): Promise<ReplicaState> {
        this.validate(state);
        const receipts = await this.readProjectionReceipts(state.projectionReceipts ?? []);
        const merged = withoutProjectionReceipts(state);
        if (receipts.length > 0) {
            merged.projectionReceipts = receipts;
        }
        return merged;
    }

    private async readProjectionReceipts(
        legacyReceipts: readonly ProjectionReceipt[],
    ): Promise<ProjectionReceipt[]> {
        validateProjectionReceipts(legacyReceipts);
        let encoded: string;
        try {
            encoded = await readFile(this.projectionStatePath, 'utf8');
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                this.lastStableProjectionPayload = undefined;
                return cloneProjectionReceipts(legacyReceipts);
            }
            throw error;
        }
        const envelope = JSON.parse(encoded) as ProjectionReceiptEnvelope;
        const {hash, ...body} = envelope;
        if (
            body.schemaVersion !== 1
            || hash !== createHash('sha256').update(JSON.stringify(body)).digest('hex')
        ) {
            throw new WalCorruptionError('Projection receipt snapshot checksum is invalid');
        }
        validateProjectionReceipts(body.receipts);
        this.lastStableProjectionPayload = stableProjectionPayload(body.receipts);
        return cloneProjectionReceipts(body.receipts);
    }

    private async saveProjectionReceipts(receipts: readonly ProjectionReceipt[]): Promise<void> {
        validateProjectionReceipts(receipts);
        const stable = stableProjectionPayload(receipts);
        if (stable === this.lastStableProjectionPayload) {
            return;
        }
        const body = {
            schemaVersion: 1 as const,
            receipts: cloneProjectionReceipts(receipts),
        };
        const envelope: ProjectionReceiptEnvelope = {
            ...body,
            hash: createHash('sha256').update(JSON.stringify(body)).digest('hex'),
        };
        await writeFileDurably(
            this.projectionStatePath,
            new TextEncoder().encode(JSON.stringify(envelope)),
        );
        this.lastStableProjectionPayload = stable;
    }

    async save(state: ReplicaState): Promise<void> {
        // Checkpoint replacement and WAL append must share one ordering. A
        // projection receipt from one document must not overwrite a receipt
        // or conflict record committed by another document concurrently.
        const snapshot = cloneReplicaState(state);
        const write = this.saveChain.then(
            () => this.saveInternal(snapshot),
            () => this.saveInternal(snapshot),
        );
        this.saveChain = write.then(() => undefined, () => undefined);
        return write;
    }

    private async saveInternal(state: ReplicaState): Promise<void> {
        this.validate(state);
        await this.saveProjectionReceipts(state.projectionReceipts ?? []);
        state = withoutProjectionReceipts(state);
        this.wal ??= await DurableWal.open<ReplicaState>(this.walPath);
        const directory = path.dirname(this.statePath);
        await createDirectoryDurably(directory, 0o700);
        const stable = stablePayload(state);
        if (stable === this.lastStablePayload) {
            await this.finishDurableStateTransition(state);
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
        if (
            this.wal.records.length >= MAX_REPLICA_STATE_WAL_RECORDS
            || this.wal.byteLength >= MAX_REPLICA_STATE_WAL_BYTES
        ) {
            await this.resetWalAfterDurableSnapshot();
        }
        await this.finishDurableStateTransition(payload);
    }

    /**
     * The full-snapshot WAL is needed only until statePath is durably
     * published. Replace it atomically afterwards so recovery always sees
     * either the complete old WAL or an empty WAL plus the durable snapshot.
     */
    private async resetWalAfterDurableSnapshot(): Promise<void> {
        const wal = this.wal;
        if (!wal) {
            return;
        }
        this.wal = undefined;
        await wal.close();
        await DurableWal.replaceWithEmpty(this.walPath);
        this.wal = await DurableWal.open<ReplicaState>(this.walPath);
    }

    private runBaselineMutation<T>(action: () => Promise<T>): Promise<T> {
        const operation = this.baselineMutationChain.then(action, action);
        this.baselineMutationChain = operation.then(() => undefined, () => undefined);
        return operation;
    }

    private reserveBaseline(hash: string): void {
        this.uncommittedBaselineReservations.set(
            hash,
            (this.uncommittedBaselineReservations.get(hash) ?? 0) + 1,
        );
    }

    private releaseBaselineReservation(hash: string): void {
        const retained = (this.uncommittedBaselineReservations.get(hash) ?? 0) - 1;
        if (retained > 0) {
            this.uncommittedBaselineReservations.set(hash, retained);
        } else {
            this.uncommittedBaselineReservations.delete(hash);
        }
    }

    private isBaselineReserved(hash: string): boolean {
        return (this.uncommittedBaselineReservations.get(hash) ?? 0) > 0;
    }

    private async finishDurableStateTransition(state: ReplicaState): Promise<void> {
        const previous = this.lastDurableState;
        const currentHashes = referencedBaselineHashes(state);
        for (const hash of currentHashes) {
            this.releaseBaselineReservation(hash);
        }
        this.lastDurableState = cloneReplicaState(state);
        if (!previous) {
            return;
        }
        const obsolete = [...referencedBaselineHashes(previous)].filter(hash => (
            !currentHashes.has(hash)
            && !this.isBaselineReserved(hash)
            && BASELINE_HASH_PATTERN.test(hash)
        ));
        if (obsolete.length === 0) {
            return;
        }
        try {
            await this.runBaselineMutation(async () => {
                let removed = 0;
                for (const hash of obsolete) {
                    if (this.isBaselineReserved(hash)) {
                        continue;
                    }
                    const candidate = path.join(this.baselineDirectory, hash);
                    try {
                        if (!(await lstat(candidate)).isFile()) {
                            continue;
                        }
                        if (this.isBaselineReserved(hash)) {
                            continue;
                        }
                        await unlink(candidate);
                        removed++;
                    } catch (error) {
                        if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                            continue;
                        }
                        throw error;
                    }
                }
                if (removed > 0) {
                    await syncDirectoryDurably(this.baselineDirectory);
                }
            });
        } catch (error) {
            // The new replica state is already durable. Orphan cleanup is
            // retried by the next startup sweep and must not pause syncing.
            console.warn(
                '[safe-sync] deferred baseline cleanup:',
                error instanceof Error ? error.message : String(error),
            );
        }
    }

    async close(): Promise<void> {
        await this.saveChain;
        await this.baselineMutationChain;
        await this.wal?.close();
        this.wal = undefined;
    }

    /** Persist one content-addressed common-base snapshot outside the project tree. */
    async saveBaseline(content: Uint8Array): Promise<string> {
        const hash = createHash('sha256').update(content).digest('hex');
        // Reserve before the first filesystem yield. A concurrent durable
        // state save must not reclaim this blob before its reference commits.
        this.reserveBaseline(hash);
        try {
            return await this.runBaselineMutation(async () => {
                await createDirectoryDurably(this.baselineDirectory, 0o700);
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
                await syncDirectoryDurably(this.baselineDirectory);
                return hash;
            });
        } catch (error) {
            this.releaseBaselineReservation(hash);
            throw error;
        }
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

    /** Replace legacy inline AI receipt text with content-addressed references. */
    async normalizeCompletedAiCommits(state: ReplicaState): Promise<number> {
        const previous = state.aiCompletedCommits;
        const normalized = {...(state.aiCompletedCommits ?? {})};
        let migrated = 0;
        for (const [leaseId, completed] of Object.entries(normalized)) {
            if (!isLegacyCompletedAiCommit(completed)) {
                continue;
            }
            const [targetHash, receiptContentHash] = await Promise.all([
                this.saveBaseline(new TextEncoder().encode(completed.targetContent)),
                this.saveBaseline(new TextEncoder().encode(completed.receipt.content)),
            ]);
            normalized[leaseId] = {
                leaseId: completed.leaseId,
                fencingToken: completed.fencingToken,
                path: completed.path,
                entityId: completed.entityId,
                targetHash,
                receiptContentHash,
                receipt: {
                    ...(completed.receipt.opId === undefined
                        ? {}
                        : {opId: completed.receipt.opId}),
                    path: completed.receipt.path,
                    journaled: true,
                    remoteAcknowledged: false,
                },
                completedAt: completed.completedAt,
            };
            migrated++;
        }
        if (migrated === 0) {
            return 0;
        }
        state.aiCompletedCommits = normalized;
        try {
            await this.save(state);
        } catch (error) {
            state.aiCompletedCommits = previous;
            throw error;
        }
        return migrated;
    }

    /**
     * Remove unreferenced content-addressed baselines during startup recovery.
     *
     * The caller must hold the replica lock and invoke this only after loading
     * state but before starting any watcher or gateway writer. saveBaseline()
     * publishes content before save() publishes its reference, so collection
     * during normal operation could otherwise delete an in-flight baseline.
     */
    async garbageCollectBaselinesAtStartup(state: ReplicaState): Promise<number> {
        this.validate(state);
        const referenced = referencedBaselineHashes(state);

        const candidates = await readdir(this.baselineDirectory, {withFileTypes: true})
            .catch(error => {
                if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                    return [];
                }
                throw error;
            });
        let removed = 0;
        for (const candidate of candidates) {
            const isContentBlob = BASELINE_HASH_PATTERN.test(candidate.name);
            const isInterruptedWrite = BASELINE_TEMP_PATTERN.test(candidate.name);
            // Preserve unknown formats, links, and directories. A strict
            // saveBaseline() temporary name cannot be referenced by state and
            // is safe to collect while the startup writer gate is closed.
            if (
                !candidate.isFile()
                || (!isContentBlob && !isInterruptedWrite)
                || (isContentBlob && referenced.has(candidate.name))
            ) {
                continue;
            }
            try {
                await unlink(path.join(this.baselineDirectory, candidate.name));
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                    continue;
                }
                throw error;
            }
            removed++;
        }
        if (removed > 0) {
            // One metadata barrier is sufficient: a crash before it can only
            // leave already-unreferenced blobs for the next startup sweep.
            await syncDirectoryDurably(this.baselineDirectory);
        }
        return removed;
    }

    private validate(state: ReplicaState): void {
        if (state.schemaVersion !== 1) {
            throw new Error(`Unsupported replica state schema: ${state.schemaVersion}`);
        }
        if (!state.replicaId || typeof state.entries !== 'object' || typeof state.conflicts !== 'object') {
            throw new Error('Replica state shape is invalid');
        }
        for (const [documentId, recovery] of Object.entries(
            state.detachedDocumentRecoveries ?? {},
        )) {
            if (
                documentId !== recovery.documentId
                || typeof recovery.artifactDirectory !== 'string'
                || !/^\.overleaf\/conflicts\/(?:unresolved|resolved)\//
                    .test(recovery.artifactDirectory)
                || typeof recovery.visibleFile !== 'string'
                || typeof recovery.acknowledgedFile !== 'string'
                || !/^[a-f0-9]{64}$/.test(recovery.visibleHash)
                || !/^[a-f0-9]{64}$/.test(recovery.acknowledgedHash)
                || !Array.isArray(recovery.pendingOperationIds)
                || recovery.pendingOperationIds.some(id => typeof id !== 'string' || id.length === 0)
                || !Number.isSafeInteger(recovery.revision)
                || recovery.revision < 0
                || !Number.isSafeInteger(recovery.epoch)
                || recovery.epoch < 0
                || !/^[a-f0-9]{64}$/.test(recovery.snapshotHash)
                || typeof recovery.walRetired !== 'boolean'
            ) {
                throw new Error(`Invalid detached document recovery: ${documentId}`);
            }
        }
        for (const [leaseId, completed] of Object.entries(state.aiCompletedCommits ?? {})) {
            const commonInvalid = (
                leaseId !== completed.leaseId
                || !Number.isSafeInteger(completed.fencingToken)
                || completed.fencingToken < 1
                || typeof completed.path !== 'string'
                || typeof completed.entityId !== 'string'
                || typeof completed.completedAt !== 'string'
                || completed.receipt?.path !== completed.path
                || completed.receipt.journaled !== true
                || completed.receipt.remoteAcknowledged !== false
                || (
                    completed.receipt.opId !== undefined
                    && typeof completed.receipt.opId !== 'string'
                )
            );
            const contentInvalid = isLegacyCompletedAiCommit(completed)
                ? typeof completed.targetContent !== 'string'
                    || typeof completed.receipt.content !== 'string'
                : !/^[a-f0-9]{64}$/.test(completed.targetHash)
                    || !/^[a-f0-9]{64}$/.test(completed.receiptContentHash)
                    || 'content' in completed.receipt;
            if (commonInvalid || contentInvalid) {
                throw new Error(`Invalid completed AI commit receipt: ${leaseId}`);
            }
        }
        const intentDocumentIds = new Set<string>();
        for (const [leaseId, intent] of Object.entries(state.aiCommitIntents ?? {})) {
            if (
                leaseId !== intent.leaseId
                || !Number.isSafeInteger(intent.fencingToken)
                || intent.fencingToken < 1
                || typeof intent.path !== 'string'
                || typeof intent.entityId !== 'string'
                || !/^[a-f0-9]{64}$/.test(intent.baseHash)
                || !/^[a-f0-9]{64}$/.test(intent.targetHash)
                || typeof intent.operationId !== 'string'
                || intent.operationId.length === 0
                || typeof intent.createdAt !== 'string'
                || state.aiCompletedCommits?.[leaseId] !== undefined
                || intentDocumentIds.has(intent.entityId)
            ) {
                throw new Error(`Invalid pending AI commit intent: ${leaseId}`);
            }
            intentDocumentIds.add(intent.entityId);
        }
        const pending = state.pendingAction;
        const documentFields = pending && [
            pending.documentOperationId,
            pending.documentOperationPhase,
            pending.documentAncestorHash,
            pending.documentRequestedTargetHash,
        ];
        if (
            documentFields
            && documentFields.some(value => value !== undefined)
            && (
                typeof pending.documentOperationId !== 'string'
                || !['prepared', 'accepted', 'settled'].includes(pending.documentOperationPhase ?? '')
                || !/^[a-f0-9]{64}$/.test(pending.documentAncestorHash ?? '')
                || !/^[a-f0-9]{64}$/.test(pending.documentRequestedTargetHash ?? '')
                || typeof pending.entityId !== 'string'
            )
        ) {
            throw new Error(`Invalid pending document operation: ${pending.id}`);
        }
    }
}

function validateStandaloneWalTail(encoded: string): WalRecord<ReplicaState> | undefined {
    if (!encoded.endsWith('\n')) {
        return undefined;
    }
    const line = encoded.slice(0, -1).split('\n').at(-1);
    if (!line) {
        return undefined;
    }
    try {
        const record = JSON.parse(line) as WalRecord<ReplicaState>;
        const {hash, ...body} = record;
        if (
            body.schemaVersion !== 1
            || !Number.isSafeInteger(body.sequence)
            || body.sequence < 1
            || !/^[a-f0-9]{64}$/.test(body.previousHash)
            || !/^[a-f0-9]{64}$/.test(hash)
            || createHash('sha256').update(JSON.stringify(body)).digest('hex') !== hash
        ) {
            return undefined;
        }
        return record;
    } catch {
        return undefined;
    }
}

function validateProjectionReceipts(
    value: unknown,
): asserts value is ProjectionReceipt[] {
    if (!Array.isArray(value) || value.length > 4096) {
        throw new WalCorruptionError('Projection receipt snapshot is invalid');
    }
    const ids = new Set<string>();
    for (const receipt of value as ProjectionReceipt[]) {
        if (
            !receipt
            || typeof receipt.id !== 'string'
            || receipt.id.length === 0
            || ids.has(receipt.id)
            || typeof receipt.path !== 'string'
            || receipt.path.length === 0
            || !['create', 'change', 'delete'].includes(receipt.kind)
            || (
                receipt.expectedHash !== undefined
                && (typeof receipt.expectedHash !== 'string' || receipt.expectedHash.length === 0)
            )
            || (
                receipt.expectedIdentity !== undefined
                && (
                    typeof receipt.expectedIdentity !== 'string'
                    || receipt.expectedIdentity.length === 0
                )
            )
            || (
                (receipt.mergeBase === undefined) !== (receipt.mergeTarget === undefined)
            )
            || (
                receipt.mergeBase !== undefined
                && (
                    receipt.kind !== 'change'
                    || typeof receipt.mergeBase !== 'string'
                    || typeof receipt.mergeTarget !== 'string'
                )
            )
        ) {
            throw new WalCorruptionError('Projection receipt snapshot is invalid');
        }
        ids.add(receipt.id);
    }
}

function cloneProjectionReceipts(
    receipts: readonly ProjectionReceipt[],
): ProjectionReceipt[] {
    return receipts.map(receipt => ({...receipt}));
}

function stableProjectionPayload(receipts: readonly ProjectionReceipt[]): string {
    return JSON.stringify(receipts);
}

function withoutProjectionReceipts(state: ReplicaState): ReplicaState {
    const durable = cloneReplicaState(state);
    delete durable.projectionReceipts;
    return durable;
}

function stablePayload(state: ReplicaState): string {
    return JSON.stringify({...state, updatedAt: ''});
}

function cloneReplicaState(state: ReplicaState): ReplicaState {
    return JSON.parse(JSON.stringify(state)) as ReplicaState;
}

function isLegacyCompletedAiCommit(
    completed: PersistedCompletedAiCommit,
): completed is LegacyCompletedAiCommit {
    return 'targetContent' in completed;
}
