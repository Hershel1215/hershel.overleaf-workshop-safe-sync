import * as path from 'path';
import { access, readFile, readdir, rm } from 'fs/promises';
import { randomUUID } from 'crypto';
import {
    AtomicCheckpointStore,
    DurableWal,
    DurableWalOptions,
    WalCorruptionError,
} from './durableWal';
import {
    OtClientState,
    OutgoingOperation,
    ReceiveResult,
    SerializedOtClientState,
} from './otClientState';
import { TextComponent, TextOperation } from './textOperation';
import { syncDirectoryDurably, writeFileDurably } from './durableFile';

interface LocalOperationEvent {
    kind: 'local-operation';
    opId: string;
    /** All source operation ids when a legacy pending queue was composed. */
    opIds?: string[];
    operation: TextComponent[];
}

interface RemoteOperationEvent {
    kind: 'remote-operation';
    revision: number;
    ownOperation: boolean;
    operation: TextComponent[];
    /** Durable disposition evidence for logical operations settled by this transition. */
    acknowledgedIds?: string[];
}

interface TransportStateEvent {
    kind: 'transport-state';
    submittedSources: string[];
    lastSubmittedRevision?: number;
}

interface PathBindingEvent {
    kind: 'path-binding';
    path: string;
}

export type DocumentJournalEvent =
    | LocalOperationEvent
    | RemoteOperationEvent
    | TransportStateEvent
    | PathBindingEvent;

interface LegacyDocumentCheckpointState {
    schemaVersion: 1;
    otState: SerializedOtClientState;
    submittedSources: string[];
    lastSubmittedRevision?: number;
}

interface DocumentCheckpointStateV2 {
    schemaVersion: 2;
    walGeneration: string;
    otState: SerializedOtClientState;
    submittedSources: string[];
    lastSubmittedRevision?: number;
    knownOperationIds: string[];
    settledOperationIds: string[];
}

interface DocumentCheckpointStateV3 {
    schemaVersion: 3;
    walGeneration: string;
    otState: SerializedOtClientState;
    submittedSources: string[];
    lastSubmittedRevision?: number;
    knownOperationIds: string[];
    settledOperationIds: string[];
    lastKnownPath?: string;
}

type DocumentCheckpointState =
    | LegacyDocumentCheckpointState
    | DocumentCheckpointStateV2
    | DocumentCheckpointStateV3;

export const MAX_DOCUMENT_WAL_RECORDS = 256;
export const MAX_DOCUMENT_WAL_BYTES = 4 * 1024 * 1024;
export const MAX_SETTLED_OPERATION_IDS = 512;

export interface DocumentJournalOpenOptions {
    directory: string;
    documentId: string;
    initialRevision: number;
    initialContent: string;
    /** Project-relative path durably bound before any local operation is admitted. */
    initialPath?: string;
    walOptions?: DurableWalOptions;
    /** Deterministic crash cuts used only by persistence tests. */
    compactionHooks?: {
        afterGenerationSync?: () => Promise<void>;
        afterCheckpointPublish?: () => Promise<void>;
    };
}

export interface JournaledLocalOperation {
    id?: string;
    send?: OutgoingOperation;
}

export interface DocumentJournalRemoveOptions {
    /** Crash-boundary injection used only by deterministic recovery tests. */
    afterTombstone?: () => Promise<void>;
    afterWalRemove?: () => Promise<void>;
    afterCheckpointRemove?: () => Promise<void>;
}

export type DocumentOperationDisposition = 'pending' | 'settled' | 'unknown';

/**
 * Compact, event-sourced durability for one document OT state.
 *
 * Every event is validated on a cloned state before its WAL append. The live
 * state changes only after that append is durable.
 */
export class DocumentJournal {
    private operationChain: Promise<void> = Promise.resolve();
    private closed = false;
    private failure?: Error;

    private constructor(
        readonly documentId: string,
        private wal: DurableWal<DocumentJournalEvent>,
        private walPath: string,
        private walGeneration: string | undefined,
        private readonly checkpointStore: AtomicCheckpointStore<DocumentCheckpointState>,
        private readonly paths: ReturnType<typeof documentJournalPaths>,
        private readonly walOptions: DurableWalOptions,
        private readonly compactionHooks: DocumentJournalOpenOptions['compactionHooks'],
        private state: OtClientState,
        private readonly submittedSources: Set<string>,
        private readonly knownOperationIds: Set<string>,
        private readonly settledOperationIds: Set<string>,
        private lastSubmittedRevision?: number,
        private lastKnownPathValue?: string,
    ) {}

    static async open(options: DocumentJournalOpenOptions): Promise<DocumentJournal> {
        await DocumentJournal.recoverPendingRemoval(options.directory, options.documentId);
        const paths = documentJournalPaths(
            options.directory,
            options.documentId,
        );
        const checkpointStore = new AtomicCheckpointStore<DocumentCheckpointState>(paths.checkpointPath);
        const checkpoint = await checkpointStore.load();
        const walGeneration = checkpoint?.state.schemaVersion === 2
            || checkpoint?.state.schemaVersion === 3
            ? validateWalGeneration(checkpoint.state.walGeneration)
            : undefined;
        const walPath = walGeneration
            ? generationWalPath(paths, walGeneration)
            : paths.walPath;
        if (walGeneration) {
            try {
                await access(walPath);
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                    throw new WalCorruptionError(
                        `Selected document WAL generation is missing: ${walGeneration}`,
                    );
                }
                throw error;
            }
        }
        const wal = await DurableWal.open<DocumentJournalEvent>(walPath, options.walOptions);

        try {
            let state: OtClientState;
            let sources: Set<string>;
            let knownOperationIds: Set<string>;
            let settledOperationIds: Set<string>;
            let lastSubmittedRevision: number | undefined;
            let lastKnownPath: string | undefined;
            let checkpointSequence = 0;
            if (checkpoint) {
                if (
                    checkpoint.state.schemaVersion !== 1
                    && checkpoint.state.schemaVersion !== 2
                    && checkpoint.state.schemaVersion !== 3
                ) {
                    throw new WalCorruptionError('Unsupported document checkpoint schema');
                }
                checkpointSequence = checkpoint.walSequence;
                const lastWalSequence = wal.records.at(-1)?.sequence ?? 0;
                const checkpointOnlyLegacySnapshot = (
                    checkpoint.state.schemaVersion === 1
                    && checkpointSequence > lastWalSequence
                    && wal.records.length === 0
                );
                if (checkpointSequence > lastWalSequence && !checkpointOnlyLegacySnapshot) {
                    throw new WalCorruptionError('Document checkpoint is ahead of its WAL');
                }
                // Pre-generation releases could leave a checksummed, full
                // schema-1 snapshot after their legacy WAL was cleared or
                // removed. An empty WAL contains no ambiguous post-checkpoint
                // events, so the checkpoint is the complete durable state and
                // can be migrated to a selected generation. A shorter,
                // non-empty WAL remains ambiguous and still fails closed.
                state = OtClientState.restore(checkpoint.state.otState);
                sources = new Set(checkpoint.state.submittedSources);
                lastSubmittedRevision = checkpoint.state.lastSubmittedRevision;
                lastKnownPath = checkpoint.state.schemaVersion === 3
                    ? validateOptionalDocumentPath(checkpoint.state.lastKnownPath)
                    : undefined;
                if (checkpoint.state.schemaVersion === 2 || checkpoint.state.schemaVersion === 3) {
                    knownOperationIds = validateOperationIds(
                        checkpoint.state.knownOperationIds,
                        'known',
                    );
                    settledOperationIds = validateOperationIds(
                        checkpoint.state.settledOperationIds,
                        'settled',
                    );
                    if (
                        knownOperationIds.size > MAX_SETTLED_OPERATION_IDS
                        || settledOperationIds.size > MAX_SETTLED_OPERATION_IDS
                        || [...settledOperationIds].some(id => !knownOperationIds.has(id))
                    ) {
                        throw new WalCorruptionError('Document checkpoint disposition history is unbounded');
                    }
                } else {
                    knownOperationIds = new Set();
                    settledOperationIds = new Set();
                    for (const record of wal.records) {
                        recordDisposition(
                            knownOperationIds,
                            settledOperationIds,
                            record.payload,
                        );
                    }
                }
            } else {
                state = new OtClientState(options.initialRevision, options.initialContent);
                sources = new Set();
                knownOperationIds = new Set();
                settledOperationIds = new Set();
                lastKnownPath = undefined;
            }

            for (const record of wal.records) {
                if (record.sequence <= checkpointSequence) {
                    continue;
                }
                ({ state, sources, lastSubmittedRevision, lastKnownPath } = applyEvent(
                    state,
                    sources,
                    lastSubmittedRevision,
                    lastKnownPath,
                    record.payload,
                ));
                recordDisposition(knownOperationIds, settledOperationIds, record.payload);
            }
            if (checkpoint?.state.schemaVersion === 1) {
                // Legacy remote-operation records predate acknowledgedIds,
                // but their append-only WAL still contains every local id.
                // After replay, any such id absent from the active OT
                // envelopes necessarily crossed an own-ACK transition and is
                // therefore settled. Preserve that proof during v1 -> v2
                // migration instead of degrading it to unknown.
                const active = new Set(state.activeOperationIds);
                for (const id of knownOperationIds) {
                    if (!active.has(id)) {
                        settledOperationIds.add(id);
                    }
                }
            }
            pruneDispositionEvidence(knownOperationIds, settledOperationIds, state.activeOperationIds);

            const journal = new DocumentJournal(
                options.documentId,
                wal,
                walPath,
                walGeneration,
                checkpointStore,
                paths,
                options.walOptions ?? {},
                options.compactionHooks,
                state,
                sources,
                knownOperationIds,
                settledOperationIds,
                lastSubmittedRevision,
                lastKnownPath,
            );
            if (options.initialPath !== undefined) {
                await journal.bindPath(options.initialPath);
            }
            if (!checkpoint || checkpoint.state.schemaVersion !== 3) {
                await journal.saveCheckpoint();
            }
            await journal.removeUnselectedWalGenerations();
            return journal;
        } catch (error) {
            try {
                await wal.close();
            } catch (closeError) {
                const opening = error instanceof Error ? error.message : String(error);
                const closing = closeError instanceof Error ? closeError.message : String(closeError);
                throw new WalCorruptionError(
                    `Document journal failed (${opening}) and its WAL handle could not close (${closing})`,
                );
            }
            throw error;
        }
    }

    static async openExisting(
        directory: string,
        documentId: string,
        walOptions?: DurableWalOptions,
    ): Promise<DocumentJournal | undefined> {
        await DocumentJournal.recoverPendingRemoval(directory, documentId);
        const { checkpointPath } = documentJournalPaths(directory, documentId);
        const checkpointStore = new AtomicCheckpointStore<DocumentCheckpointState>(checkpointPath);
        if (!await checkpointStore.load()) {
            return undefined;
        }
        return DocumentJournal.open({
            directory,
            documentId,
            initialRevision: 0,
            initialContent: '',
            walOptions,
        });
    }

    static async listExisting(directory: string): Promise<string[]> {
        const documentsDirectory = path.join(directory, 'documents');
        let entries = await DocumentJournal.readDocumentDirectory(documentsDirectory);
        for (const entry of entries) {
            if (!entry.endsWith(DOCUMENT_REMOVAL_SUFFIX)) {
                continue;
            }
            const documentId = decodeDocumentJournalName(entry, DOCUMENT_REMOVAL_SUFFIX);
            await DocumentJournal.recoverPendingRemoval(directory, documentId);
        }
        entries = await DocumentJournal.readDocumentDirectory(documentsDirectory);
        const ids = new Set<string>();
        for (const entry of entries) {
            if (!entry.endsWith(DOCUMENT_CHECKPOINT_SUFFIX)) {
                continue;
            }
            ids.add(decodeDocumentJournalName(entry, DOCUMENT_CHECKPOINT_SUFFIX));
        }
        return [...ids].sort();
    }

    private static async readDocumentDirectory(documentsDirectory: string): Promise<string[]> {
        try {
            return await readdir(documentsDirectory);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                return [];
            }
            throw error;
        }
    }

    static async remove(
        directory: string,
        documentId: string,
        options: DocumentJournalRemoveOptions = {},
    ): Promise<void> {
        const paths = documentJournalPaths(directory, documentId);
        await writeFileDurably(
            paths.removalPath,
            new TextEncoder().encode(JSON.stringify({
                schemaVersion: 1,
                documentId,
            })),
        );
        await options.afterTombstone?.();
        await DocumentJournal.completePendingRemoval(paths, options);
    }

    private static async recoverPendingRemoval(
        directory: string,
        documentId: string,
    ): Promise<void> {
        const paths = documentJournalPaths(directory, documentId);
        let tombstone: unknown;
        try {
            tombstone = JSON.parse(await readFile(paths.removalPath, 'utf8'));
        } catch (error) {
            if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') {
                return;
            }
            const message = error instanceof Error ? error.message : String(error);
            throw new WalCorruptionError(
                `Invalid document journal removal tombstone for ${documentId}: ${message}`,
            );
        }
        const value = tombstone as {schemaVersion?: unknown; documentId?: unknown};
        if (value?.schemaVersion !== 1 || value.documentId !== documentId) {
            throw new WalCorruptionError(
                `Invalid document journal removal tombstone for ${documentId}`,
            );
        }
        await DocumentJournal.completePendingRemoval(paths);
    }

    private static async completePendingRemoval(
        paths: ReturnType<typeof documentJournalPaths>,
        options: DocumentJournalRemoveOptions = {},
    ): Promise<void> {
        const directory = paths.documentsDirectory;
        const entries = await DocumentJournal.readDocumentDirectory(directory);
        for (const entry of entries) {
            if (isDocumentWalEntry(entry, paths.walName)) {
                await rm(path.join(directory, entry), {force: true});
            }
        }
        await syncDirectoryDurably(directory);
        await options.afterWalRemove?.();
        await rm(paths.checkpointPath, {force: true});
        await syncDirectoryDurably(directory);
        await options.afterCheckpointRemove?.();
        await rm(paths.removalPath, {force: true});
        await syncDirectoryDurably(directory);
    }

    get content(): string {
        return this.state.visibleContent;
    }

    get acknowledgedContent(): string {
        return this.state.acknowledgedContent;
    }

    get revision(): number {
        return this.state.currentRevision;
    }

    get currentOutgoingOperation(): OutgoingOperation | undefined {
        return this.state.currentOutgoingOperation;
    }

    get hasPendingOperations(): boolean {
        return this.state.hasPendingOperations;
    }

    get pendingOperationIds(): readonly string[] {
        return this.state.activeOperationIds;
    }

    operationDisposition(opId: string): DocumentOperationDisposition {
        if (this.state.activeOperationIds.includes(opId)) {
            return 'pending';
        }
        if (this.knownOperationIds.has(opId) && this.settledOperationIds.has(opId)) {
            return 'settled';
        }
        return 'unknown';
    }

    get transportSources(): readonly string[] {
        return [...this.submittedSources];
    }

    get previousSubmittedRevision(): number | undefined {
        return this.lastSubmittedRevision;
    }

    get lastKnownPath(): string | undefined {
        return this.lastKnownPathValue;
    }

    async bindPath(path: string, barrier = true): Promise<void> {
        const normalized = validateDocumentPath(path);
        return this.runExclusive(async () => {
            if (this.lastKnownPathValue === normalized) {
                return;
            }
            await this.wal.append({kind: 'path-binding', path: normalized}, barrier);
            this.lastKnownPathValue = normalized;
            await this.maybeCompact();
        });
    }

    async commitLocalOperation(
        operation: TextOperation,
        opId: string,
        barrier = false,
        opIds: readonly string[] = [opId],
    ): Promise<JournaledLocalOperation> {
        return this.runExclusive(() => this.commitLocalOperationInternal(
            operation,
            opId,
            barrier,
            opIds,
        ));
    }

    private async commitLocalOperationInternal(
        operation: TextOperation,
        opId: string,
        barrier: boolean,
        opIds: readonly string[],
    ): Promise<JournaledLocalOperation> {
        if (this.lastKnownPathValue === undefined) {
            throw new WalCorruptionError(
                'Local document operation requires a durable project path binding',
            );
        }
        const next = OtClientState.restore(this.state.serialize());
        const ids = [...opIds];
        if (ids.length === 0 || ids[0] !== opId) {
            throw new WalCorruptionError('Local operation ids are not ordered');
        }
        const submission = next.submitOperation(operation, opId, ids);
        if (!submission.id) {
            return {};
        }
        await this.wal.append({
            kind: 'local-operation',
            opId,
            opIds: ids,
            operation: operation.toJSON(),
        }, barrier);
        this.state = next;
        for (const id of ids) {
            this.knownOperationIds.add(id);
        }
        await this.maybeCompact();
        return submission;
    }

    async commitRemoteOperation(
        operation: TextOperation,
        revision: number,
        ownOperation: boolean,
        barrier = true,
    ): Promise<ReceiveResult> {
        return this.runExclusive(() => this.commitRemoteOperationInternal(
            operation,
            revision,
            ownOperation,
            barrier,
        ));
    }

    private async commitRemoteOperationInternal(
        operation: TextOperation,
        revision: number,
        ownOperation: boolean,
        barrier: boolean,
    ): Promise<ReceiveResult> {
        const next = OtClientState.restore(this.state.serialize());
        const result = next.receive(operation, revision, ownOperation);
        await this.wal.append({
            kind: 'remote-operation',
            revision,
            ownOperation,
            operation: operation.toJSON(),
            acknowledgedIds: result.acknowledgedIds,
        }, barrier);
        this.state = next;
        for (const id of result.acknowledgedIds) {
            this.settledOperationIds.add(id);
        }
        if (ownOperation) {
            this.submittedSources.clear();
        }
        pruneDispositionEvidence(
            this.knownOperationIds,
            this.settledOperationIds,
            this.state.activeOperationIds,
        );
        await this.maybeCompact();
        return result;
    }

    async commitTransportState(
        submittedSources: readonly string[],
        lastSubmittedRevision: number | undefined,
        barrier = true,
    ): Promise<void> {
        return this.runExclusive(() => this.commitTransportStateInternal(
            submittedSources,
            lastSubmittedRevision,
            barrier,
        ));
    }

    private async commitTransportStateInternal(
        submittedSources: readonly string[],
        lastSubmittedRevision: number | undefined,
        barrier: boolean,
    ): Promise<void> {
        const uniqueSources = [...new Set(submittedSources)];
        await this.wal.append({
            kind: 'transport-state',
            submittedSources: uniqueSources,
            lastSubmittedRevision,
        }, barrier);
        this.submittedSources.clear();
        for (const source of uniqueSources) {
            this.submittedSources.add(source);
        }
        this.lastSubmittedRevision = lastSubmittedRevision;
        await this.maybeCompact();
    }

    /** Import intact legacy operation envelopes into a fresh v2 journal. */
    async seedLegacyState(saved: SerializedOtClientState): Promise<void> {
        return this.runExclusive(async () => {
            if (this.revision !== saved.revision || this.acknowledgedContent !== saved.serverContent) {
                throw new WalCorruptionError('Legacy document state does not match its journal genesis');
            }
            for (const envelope of [saved.outstanding, saved.pending]) {
                if (!envelope) {
                    continue;
                }
                if (envelope.ids.length === 0) {
                    throw new WalCorruptionError('Legacy operation envelope has no source ids');
                }
                await this.commitLocalOperationInternal(
                    TextOperation.fromJSON(envelope.operation),
                    envelope.ids[0],
                    true,
                    envelope.ids,
                );
            }
            await this.commitTransportStateInternal(
                saved.submittedSources ?? [],
                saved.lastSubmittedRevision,
                true,
            );
            await this.compact();
        });
    }

    async saveCheckpoint(): Promise<void> {
        return this.runExclusive(() => this.compact());
    }

    private async maybeCompact(): Promise<void> {
        if (
            this.wal.records.length >= MAX_DOCUMENT_WAL_RECORDS
            || this.wal.byteLength >= MAX_DOCUMENT_WAL_BYTES
        ) {
            await this.compact();
        }
    }

    private async compact(): Promise<void> {
        await this.wal.flush();
        if (this.walGeneration !== undefined && this.wal.records.length === 0) {
            return;
        }

        pruneDispositionEvidence(
            this.knownOperationIds,
            this.settledOperationIds,
            this.state.activeOperationIds,
        );
        const generation = randomUUID();
        const nextWalPath = generationWalPath(this.paths, generation);
        let nextWal: DurableWal<DocumentJournalEvent> | undefined;
        try {
            await DurableWal.createEmpty(nextWalPath);
            await this.compactionHooks?.afterGenerationSync?.();
            nextWal = await DurableWal.open<DocumentJournalEvent>(nextWalPath, this.walOptions);
            await this.checkpointStore.save({
                walSequence: 0,
                state: {
                    schemaVersion: 3,
                    walGeneration: generation,
                    otState: this.state.serialize(),
                    submittedSources: [...this.submittedSources],
                    lastSubmittedRevision: this.lastSubmittedRevision,
                    // Active ids already live in otState. Keep only bounded
                    // historical disposition evidence in the checkpoint.
                    knownOperationIds: [...this.settledOperationIds],
                    settledOperationIds: [...this.settledOperationIds],
                    lastKnownPath: this.lastKnownPathValue,
                },
            });
            await this.compactionHooks?.afterCheckpointPublish?.();
        } catch (error) {
            const normalized = error instanceof Error ? error : new Error(String(error));
            this.failure = normalized;
            await nextWal?.close().catch(() => {});
            await this.wal.close().catch(() => {});
            this.closed = true;
            throw normalized;
        }

        const previousWal = this.wal;
        const previousWalPath = this.walPath;
        this.wal = nextWal;
        this.walPath = nextWalPath;
        this.walGeneration = generation;
        await previousWal.close();
        if (previousWalPath !== nextWalPath) {
            await rm(previousWalPath, { force: true });
            await syncDirectoryDurably(this.paths.documentsDirectory);
        }
        await this.removeUnselectedWalGenerations();
    }

    async close(): Promise<void> {
        const close = this.operationChain.then(async () => {
            if (this.closed) {
                if (this.failure) {
                    throw this.failure;
                }
                return;
            }
            await this.compact();
            this.closed = true;
            await this.wal.close();
        });
        this.operationChain = close.then(() => undefined, () => undefined);
        return close;
    }

    private runExclusive<T>(operation: () => Promise<T>): Promise<T> {
        const run = this.operationChain.then(async () => {
            if (this.failure) {
                throw this.failure;
            }
            if (this.closed) {
                throw new Error('Document journal is closed');
            }
            return operation();
        });
        this.operationChain = run.then(() => undefined, () => undefined);
        return run;
    }

    private async removeUnselectedWalGenerations(): Promise<void> {
        const entries = await DocumentJournal.readDocumentDirectory(this.paths.documentsDirectory);
        let removed = false;
        for (const entry of entries) {
            if (
                isDocumentWalEntry(entry, this.paths.walName)
                && path.join(this.paths.documentsDirectory, entry) !== this.walPath
            ) {
                await rm(path.join(this.paths.documentsDirectory, entry), { force: true });
                removed = true;
            }
        }
        if (removed) {
            await syncDirectoryDurably(this.paths.documentsDirectory);
        }
    }
}

function documentJournalPaths(directory: string, documentId: string): {
    walPath: string;
    walName: string;
    checkpointPath: string;
    removalPath: string;
    documentsDirectory: string;
} {
    const safeDocumentId = encodeURIComponent(documentId);
    const documentsDirectory = path.join(directory, 'documents');
    const walName = `${safeDocumentId}.wal`;
    return {
        walPath: path.join(documentsDirectory, walName),
        walName,
        checkpointPath: path.join(documentsDirectory, `${safeDocumentId}${DOCUMENT_CHECKPOINT_SUFFIX}`),
        removalPath: path.join(documentsDirectory, `${safeDocumentId}${DOCUMENT_REMOVAL_SUFFIX}`),
        documentsDirectory,
    };
}

const DOCUMENT_CHECKPOINT_SUFFIX = '.checkpoint.json';
const DOCUMENT_REMOVAL_SUFFIX = '.delete.json';

function generationWalPath(
    paths: ReturnType<typeof documentJournalPaths>,
    generation: string,
): string {
    return path.join(paths.documentsDirectory, `${paths.walName}.${generation}`);
}

function validateWalGeneration(generation: unknown): string {
    if (
        typeof generation !== 'string'
        || !DOCUMENT_WAL_GENERATION_PATTERN.test(generation)
    ) {
        throw new WalCorruptionError('Invalid document WAL generation');
    }
    return generation;
}

const DOCUMENT_WAL_GENERATION_PATTERN = (
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
);

function isDocumentWalEntry(entry: string, walName: string): boolean {
    if (entry === walName) {
        return true;
    }
    const prefix = `${walName}.`;
    return entry.startsWith(prefix)
        && DOCUMENT_WAL_GENERATION_PATTERN.test(entry.slice(prefix.length));
}

function validateOperationIds(ids: unknown, label: string): Set<string> {
    if (
        !Array.isArray(ids)
        || ids.some(id => typeof id !== 'string' || id.length === 0)
        || new Set(ids).size !== ids.length
    ) {
        throw new WalCorruptionError(`Invalid ${label} operation disposition evidence`);
    }
    return new Set(ids);
}

function recordDisposition(
    knownOperationIds: Set<string>,
    settledOperationIds: Set<string>,
    event: DocumentJournalEvent,
): void {
    if (event.kind === 'local-operation') {
        for (const id of event.opIds ?? [event.opId]) {
            knownOperationIds.add(id);
        }
    } else if (event.kind === 'remote-operation') {
        for (const id of event.acknowledgedIds ?? []) {
            // A compacted checkpoint intentionally omits active ids from its
            // bounded historical set because otState already owns them. When
            // an ACK is replayed from the next WAL generation, that active id
            // disappears from otState, so the ACK itself must re-establish
            // both halves of the settled-disposition proof.
            knownOperationIds.add(id);
            settledOperationIds.add(id);
        }
    }
}

function pruneDispositionEvidence(
    knownOperationIds: Set<string>,
    settledOperationIds: Set<string>,
    activeOperationIds: readonly string[],
): void {
    const active = new Set(activeOperationIds);
    while (settledOperationIds.size > MAX_SETTLED_OPERATION_IDS) {
        const oldest = settledOperationIds.values().next().value as string | undefined;
        if (oldest === undefined) {
            break;
        }
        settledOperationIds.delete(oldest);
        if (!active.has(oldest)) {
            knownOperationIds.delete(oldest);
        }
    }
    for (const id of knownOperationIds) {
        if (!active.has(id) && !settledOperationIds.has(id)) {
            knownOperationIds.delete(id);
        }
    }
    for (const id of active) {
        knownOperationIds.add(id);
    }
}

function decodeDocumentJournalName(entry: string, suffix: string): string {
    const encoded = entry.slice(0, -suffix.length);
    let documentId: string;
    try {
        documentId = decodeURIComponent(encoded);
    } catch {
        throw new WalCorruptionError(`Invalid document journal filename: ${entry}`);
    }
    if (!documentId || encodeURIComponent(documentId) !== encoded) {
        throw new WalCorruptionError(`Non-canonical document journal filename: ${entry}`);
    }
    return documentId;
}

function applyEvent(
    current: OtClientState,
    currentSources: Set<string>,
    currentLastSubmittedRevision: number | undefined,
    currentLastKnownPath: string | undefined,
    event: DocumentJournalEvent,
): {
    state: OtClientState;
    sources: Set<string>;
    lastSubmittedRevision?: number;
    lastKnownPath?: string;
} {
    const state = OtClientState.restore(current.serialize());
    const sources = new Set(currentSources);
    let lastSubmittedRevision = currentLastSubmittedRevision;
    let lastKnownPath = currentLastKnownPath;
    switch (event.kind) {
        case 'local-operation':
            state.submitOperation(
                TextOperation.fromJSON(event.operation),
                event.opId,
                event.opIds ?? [event.opId],
            );
            break;
        case 'remote-operation':
            const result = state.receive(
                TextOperation.fromJSON(event.operation),
                event.revision,
                event.ownOperation,
            );
            if (
                event.acknowledgedIds !== undefined
                && JSON.stringify(event.acknowledgedIds) !== JSON.stringify(result.acknowledgedIds)
            ) {
                throw new WalCorruptionError('Document operation disposition does not match OT transition');
            }
            if (event.ownOperation) {
                sources.clear();
            }
            break;
        case 'transport-state':
            sources.clear();
            for (const source of event.submittedSources) {
                sources.add(source);
            }
            lastSubmittedRevision = event.lastSubmittedRevision;
            break;
        case 'path-binding':
            lastKnownPath = validateDocumentPath(event.path);
            break;
    }
    return { state, sources, lastSubmittedRevision, lastKnownPath };
}

function validateOptionalDocumentPath(path: unknown): string | undefined {
    return path === undefined ? undefined : validateDocumentPath(path);
}

function validateDocumentPath(path: unknown): string {
    if (typeof path !== 'string') {
        throw new WalCorruptionError('Invalid document path binding');
    }
    const normalized = path.replace(/\\/g, '/').replace(/^\/+/, '').replace(/\/+$/, '');
    if (
        normalized.length === 0
        || normalized.split('/').some(part => part === '' || part === '.' || part === '..')
    ) {
        throw new WalCorruptionError('Invalid document path binding');
    }
    return normalized;
}
