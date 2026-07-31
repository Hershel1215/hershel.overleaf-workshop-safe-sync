import * as path from 'path';
import { rm } from 'fs/promises';
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
}

interface TransportStateEvent {
    kind: 'transport-state';
    submittedSources: string[];
    lastSubmittedRevision?: number;
}

export type DocumentJournalEvent =
    | LocalOperationEvent
    | RemoteOperationEvent
    | TransportStateEvent;

interface DocumentCheckpointState {
    schemaVersion: 1;
    otState: SerializedOtClientState;
    submittedSources: string[];
    lastSubmittedRevision?: number;
}

export interface DocumentJournalOpenOptions {
    directory: string;
    documentId: string;
    initialRevision: number;
    initialContent: string;
    walOptions?: DurableWalOptions;
}

export interface JournaledLocalOperation {
    id?: string;
    send?: OutgoingOperation;
}

/**
 * Compact, event-sourced durability for one document OT state.
 *
 * Every event is validated on a cloned state before its WAL append. The live
 * state changes only after that append is durable.
 */
export class DocumentJournal {
    private constructor(
        readonly documentId: string,
        private readonly wal: DurableWal<DocumentJournalEvent>,
        private readonly checkpointStore: AtomicCheckpointStore<DocumentCheckpointState>,
        private state: OtClientState,
        private readonly submittedSources: Set<string>,
        private lastSubmittedRevision?: number,
    ) {}

    static async open(options: DocumentJournalOpenOptions): Promise<DocumentJournal> {
        const { walPath, checkpointPath } = documentJournalPaths(
            options.directory,
            options.documentId,
        );
        const checkpointStore = new AtomicCheckpointStore<DocumentCheckpointState>(checkpointPath);
        const checkpoint = await checkpointStore.load();
        const wal = await DurableWal.open<DocumentJournalEvent>(walPath, options.walOptions);

        let state: OtClientState;
        let sources: Set<string>;
        let lastSubmittedRevision: number | undefined;
        let checkpointSequence = 0;
        if (checkpoint) {
            if (checkpoint.state.schemaVersion !== 1) {
                throw new WalCorruptionError('Unsupported document checkpoint schema');
            }
            checkpointSequence = checkpoint.walSequence;
            if (checkpointSequence > (wal.records.at(-1)?.sequence ?? 0)) {
                throw new WalCorruptionError('Document checkpoint is ahead of its WAL');
            }
            state = OtClientState.restore(checkpoint.state.otState);
            sources = new Set(checkpoint.state.submittedSources);
            lastSubmittedRevision = checkpoint.state.lastSubmittedRevision;
        } else {
            state = new OtClientState(options.initialRevision, options.initialContent);
            sources = new Set();
        }

        for (const record of wal.records) {
            if (record.sequence <= checkpointSequence) {
                continue;
            }
            ({ state, sources, lastSubmittedRevision } = applyEvent(
                state,
                sources,
                lastSubmittedRevision,
                record.payload,
            ));
        }

        const journal = new DocumentJournal(
            options.documentId,
            wal,
            checkpointStore,
            state,
            sources,
            lastSubmittedRevision,
        );
        if (!checkpoint) {
            await journal.saveCheckpoint();
        }
        return journal;
    }

    static async openExisting(
        directory: string,
        documentId: string,
        walOptions?: DurableWalOptions,
    ): Promise<DocumentJournal | undefined> {
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

    static async remove(directory: string, documentId: string): Promise<void> {
        const { walPath, checkpointPath } = documentJournalPaths(directory, documentId);
        await Promise.all([
            rm(walPath, { force: true }),
            rm(checkpointPath, { force: true }),
        ]);
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

    get transportSources(): readonly string[] {
        return [...this.submittedSources];
    }

    get previousSubmittedRevision(): number | undefined {
        return this.lastSubmittedRevision;
    }

    async commitLocalOperation(
        operation: TextOperation,
        opId: string,
        barrier = false,
        opIds: readonly string[] = [opId],
    ): Promise<JournaledLocalOperation> {
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
        return submission;
    }

    async commitRemoteOperation(
        operation: TextOperation,
        revision: number,
        ownOperation: boolean,
        barrier = true,
    ): Promise<ReceiveResult> {
        const next = OtClientState.restore(this.state.serialize());
        const result = next.receive(operation, revision, ownOperation);
        await this.wal.append({
            kind: 'remote-operation',
            revision,
            ownOperation,
            operation: operation.toJSON(),
        }, barrier);
        this.state = next;
        if (ownOperation) {
            this.submittedSources.clear();
        }
        return result;
    }

    async commitTransportState(
        submittedSources: readonly string[],
        lastSubmittedRevision: number | undefined,
        barrier = true,
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
    }

    /** Import intact legacy operation envelopes into a fresh v2 journal. */
    async seedLegacyState(saved: SerializedOtClientState): Promise<void> {
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
            await this.commitLocalOperation(
                TextOperation.fromJSON(envelope.operation),
                envelope.ids[0],
                true,
                envelope.ids,
            );
        }
        await this.commitTransportState(
            saved.submittedSources ?? [],
            saved.lastSubmittedRevision,
            true,
        );
        await this.saveCheckpoint();
    }

    async saveCheckpoint(): Promise<void> {
        await this.wal.flush();
        await this.checkpointStore.save({
            walSequence: this.wal.records.at(-1)?.sequence ?? 0,
            state: {
                schemaVersion: 1,
                otState: this.state.serialize(),
                submittedSources: [...this.submittedSources],
                lastSubmittedRevision: this.lastSubmittedRevision,
            },
        });
    }

    async close(): Promise<void> {
        await this.saveCheckpoint();
        await this.wal.close();
    }
}

function documentJournalPaths(directory: string, documentId: string): {
    walPath: string;
    checkpointPath: string;
} {
    const safeDocumentId = encodeURIComponent(documentId);
    return {
        walPath: path.join(directory, 'documents', `${safeDocumentId}.wal`),
        checkpointPath: path.join(directory, 'documents', `${safeDocumentId}.checkpoint.json`),
    };
}

function applyEvent(
    current: OtClientState,
    currentSources: Set<string>,
    currentLastSubmittedRevision: number | undefined,
    event: DocumentJournalEvent,
): {
    state: OtClientState;
    sources: Set<string>;
    lastSubmittedRevision?: number;
} {
    const state = OtClientState.restore(current.serialize());
    const sources = new Set(currentSources);
    let lastSubmittedRevision = currentLastSubmittedRevision;
    switch (event.kind) {
        case 'local-operation':
            state.submitOperation(
                TextOperation.fromJSON(event.operation),
                event.opId,
                event.opIds ?? [event.opId],
            );
            break;
        case 'remote-operation':
            state.receive(
                TextOperation.fromJSON(event.operation),
                event.revision,
                event.ownOperation,
            );
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
    }
    return { state, sources, lastSubmittedRevision };
}
