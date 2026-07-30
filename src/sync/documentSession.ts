/* eslint-disable @typescript-eslint/naming-convention */
import { createHash } from 'crypto';
import { OutgoingOperation, OtClientState, SerializedOtClientState } from './otClientState';
import { LegacyTextEdit, TextOperation } from './textOperation';

export interface LegacyDocumentUpdate {
    doc: string;
    v: number;
    op?: LegacyTextEdit[];
    lastV?: number;
    hash?: string;
    dupIfSource?: string[];
    error?: string;
    meta?: {
        source?: string;
        ts?: number;
        user_id?: string;
        [key: string]: unknown;
    };
}

export interface DocumentSessionOptions {
    documentId: string;
    content: string;
    revision: number;
    savedState?: SerializedOtClientState;
    /** Ordered authoritative updates from savedState.revision to revision. */
    catchUpUpdates?: LegacyDocumentUpdate[];
    sendUpdate: (update: LegacyDocumentUpdate) => Promise<void>;
    persist: (state: SerializedOtClientState) => Promise<void>;
    getSourceId: () => string | undefined;
    onChange: (content: string, revision: number, acknowledgedContent: string) => void;
    onError: (error: Error) => void;
    retryDelayMs?: number;
    fatalDelayMs?: number;
}

interface Completion {
    resolve: (content: string) => void;
    reject: (error: Error) => void;
}

/**
 * Binds the pure OT client state to Overleaf's legacy applyOtUpdate protocol.
 * All transitions are serialized, persisted before network transmission, and
 * acknowledged only by authoritative otUpdateApplied messages.
 */
export class DocumentSession {
    private readonly documentId: string;
    private readonly sendUpdate: (update: LegacyDocumentUpdate) => Promise<void>;
    private readonly persistState: (state: SerializedOtClientState) => Promise<void>;
    private readonly getSourceId: () => string | undefined;
    private readonly onChange: (content: string, revision: number, acknowledgedContent: string) => void;
    private readonly onError: (error: Error) => void;
    private readonly retryDelayMs: number;
    private readonly fatalDelayMs: number;
    private readonly completions = new Map<string, Completion>();
    private readonly submittedSources = new Set<string>();
    private readonly bufferedUpdates = new Map<number, LegacyDocumentUpdate>();
    private state: OtClientState;
    private transition: Promise<void> = Promise.resolve();
    private inflightWire?: LegacyDocumentUpdate;
    private retryTimer?: NodeJS.Timeout;
    private fatalTimer?: NodeJS.Timeout;
    private lastSubmittedRevision?: number;
    private lastHashTimestamp = 0;
    private failed?: Error;

    private constructor(options: DocumentSessionOptions, state: OtClientState) {
        this.documentId = options.documentId;
        this.sendUpdate = options.sendUpdate;
        this.persistState = options.persist;
        this.getSourceId = options.getSourceId;
        this.onChange = options.onChange;
        this.onError = options.onError;
        this.retryDelayMs = options.retryDelayMs ?? 5000;
        this.fatalDelayMs = options.fatalDelayMs ?? 45000;
        this.state = state;
    }

    static async create(options: DocumentSessionOptions): Promise<DocumentSession> {
        const state = options.savedState
            ? DocumentSession.restoreState(options)
            : new OtClientState(options.revision, options.content);
        const session = new DocumentSession(options, state);
        for (const source of options.savedState?.submittedSources ?? []) {
            session.submittedSources.add(source);
        }
        session.lastSubmittedRevision = options.savedState?.lastSubmittedRevision;

        if (options.savedState && options.catchUpUpdates) {
            for (const update of options.catchUpUpdates) {
                if (update.v !== session.revision) {
                    throw new Error(
                        `Catch-up update version ${update.v} does not match expected ${session.revision}`,
                    );
                }
                await session.processUpdate({ ...update, doc: options.documentId }, false);
            }
            if (session.revision !== options.revision
                || session.acknowledgedContent !== options.content) {
                throw new Error('Catch-up operations do not reproduce the authoritative server snapshot');
            }
        }
        await session.persist();
        session.notifyChange();
        const outgoing = session.state.currentOutgoingOperation;
        if (outgoing) {
            await session.startOutgoing(outgoing, options.savedState !== undefined);
        }
        return session;
    }

    private static restoreState(options: DocumentSessionOptions): OtClientState {
        const saved = options.savedState!;
        if (options.revision < saved.revision) {
            throw new Error('Server revision moved backwards; saved ancestry is no longer trustworthy');
        }
        if (options.catchUpUpdates) {
            return OtClientState.restore(saved);
        }
        const hasLocalOperations = saved.outstanding !== undefined || saved.pending !== undefined;
        if (!hasLocalOperations) {
            return new OtClientState(options.revision, options.content);
        }
        if (options.revision === saved.revision && options.content === saved.serverContent) {
            return OtClientState.restore(saved);
        }
        if (options.content === saved.localContent) {
            // The exact desired snapshot is already authoritative. It is safe to
            // accept it even when the ACK and subsequent metadata were lost.
            return new OtClientState(options.revision, options.content);
        }
        throw new Error(
            'Cannot prove whether the saved local operation is already present in the server snapshot',
        );
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

    get isFailed(): boolean {
        return this.failed !== undefined;
    }

    submitSnapshot(content: string): Promise<void> {
        return this.submitPlannedSnapshot(() => content).then(() => undefined);
    }

    /**
     * Submit a snapshot whose operation was prepared against a durable common
     * ancestor. If newer remote or same-client edits arrived since that scan,
     * they receive ordering priority and the requested change is transformed
     * behind them instead of replacing them.
     */
    submitSnapshotAgainstAncestor(ancestor: string, target: string): Promise<string> {
        return this.submitPlannedSnapshot(currentContent => {
            if (currentContent === ancestor) {
                return target;
            }
            const currentOperation = TextOperation.fromDiff(ancestor, currentContent);
            const requestedOperation = TextOperation.fromDiff(ancestor, target);
            const [, requestedPrime] = TextOperation.transform(currentOperation, requestedOperation);
            return requestedPrime.apply(currentContent);
        });
    }

    private submitPlannedSnapshot(planner: (currentContent: string) => string): Promise<string> {
        if (this.failed) {
            return Promise.reject(this.failed);
        }
        return new Promise<string>((resolve, reject) => {
            this.enqueue(async () => {
                this.assertHealthy();
                const content = planner(this.state.visibleContent);
                const submission = this.state.submitSnapshot(content);
                if (!submission.id) {
                    resolve(this.state.visibleContent);
                    return;
                }
                this.completions.set(submission.id, { resolve, reject });
                await this.persist();
                this.notifyChange();
                if (submission.send) {
                    await this.startOutgoing(submission.send);
                }
            }).catch(error => {
                const normalized = this.normalizeError(error);
                reject(normalized);
                this.fail(normalized);
            });
        });
    }

    receiveUpdate(update: LegacyDocumentUpdate): Promise<void> {
        return this.enqueue(async () => {
            this.assertHealthy();
            if (update.doc !== this.documentId) {
                return;
            }
            if (update.v < this.revision) {
                return;
            }
            if (update.v > this.revision) {
                const existing = this.bufferedUpdates.get(update.v);
                if (existing && JSON.stringify(existing) !== JSON.stringify(update)) {
                    throw new Error(`Conflicting buffered updates at revision ${update.v}`);
                }
                this.bufferedUpdates.set(update.v, update);
                return;
            }

            await this.processUpdate(update);
            while (this.bufferedUpdates.has(this.revision)) {
                const next = this.bufferedUpdates.get(this.revision)!;
                this.bufferedUpdates.delete(this.revision);
                await this.processUpdate(next);
            }
        }).catch(error => this.fail(this.normalizeError(error)));
    }

    dispose(): void {
        this.clearInflightTimers();
        const error = new Error('Document session disposed before acknowledgement');
        for (const completion of this.completions.values()) {
            completion.reject(error);
        }
        this.completions.clear();
    }

    private async processUpdate(
        update: LegacyDocumentUpdate,
        sendNext = true,
    ): Promise<OutgoingOperation | undefined> {
        if (update.error) {
            throw new Error(`Overleaf rejected document operation: ${update.error}`);
        }

        let operation: TextOperation;
        let ownOperation = false;
        if (update.op === undefined) {
            const outstanding = this.state.outstandingOperation;
            if (!outstanding) {
                throw new Error(`Unexpected acknowledgement at revision ${update.v}`);
            }
            operation = outstanding;
            ownOperation = true;
        } else {
            operation = TextOperation.fromLegacyEdits(
                this.state.acknowledgedContent,
                update.op,
                decodePackedUtf8,
            );
            ownOperation = update.meta?.source !== undefined
                && this.submittedSources.has(update.meta.source);
        }

        const result = this.state.receive(operation, update.v, ownOperation);

        if (ownOperation) {
            this.clearInflightTimers();
            this.inflightWire = undefined;
            this.submittedSources.clear();
        }
        await this.persist();
        this.notifyChange();

        if (ownOperation) {
            for (const id of result.acknowledgedIds) {
                this.completions.get(id)?.resolve(this.state.visibleContent);
                this.completions.delete(id);
            }
            if (result.send && sendNext) {
                await this.startOutgoing(result.send);
            }
        }
        return result.send;
    }

    private async startOutgoing(outgoing: OutgoingOperation, recovering = false): Promise<void> {
        const sourceId = this.getSourceId();
        if (sourceId) {
            this.submittedSources.add(sourceId);
        }
        const targetContent = outgoing.operation.apply(this.state.acknowledgedContent);
        const now = Date.now();
        const includeHash = this.lastSubmittedRevision === undefined
            || now - this.lastHashTimestamp >= 5000;
        const update: LegacyDocumentUpdate = {
            doc: this.documentId,
            v: outgoing.revision,
            lastV: this.lastSubmittedRevision,
            op: outgoing.operation.toLegacyEdits(this.state.acknowledgedContent),
            hash: includeHash ? gitBlobHash(targetContent) : undefined,
            dupIfSource: recovering && this.submittedSources.size > 0
                ? [...this.submittedSources]
                : undefined,
        };
        this.lastSubmittedRevision = outgoing.revision;
        if (includeHash) {
            this.lastHashTimestamp = now;
        }
        this.inflightWire = update;
        await this.persist();
        await this.sendUpdate(update);
        this.scheduleInflightTimers();
    }

    private scheduleInflightTimers(): void {
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
        }
        this.retryTimer = setTimeout(() => {
            this.enqueue(async () => {
                if (!this.inflightWire || this.failed) {
                    return;
                }
                const sourceId = this.getSourceId();
                if (sourceId) {
                    this.submittedSources.add(sourceId);
                }
                await this.persist();
                await this.sendUpdate({
                    ...this.inflightWire,
                    dupIfSource: [...this.submittedSources],
                });
                this.scheduleInflightTimers();
            }).catch(error => this.fail(this.normalizeError(error)));
        }, this.retryDelayMs);

        if (!this.fatalTimer) {
            this.fatalTimer = setTimeout(() => {
                this.fail(new Error(`Document operation acknowledgement timed out after ${this.fatalDelayMs} ms`));
            }, this.fatalDelayMs);
        }
    }

    private clearInflightTimers(): void {
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
            this.retryTimer = undefined;
        }
        if (this.fatalTimer) {
            clearTimeout(this.fatalTimer);
            this.fatalTimer = undefined;
        }
    }

    private async persist(): Promise<void> {
        const serialized = this.state.serialize();
        serialized.submittedSources = [...this.submittedSources];
        serialized.lastSubmittedRevision = this.lastSubmittedRevision;
        await this.persistState(serialized);
    }

    private notifyChange(): void {
        this.onChange(this.content, this.revision, this.acknowledgedContent);
    }

    private enqueue(task: () => Promise<void>): Promise<void> {
        const next = this.transition.then(task, task);
        this.transition = next.then(() => undefined, () => undefined);
        return next;
    }

    private assertHealthy(): void {
        if (this.failed) {
            throw this.failed;
        }
    }

    private fail(error: Error): void {
        if (this.failed) {
            return;
        }
        this.failed = error;
        this.clearInflightTimers();
        for (const completion of this.completions.values()) {
            completion.reject(error);
        }
        this.completions.clear();
        this.onError(error);
    }

    private normalizeError(error: unknown): Error {
        return error instanceof Error ? error : new Error(String(error));
    }
}

export function decodePackedUtf8(value: string): string {
    if ([...value].some(character => character.charCodeAt(0) > 0xff)) {
        return value;
    }
    const decoded = Buffer.from(value, 'latin1').toString('utf8');
    return decoded.includes('\ufffd') ? value : decoded;
}

function gitBlobHash(content: string): string {
    return createHash('sha1')
        .update(`blob ${content.length}\0${content}`)
        .digest('hex');
}
