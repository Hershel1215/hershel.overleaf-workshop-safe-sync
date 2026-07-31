/* eslint-disable @typescript-eslint/naming-convention */
import { createHash, randomUUID } from 'crypto';
import { DocumentJournal } from './documentJournal';
import { OutgoingOperation } from './otClientState';
import { LegacyTextEdit, TextOperation } from './textOperation';

export interface DurableLegacyDocumentUpdate {
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

export interface DurableDocumentSessionOptions {
    documentId: string;
    authoritativeContent: string;
    authoritativeRevision: number;
    journal: DocumentJournal;
    catchUpUpdates?: DurableLegacyDocumentUpdate[];
    sendUpdate: (update: DurableLegacyDocumentUpdate) => Promise<void>;
    getSourceId: () => string | undefined;
    createOperationId?: () => string;
    onChange: (content: string, revision: number, acknowledgedContent: string) => void;
    onError: (error: Error) => void;
    retryDelayMs?: number;
    fatalDelayMs?: number;
}

interface Completion {
    resolve: (content: string) => void;
    reject: (error: Error) => void;
}

export interface AcceptedDocumentOperation {
    opId?: string;
    content: string;
    acknowledgement: Promise<string>;
}

/** Overleaf protocol binding whose state transitions are committed to a WAL. */
export class DurableDocumentSession {
    private readonly documentId: string;
    private readonly sendUpdate: (update: DurableLegacyDocumentUpdate) => Promise<void>;
    private readonly getSourceId: () => string | undefined;
    private readonly createOperationId: () => string;
    private readonly onChange: (content: string, revision: number, acknowledgedContent: string) => void;
    private readonly onError: (error: Error) => void;
    private readonly retryDelayMs: number;
    private readonly fatalDelayMs: number;
    private readonly completions = new Map<string, Completion>();
    private readonly bufferedUpdates = new Map<number, DurableLegacyDocumentUpdate>();
    private transition: Promise<void> = Promise.resolve();
    private inflightWire?: DurableLegacyDocumentUpdate;
    private retryTimer?: NodeJS.Timeout;
    private fatalTimer?: NodeJS.Timeout;
    private lastHashTimestamp = 0;
    private failed?: Error;

    private constructor(
        options: DurableDocumentSessionOptions,
        private readonly journal: DocumentJournal,
    ) {
        this.documentId = options.documentId;
        this.sendUpdate = options.sendUpdate;
        this.getSourceId = options.getSourceId;
        this.createOperationId = options.createOperationId ?? randomUUID;
        this.onChange = options.onChange;
        this.onError = options.onError;
        this.retryDelayMs = options.retryDelayMs ?? 5000;
        this.fatalDelayMs = options.fatalDelayMs ?? 45000;
    }

    static async create(options: DurableDocumentSessionOptions): Promise<DurableDocumentSession> {
        const session = new DurableDocumentSession(options, options.journal);
        for (const update of options.catchUpUpdates ?? []) {
            if (update.v !== session.revision) {
                throw new Error(
                    `Catch-up update version ${update.v} does not match expected ${session.revision}`,
                );
            }
            await session.processUpdate({ ...update, doc: options.documentId }, false);
        }
        if (
            session.revision !== options.authoritativeRevision
            || session.acknowledgedContent !== options.authoritativeContent
        ) {
            throw new Error('Operation history does not reproduce the authoritative server snapshot');
        }
        session.notifyChange();
        const outgoing = session.journal.currentOutgoingOperation;
        if (outgoing) {
            await session.startOutgoing(outgoing, true);
        }
        return session;
    }

    get content(): string {
        return this.journal.content;
    }

    get acknowledgedContent(): string {
        return this.journal.acknowledgedContent;
    }

    get revision(): number {
        return this.journal.revision;
    }

    get isFailed(): boolean {
        return this.failed !== undefined;
    }

    submitSnapshot(content: string, barrier = false): Promise<void> {
        return this.acceptPlannedSnapshot(() => content, barrier)
            .then(accepted => accepted.acknowledgement)
            .then(() => undefined);
    }

    submitSnapshotAgainstAncestor(
        ancestor: string,
        target: string,
        barrier = false,
    ): Promise<string> {
        return this.acceptSnapshotAgainstAncestor(ancestor, target, barrier)
            .then(accepted => accepted.acknowledgement);
    }

    acceptSnapshotAgainstAncestor(
        ancestor: string,
        target: string,
        barrier = false,
    ): Promise<AcceptedDocumentOperation> {
        return this.acceptPlannedSnapshot(currentContent => {
            if (currentContent === ancestor) {
                return target;
            }
            const currentOperation = TextOperation.fromDiff(ancestor, currentContent);
            const requestedOperation = TextOperation.fromDiff(ancestor, target);
            const [, requestedPrime] = TextOperation.transform(currentOperation, requestedOperation);
            return requestedPrime.apply(currentContent);
        }, barrier);
    }

    receiveUpdate(update: DurableLegacyDocumentUpdate): Promise<void> {
        return this.enqueue(async () => {
            this.assertHealthy();
            if (update.doc !== this.documentId || update.v < this.revision) {
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

    async close(): Promise<void> {
        this.dispose();
        await this.journal.close();
    }

    private acceptPlannedSnapshot(
        planner: (currentContent: string) => string,
        barrier: boolean,
    ): Promise<AcceptedDocumentOperation> {
        if (this.failed) {
            return Promise.reject(this.failed);
        }
        return new Promise<AcceptedDocumentOperation>((resolve, reject) => {
            this.enqueue(async () => {
                this.assertHealthy();
                const content = planner(this.journal.content);
                const operation = TextOperation.fromDiff(this.journal.content, content);
                if (operation.isNoop) {
                    const current = this.journal.content;
                    resolve({ content: current, acknowledgement: Promise.resolve(current) });
                    return;
                }
                const opId = this.createOperationId();
                const submission = await this.journal.commitLocalOperation(operation, opId, barrier);
                let acknowledge: (content: string) => void;
                let rejectAcknowledgement: (error: Error) => void;
                const acknowledgement = new Promise<string>((resolveAcknowledgement, rejectPending) => {
                    acknowledge = resolveAcknowledgement;
                    rejectAcknowledgement = rejectPending;
                });
                this.completions.set(opId, {
                    resolve: content => acknowledge(content),
                    reject: error => rejectAcknowledgement(error),
                });
                this.notifyChange();
                if (submission.send) {
                    await this.startOutgoing(submission.send);
                }
                resolve({ opId, content: this.journal.content, acknowledgement });
            }).catch(error => {
                const normalized = this.normalizeError(error);
                reject(normalized);
                this.fail(normalized);
            });
        });
    }

    private async processUpdate(
        update: DurableLegacyDocumentUpdate,
        sendNext = true,
    ): Promise<OutgoingOperation | undefined> {
        if (update.error) {
            throw new Error(`Overleaf rejected document operation: ${update.error}`);
        }

        let operation: TextOperation;
        let ownOperation = false;
        if (update.op === undefined) {
            const outgoing = this.journal.currentOutgoingOperation;
            if (!outgoing) {
                throw new Error(`Unexpected acknowledgement at revision ${update.v}`);
            }
            operation = outgoing.operation;
            ownOperation = true;
        } else {
            operation = TextOperation.fromLegacyEdits(
                this.journal.acknowledgedContent,
                update.op,
                decodePackedUtf8,
            );
            ownOperation = update.meta?.source !== undefined
                && this.journal.transportSources.includes(update.meta.source);
        }

        const result = await this.journal.commitRemoteOperation(operation, update.v, ownOperation);
        if (ownOperation) {
            this.clearInflightTimers();
            this.inflightWire = undefined;
        }
        this.notifyChange();

        if (ownOperation) {
            for (const id of result.acknowledgedIds) {
                this.completions.get(id)?.resolve(this.journal.content);
                this.completions.delete(id);
            }
            if (result.send && sendNext) {
                await this.startOutgoing(result.send);
            }
        }
        return result.send;
    }

    private async startOutgoing(outgoing: OutgoingOperation, recovering = false): Promise<void> {
        const sources = [...this.journal.transportSources];
        const sourceId = this.getSourceId();
        if (sourceId && !sources.includes(sourceId)) {
            sources.push(sourceId);
        }
        const previousSubmittedRevision = this.journal.previousSubmittedRevision;
        await this.journal.commitTransportState(sources, outgoing.revision);

        const targetContent = outgoing.operation.apply(this.journal.acknowledgedContent);
        const now = Date.now();
        const includeHash = this.journal.previousSubmittedRevision === undefined
            || now - this.lastHashTimestamp >= 5000;
        const update: DurableLegacyDocumentUpdate = {
            doc: this.documentId,
            v: outgoing.revision,
            lastV: previousSubmittedRevision,
            op: outgoing.operation.toLegacyEdits(this.journal.acknowledgedContent),
            hash: includeHash ? gitBlobHash(targetContent) : undefined,
            dupIfSource: recovering && sources.length > 0 ? sources : undefined,
        };
        if (includeHash) {
            this.lastHashTimestamp = now;
        }
        this.inflightWire = update;
        this.scheduleInflightTimers();
        // Journal durability is the caller-visible boundary. A transient
        // transport rejection must leave the same source-tagged wire in
        // flight so the retry path can resend it idempotently.
        void this.sendWire(update);
    }

    private async sendWire(update: DurableLegacyDocumentUpdate): Promise<void> {
        try {
            await this.sendUpdate(update);
        } catch {
            // Keep the durable inflight operation. The retry timer resends it
            // with dupIfSource; a later ACK or the fatal ACK deadline decides
            // whether the document is acknowledged or isolated.
        }
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
                const sources = [...this.journal.transportSources];
                const sourceId = this.getSourceId();
                if (sourceId && !sources.includes(sourceId)) {
                    sources.push(sourceId);
                }
                await this.journal.commitTransportState(
                    sources,
                    this.journal.previousSubmittedRevision,
                );
                await this.sendWire({ ...this.inflightWire, dupIfSource: sources });
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
