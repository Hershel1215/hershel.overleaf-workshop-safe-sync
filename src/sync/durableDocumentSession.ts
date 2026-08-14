/* eslint-disable @typescript-eslint/naming-convention */
import { createHash, randomUUID } from 'crypto';
import { DocumentJournal, DocumentOperationDisposition } from './documentJournal';
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
    onChange: (
        content: string,
        revision: number,
        acknowledgedContent: string,
        hasPendingOperations: boolean,
        sequence: number,
    ) => void;
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
    /** Ordinal of the session state `content` was read from. */
    sequence: number;
}

export interface DocumentOperationInspection {
    disposition: DocumentOperationDisposition;
    content: string;
    /** Ordinal of the session state `content` was read from. */
    sequence: number;
}

/**
 * Ordinals for published content states, counted across every session in this
 * extension host rather than per session. A document's session is closed and
 * re-opened by recovery and reconnect, and a consumer holding an ordinal from
 * the previous session would otherwise read the new session's states — which
 * restart from zero — as older than what it already has, and refuse every one
 * of them for the life of the document.
 */
let nextPublishedContentSequence = 0;

/** Expected acknowledgement cancellation while a recoverable WAL owner retires. */
export class DocumentSessionRetiredError extends Error {}

/** The WAL cannot prove that an operation id belongs to this document history. */
export class DocumentOperationAncestryError extends Error {}

/** Overleaf protocol binding whose state transitions are committed to a WAL. */
export class DurableDocumentSession {
    private readonly documentId: string;
    private readonly sendUpdate: (update: DurableLegacyDocumentUpdate) => Promise<void>;
    private readonly getSourceId: () => string | undefined;
    private readonly createOperationId: () => string;
    private readonly onChange: DurableDocumentSessionOptions['onChange'];
    private readonly onError: (error: Error) => void;
    private readonly retryDelayMs: number;
    private readonly fatalDelayMs: number;
    private readonly completions = new Map<string, Set<Completion>>();
    private readonly bufferedUpdates = new Map<number, DurableLegacyDocumentUpdate>();
    private readonly transportSends = new Set<Promise<void>>();
    private transition: Promise<void> = Promise.resolve();
    private inflightWire?: DurableLegacyDocumentUpdate;
    private retryTimer?: NodeJS.Timeout;
    private fatalTimer?: NodeJS.Timeout;
    private closePromise?: Promise<void>;
    private lastHashTimestamp = 0;
    /**
     * Ordinal of the current content state, advanced on every transition this
     * session publishes. The revision cannot serve here: it only moves when
     * the server acknowledges, so a burst of local operations produces many
     * distinct contents under one revision and a consumer that reads them out
     * of order has no way to tell which one is current.
     */
    private contentSequence = ++nextPublishedContentSequence;
    private failed?: Error;
    private transportSuspended = false;
    private closing = false;
    private closed = false;

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

    /** Ordinal of the content state this session currently exposes. */
    get sequence(): number {
        return this.contentSequence;
    }

    get isFailed(): boolean {
        return this.failed !== undefined;
    }

    get hasPendingOperations(): boolean {
        return this.journal.hasPendingOperations;
    }

    get pendingOperationIds(): readonly string[] {
        return this.journal.pendingOperationIds;
    }

    get lastKnownPath(): string | undefined {
        return this.journal.lastKnownPath;
    }

    bindPath(path: string): Promise<void> {
        return this.journal.bindPath(path, true);
    }

    get isTransportSuspended(): boolean {
        return this.transportSuspended;
    }

    /** Keep the WAL owner writable while the project socket is unavailable. */
    suspendTransport(): void {
        if (this.closing || this.closed) {
            return;
        }
        this.transportSuspended = true;
        this.clearInflightTimers();
    }

    /**
     * Rebase the live WAL owner on the authoritative reconnect snapshot before
     * allowing its fixed outgoing operation back onto the wire.
     */
    resumeTransport(
        authoritativeContent: string,
        authoritativeRevision: number,
        catchUpUpdates: DurableLegacyDocumentUpdate[],
    ): Promise<void> {
        return this.enqueue(async () => {
            this.assertHealthy();
            if (this.closing || this.closed) {
                throw new DocumentSessionRetiredError('Document session is closed');
            }
            for (const update of catchUpUpdates) {
                if (update.doc !== this.documentId || update.v !== this.revision) {
                    throw new Error(
                        `Catch-up update version ${update.v} does not match expected ${this.revision}`,
                    );
                }
                this.bufferedUpdates.delete(update.v);
                await this.processUpdate(update, false);
            }
            if (
                this.revision !== authoritativeRevision
                || this.acknowledgedContent !== authoritativeContent
            ) {
                throw new Error(
                    'Operation history does not reproduce the authoritative reconnect snapshot',
                );
            }
            while (this.bufferedUpdates.has(this.revision)) {
                const next = this.bufferedUpdates.get(this.revision)!;
                this.bufferedUpdates.delete(this.revision);
                await this.processUpdate(next, false);
            }
            this.transportSuspended = false;
            const outgoing = this.journal.currentOutgoingOperation;
            if (outgoing) {
                await this.startOutgoing(outgoing, true);
            }
        }).catch(error => {
            const normalized = this.normalizeError(error);
            if (!(normalized instanceof DocumentSessionRetiredError)) {
                this.fail(normalized);
            }
            throw normalized;
        });
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
        operationId?: string,
    ): Promise<AcceptedDocumentOperation> {
        return this.acceptPlannedSnapshot(currentContent => {
            if (currentContent === ancestor) {
                return target;
            }
            const currentOperation = TextOperation.fromDiff(ancestor, currentContent);
            const requestedOperation = TextOperation.fromDiff(ancestor, target);
            const [, requestedPrime] = TextOperation.transform(currentOperation, requestedOperation);
            return requestedPrime.apply(currentContent);
        }, barrier, operationId);
    }

    inspectOperation(opId: string): DocumentOperationInspection {
        return {
            disposition: this.journal.operationDisposition(opId),
            content: this.journal.content,
            sequence: this.contentSequence,
        };
    }

    /** Observe one already-journaled logical operation in this connection generation. */
    observeOperation(opId: string): Promise<string> {
        if (this.closed) {
            return Promise.reject(new DocumentSessionRetiredError('Document session is closed'));
        }
        if (this.failed) {
            return Promise.reject(this.failed);
        }
        switch (this.journal.operationDisposition(opId)) {
            case 'settled':
                return Promise.resolve(this.journal.content);
            case 'pending':
                return this.createCompletion(opId);
            case 'unknown':
                return Promise.reject(new DocumentOperationAncestryError(
                    `Cannot prove disposition of document operation ${opId}`,
                ));
        }
    }

    receiveUpdate(update: DurableLegacyDocumentUpdate): Promise<void> {
        if (this.closed) {
            return Promise.resolve();
        }
        return this.enqueue(async () => {
            if (this.closed) {
                return;
            }
            this.assertHealthy();
            if (update.doc !== this.documentId || update.v < this.revision) {
                return;
            }
            if (this.transportSuspended) {
                const existing = this.bufferedUpdates.get(update.v);
                if (existing && JSON.stringify(existing) !== JSON.stringify(update)) {
                    throw new Error(`Conflicting buffered updates at revision ${update.v}`);
                }
                this.bufferedUpdates.set(update.v, update);
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

    /** Wait until every wire send started by this generation has settled. */
    async waitForTransportIdle(): Promise<void> {
        for (;;) {
            await this.transition;
            const sends = [...this.transportSends];
            if (sends.length === 0) {
                return;
            }
            await Promise.allSettled(sends);
            // An acknowledgement can be queued by the same server turn that
            // settles applyOtUpdate. Process it before exposing disposition.
            await this.transition;
            if (this.transportSends.size === 0) {
                return;
            }
        }
    }

    dispose(): void {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.clearInflightTimers();
        const error = new DocumentSessionRetiredError(
            'Document session disposed before acknowledgement',
        );
        for (const completions of this.completions.values()) {
            for (const completion of completions) {
                completion.reject(error);
            }
        }
        this.completions.clear();
    }

    close(): Promise<void> {
        if (!this.closePromise) {
            this.closing = true;
            this.clearInflightTimers();
            this.closePromise = (async () => {
                await this.transition;
                await this.waitForTransportIdle();
                await this.transition;
                this.dispose();
                await this.transition;
                await this.journal.close();
            })();
        }
        return this.closePromise;
    }

    private acceptPlannedSnapshot(
        planner: (currentContent: string) => string,
        barrier: boolean,
        operationId?: string,
    ): Promise<AcceptedDocumentOperation> {
        if (this.closing || this.closed || this.failed) {
            return Promise.reject(
                this.failed ?? new DocumentSessionRetiredError('Document session is closed'),
            );
        }
        return new Promise<AcceptedDocumentOperation>((resolve, reject) => {
            this.enqueue(async () => {
                this.assertHealthy();
                if (operationId !== undefined) {
                    const disposition = this.journal.operationDisposition(operationId);
                    if (disposition !== 'unknown') {
                        resolve({
                            opId: operationId,
                            content: this.journal.content,
                            acknowledgement: this.observeOperation(operationId),
                            sequence: this.contentSequence,
                        });
                        return;
                    }
                }
                const content = planner(this.journal.content);
                const operation = TextOperation.fromDiff(this.journal.content, content);
                if (operation.isNoop) {
                    const current = this.journal.content;
                    resolve({
                        content: current,
                        acknowledgement: Promise.resolve(current),
                        sequence: this.contentSequence,
                    });
                    return;
                }
                const opId = operationId ?? this.createOperationId();
                const submission = await this.journal.commitLocalOperation(operation, opId, barrier);
                if (this.closing || this.closed) {
                    const retired = Promise.reject<string>(new DocumentSessionRetiredError(
                        'Document session closed after journal commit',
                    ));
                    void retired.catch(() => {});
                    // The journal already moved; nothing will publish this
                    // state, so claim its ordinal here rather than hand back a
                    // content newer than the sequence that labels it.
                    this.contentSequence = ++nextPublishedContentSequence;
                    resolve({
                        opId,
                        content: this.journal.content,
                        acknowledgement: retired,
                        sequence: this.contentSequence,
                    });
                    return;
                }
                const acknowledgement = this.createCompletion(opId);
                this.notifyChange();
                // The local-operation WAL append above is the durability
                // boundary. Return its fixed opId before transport metadata or
                // socket work can fail, so callers never mistake a journaled
                // operation for one that is safe to derive and submit again.
                resolve({
                    opId,
                    content: this.journal.content,
                    acknowledgement,
                    sequence: this.contentSequence,
                });
                if (submission.send) {
                    await this.startOutgoing(submission.send);
                }
            }).catch(error => {
                const normalized = this.normalizeError(error);
                reject(normalized);
                if (!(normalized instanceof DocumentSessionRetiredError)) {
                    this.fail(normalized);
                }
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
                for (const completion of this.completions.get(id) ?? []) {
                    completion.resolve(this.journal.content);
                }
                this.completions.delete(id);
            }
            if (result.send && sendNext) {
                await this.startOutgoing(result.send);
            }
        }
        return result.send;
    }

    private async startOutgoing(outgoing: OutgoingOperation, recovering = false): Promise<void> {
        if (this.closing || this.closed) {
            return;
        }
        const sources = [...this.journal.transportSources];
        const sourceId = this.getSourceId();
        if (sourceId && !sources.includes(sourceId)) {
            sources.push(sourceId);
        }
        const previousSubmittedRevision = this.journal.previousSubmittedRevision;
        await this.journal.commitTransportState(sources, outgoing.revision);
        if (this.closing || this.closed) {
            return;
        }

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
        if (this.transportSuspended) {
            return;
        }
        this.scheduleInflightTimers();
        // Journal durability is the caller-visible boundary. A transient
        // transport rejection must leave the same source-tagged wire in
        // flight so the retry path can resend it idempotently.
        void this.trackTransportSend(update);
    }

    private trackTransportSend(update: DurableLegacyDocumentUpdate): Promise<void> {
        let tracked: Promise<void>;
        tracked = this.sendWire(update)
            .finally(() => this.transportSends.delete(tracked));
        this.transportSends.add(tracked);
        return tracked;
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
        if (this.closing || this.closed || this.transportSuspended) {
            return;
        }
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
        }
        this.retryTimer = setTimeout(() => {
            this.enqueue(async () => {
                if (
                    this.closing
                    || this.closed
                    || this.transportSuspended
                    || !this.inflightWire
                    || this.failed
                ) {
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
                // ACK processing shares this transition queue. Never hold it
                // behind a socket Promise: the server may have accepted the
                // retry even when the transport callback never settles.
                if (this.transportSends.size === 0) {
                    void this.trackTransportSend({ ...this.inflightWire, dupIfSource: sources });
                }
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
        this.contentSequence = ++nextPublishedContentSequence;
        this.onChange(
            this.content,
            this.revision,
            this.acknowledgedContent,
            this.hasPendingOperations,
            this.contentSequence,
        );
    }

    private createCompletion(opId: string): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            const completion = {resolve, reject};
            const current = this.completions.get(opId) ?? new Set<Completion>();
            current.add(completion);
            this.completions.set(opId, current);
        });
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
        if (this.closed || this.failed) {
            return;
        }
        this.failed = error;
        this.clearInflightTimers();
        for (const completions of this.completions.values()) {
            for (const completion of completions) {
                completion.reject(error);
            }
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
