import { AcceptedDocumentOperation } from './durableDocumentSession';

export interface BufferedDocumentInputOptions {
    idleCommitMs?: number;
    maxCommitMs?: number;
    onError: (error: Error) => void;
    commit: (
        ancestor: string,
        target: string,
        barrier: boolean,
    ) => Promise<AcceptedDocumentOperation>;
    onIdle?: () => Promise<void>;
}

// The editor changes immediately; these delays affect only WAL/network
// submission. Remote VS Code workbenches can deliver one physical input burst
// as callbacks separated by more than 50 ms, so a shorter window degenerates
// into one fdatasync per callback. Remote OT arrivals are transformed over the
// buffered event-time ancestor without shortening this window. Explicit
// consistency boundaries (for example an AI transaction begin) still flush,
// while uninterrupted typing is durable within the hard bound.
const DEFAULT_IDLE_COMMIT_MS = 350;
const DEFAULT_MAX_COMMIT_MS = 750;

/** Composes continuous editor snapshots into bounded durable OT submissions. */
export class BufferedDocumentInput {
    private readonly idleCommitMs: number;
    private readonly maxCommitMs: number;
    private readonly onError: (error: Error) => void;
    private readonly commit: BufferedDocumentInputOptions['commit'];
    private readonly onIdle?: BufferedDocumentInputOptions['onIdle'];
    private ancestor?: string;
    private target?: string;
    private activeCommit?: Promise<AcceptedDocumentOperation>;
    private idleTimer?: NodeJS.Timeout;
    private maxTimer?: NodeJS.Timeout;
    private disposed = false;

    constructor(options: BufferedDocumentInputOptions) {
        this.idleCommitMs = options.idleCommitMs ?? DEFAULT_IDLE_COMMIT_MS;
        this.maxCommitMs = options.maxCommitMs ?? DEFAULT_MAX_COMMIT_MS;
        this.onError = options.onError;
        this.commit = options.commit;
        this.onIdle = options.onIdle;
        if (
            this.idleCommitMs < 0
            || this.maxCommitMs <= 0
            || this.idleCommitMs > this.maxCommitMs
        ) {
            throw new Error('Invalid document input batching window');
        }
    }

    get hasBufferedInput(): boolean {
        return !this.disposed
            && (this.ancestor !== undefined || this.activeCommit !== undefined);
    }

    update(ancestor: string, target: string): void {
        if (this.disposed) {
            return;
        }
        if (this.ancestor === undefined) {
            this.ancestor = ancestor;
        }
        this.target = target;
        this.schedule();
    }

    /**
     * Append an editor transition whose ancestor was captured when VS Code
     * emitted the change event. Contiguous transitions compose in memory. If
     * another source has changed the visible editor between two observations,
     * commit the earlier causal chain before starting the next one.
     */
    async updateTransition(ancestor: string, target: string): Promise<void> {
        if (this.disposed) {
            return;
        }
        if (
            this.ancestor !== undefined
            && this.target !== undefined
            && this.target !== ancestor
        ) {
            await this.flush(true);
        }
        if (this.disposed) {
            return;
        }
        if (this.ancestor === undefined) {
            this.ancestor = ancestor;
        }
        this.target = target;
        this.schedule();
    }

    async flush(barrier = false): Promise<AcceptedDocumentOperation | undefined> {
        if (this.disposed) {
            return undefined;
        }
        let latest: AcceptedDocumentOperation | undefined;
        let startedCommit = false;
        try {
            while (!this.disposed) {
                if (this.activeCommit) {
                    latest = await this.activeCommit;
                } else if (this.ancestor !== undefined && this.target !== undefined) {
                    latest = await this.startCommit(barrier);
                    startedCommit = true;
                } else {
                    break;
                }
                if (!barrier && startedCommit) {
                    break;
                }
            }
            if (
                !this.disposed
                && this.activeCommit === undefined
                && this.ancestor === undefined
            ) {
                await this.onIdle?.();
            }
            return latest;
        } catch (error) {
            const normalized = error instanceof Error ? error : new Error(String(error));
            this.onError(normalized);
            throw normalized;
        }
    }

    dispose(): void {
        this.disposed = true;
        this.clearTimers();
        this.ancestor = undefined;
        this.target = undefined;
    }

    async disposeAndWait(): Promise<void> {
        const active = this.activeCommit;
        this.dispose();
        if (active) {
            await active.catch(() => {});
        }
    }

    private async startCommit(barrier: boolean): Promise<AcceptedDocumentOperation> {
        if (this.ancestor === undefined || this.target === undefined) {
            throw new Error('Cannot start an empty document input commit');
        }
        const ancestor = this.ancestor;
        const target = this.target;
        this.ancestor = undefined;
        this.target = undefined;
        this.clearTimers();
        const active = Promise.resolve().then(() => this.commit(ancestor, target, barrier));
        this.activeCommit = active;
        try {
            return await active;
        } finally {
            if (this.activeCommit === active) {
                this.activeCommit = undefined;
            }
        }
    }

    private schedule(): void {
        if (this.idleTimer) {
            clearTimeout(this.idleTimer);
        }
        this.idleTimer = setTimeout(() => {
            void this.flush().catch(() => {});
        }, this.idleCommitMs);
        if (!this.maxTimer) {
            this.maxTimer = setTimeout(() => {
                void this.flush().catch(() => {});
            }, this.maxCommitMs);
        }
    }

    private clearTimers(): void {
        if (this.idleTimer) {
            clearTimeout(this.idleTimer);
            this.idleTimer = undefined;
        }
        if (this.maxTimer) {
            clearTimeout(this.maxTimer);
            this.maxTimer = undefined;
        }
    }
}
