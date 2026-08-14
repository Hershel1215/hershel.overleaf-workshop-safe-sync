import { AcceptedDocumentOperation } from './durableDocumentSession';
import { TextOperation } from './textOperation';

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
    /** A finite durability cut completed, possibly with newer input buffered. */
    onBoundary?: () => Promise<void>;
    /** Serialize timer-driven boundaries with the owning document scheduler. */
    scheduleBoundary?: (task: () => Promise<void>) => Promise<void>;
}

// The editor changes immediately; these delays affect only WAL/network
// submission. Remote VS Code workbenches can deliver one physical input burst
// as callbacks separated by more than 50 ms, so a shorter window degenerates
// into one fdatasync per callback. Remote OT arrivals are transformed over the
// buffered event-time ancestor without shortening this window. Explicit
// consistency boundaries (for example an AI transaction begin) still flush,
// while uninterrupted typing is durable within the hard bound.
const DEFAULT_IDLE_COMMIT_MS = 150;
const DEFAULT_MAX_COMMIT_MS = 300;

/** Composes continuous editor snapshots into bounded durable OT submissions. */
export class BufferedDocumentInput {
    private readonly idleCommitMs: number;
    private readonly maxCommitMs: number;
    private readonly onError: (error: Error) => void;
    private readonly commit: BufferedDocumentInputOptions['commit'];
    private readonly onIdle?: BufferedDocumentInputOptions['onIdle'];
    private readonly onBoundary?: BufferedDocumentInputOptions['onBoundary'];
    private readonly scheduleBoundary?: BufferedDocumentInputOptions['scheduleBoundary'];
    private ancestor?: string;
    private target?: string;
    /** Newest editor snapshot this batch has already submitted as an operation. */
    private submittedTarget?: string;
    private activeCommit?: Promise<AcceptedDocumentOperation>;
    private idleTimer?: NodeJS.Timeout;
    private maxTimer?: NodeJS.Timeout;
    private timerFlushScheduled = false;
    private scheduledTimerBarrier = false;
    private disposed = false;

    constructor(options: BufferedDocumentInputOptions) {
        this.idleCommitMs = options.idleCommitMs ?? DEFAULT_IDLE_COMMIT_MS;
        this.maxCommitMs = options.maxCommitMs ?? DEFAULT_MAX_COMMIT_MS;
        this.onError = options.onError;
        this.commit = options.commit;
        this.onIdle = options.onIdle;
        this.onBoundary = options.onBoundary;
        this.scheduleBoundary = options.scheduleBoundary;
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

    /**
     * The last editor snapshot handed to the OT session. A frozen observation
     * that ends here carries no unjournaled bytes, however stale its ancestor.
     */
    get lastSubmittedTarget(): string | undefined {
        return this.submittedTarget;
    }

    /** Wait for a durable commit already in flight without starting another. */
    async waitForActiveCommit(): Promise<void> {
        const active = this.activeCommit;
        if (active) {
            await active.catch(() => {});
        }
    }

    /** Rebase the buffered local suffix over the latest canonical OT head. */
    previewAgainst(canonical: string): string | undefined {
        if (
            this.disposed
            || this.activeCommit !== undefined
            || this.ancestor === undefined
            || this.target === undefined
        ) {
            return undefined;
        }
        if (canonical === this.ancestor) {
            return this.target;
        }
        const canonicalOperation = TextOperation.fromDiff(this.ancestor, canonical);
        const bufferedOperation = TextOperation.fromDiff(this.ancestor, this.target);
        const [, bufferedPrime] = TextOperation.transform(
            canonicalOperation,
            bufferedOperation,
        );
        return bufferedPrime.apply(canonical);
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

    /** Synchronously admit one contiguous event-time editor transition. */
    tryUpdateTransition(ancestor: string, target: string): boolean {
        if (this.disposed) {
            return false;
        }
        if (
            this.ancestor !== undefined
            && this.target !== undefined
            && this.target !== ancestor
        ) {
            // A queued earlier event can resume after a durability cut while a
            // later synchronous event has already started the next batch. The
            // chains are still contiguous in event order, so prepend the
            // missing transition instead of treating normal input as a fork.
            if (target === this.ancestor) {
                this.ancestor = ancestor;
                return true;
            }
            return false;
        }
        this.update(ancestor, target);
        return true;
    }

    /** Re-anchor an unjournaled suffix after its transformed preview is installed. */
    adoptProjectedPreview(canonical: string, projected: string): boolean {
        if (
            this.disposed
            || this.activeCommit !== undefined
            || this.previewAgainst(canonical) !== projected
        ) {
            return false;
        }
        this.ancestor = canonical;
        this.target = projected;
        return true;
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
        if (this.tryUpdateTransition(ancestor, target)) {
            return;
        }
        await this.flush(true);
        if (this.disposed) {
            return;
        }
        if (!this.tryUpdateTransition(ancestor, target)) {
            throw new Error('Editor transition remained non-contiguous after its durability boundary');
        }
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
                } else if (
                    !startedCommit
                    && this.ancestor !== undefined
                    && this.target !== undefined
                ) {
                    latest = await this.startCommit(barrier);
                    startedCommit = true;
                } else {
                    break;
                }
            }
            if (!this.disposed && barrier) {
                await this.onBoundary?.();
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
        this.submittedTarget = target;
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
            this.idleTimer = undefined;
            this.requestTimerFlush(false);
        }, this.idleCommitMs);
        if (!this.maxTimer) {
            this.maxTimer = setTimeout(() => {
                this.maxTimer = undefined;
                // The hard deadline is a real durability/projection boundary.
                // It commits a finite prefix; input arriving during that commit
                // belongs to the next batch and is OT-previewed at the boundary.
                // This prevents a continuous writer from starving projection.
                this.requestTimerFlush(true);
            }, this.maxCommitMs);
        }
    }

    private requestTimerFlush(barrier: boolean): void {
        if (this.disposed) {
            return;
        }
        this.scheduledTimerBarrier ||= barrier;
        if (this.timerFlushScheduled) {
            return;
        }
        this.timerFlushScheduled = true;
        let taskStarted = false;
        const task = async (): Promise<void> => {
            taskStarted = true;
            const requestedBarrier = this.scheduledTimerBarrier;
            this.scheduledTimerBarrier = false;
            this.timerFlushScheduled = false;
            await this.flush(requestedBarrier);
        };
        const scheduled = this.scheduleBoundary
            ? this.scheduleBoundary(task)
            : task();
        void scheduled.catch(error => {
            this.timerFlushScheduled = false;
            this.scheduledTimerBarrier = false;
            if (!taskStarted && !this.disposed) {
                this.onError(error instanceof Error ? error : new Error(String(error)));
            }
        });
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
