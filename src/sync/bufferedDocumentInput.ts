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
}

/** Composes continuous editor snapshots into bounded durable OT submissions. */
export class BufferedDocumentInput {
    private readonly idleCommitMs: number;
    private readonly maxCommitMs: number;
    private readonly onError: (error: Error) => void;
    private readonly commit: BufferedDocumentInputOptions['commit'];
    private ancestor?: string;
    private target?: string;
    private idleTimer?: NodeJS.Timeout;
    private maxTimer?: NodeJS.Timeout;

    constructor(options: BufferedDocumentInputOptions) {
        this.idleCommitMs = options.idleCommitMs ?? 50;
        this.maxCommitMs = options.maxCommitMs ?? 250;
        this.onError = options.onError;
        this.commit = options.commit;
        if (
            this.idleCommitMs < 0
            || this.maxCommitMs <= 0
            || this.idleCommitMs > this.maxCommitMs
        ) {
            throw new Error('Invalid document input batching window');
        }
    }

    get hasBufferedInput(): boolean {
        return this.ancestor !== undefined;
    }

    update(ancestor: string, target: string): void {
        if (this.ancestor === undefined) {
            this.ancestor = ancestor;
        }
        this.target = target;
        this.schedule();
    }

    async flush(barrier = false): Promise<AcceptedDocumentOperation | undefined> {
        if (this.ancestor === undefined || this.target === undefined) {
            return undefined;
        }
        const ancestor = this.ancestor;
        const target = this.target;
        this.ancestor = undefined;
        this.target = undefined;
        this.clearTimers();
        try {
            return await this.commit(ancestor, target, barrier);
        } catch (error) {
            const normalized = error instanceof Error ? error : new Error(String(error));
            this.onError(normalized);
            throw normalized;
        }
    }

    dispose(): void {
        this.clearTimers();
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
