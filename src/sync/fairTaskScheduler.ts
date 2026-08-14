export class SchedulerBackpressureError extends Error {}
export class DocumentSchedulerRetiredError extends Error {}

interface ScheduledTask<T> {
    run: () => Promise<T>;
    resolve: (value: T) => void;
    reject: (error: Error) => void;
}

/** A one-task-per-source round-robin scheduler for a single document. */
export class FairTaskScheduler {
    private readonly queues = new Map<string, ScheduledTask<unknown>[]>();
    private sourceCursor = 0;
    private pumping = false;
    private disposed = false;
    private activeSource?: string;
    private activeStartedAt?: number;
    private readonly retirementWaiters: Array<() => void> = [];

    constructor(private readonly maxPendingPerSource = 1024) {}

    get pendingCount(): number {
        return [...this.queues.values()].reduce((total, queue) => total + queue.length, 0);
    }

    get isIdle(): boolean {
        return !this.pumping && this.pendingCount === 0;
    }

    get activeTask(): {source: string; durationMs: number} | undefined {
        return this.activeSource === undefined || this.activeStartedAt === undefined
            ? undefined
            : {
                source: this.activeSource,
                durationMs: Date.now() - this.activeStartedAt,
            };
    }

    pendingCountFor(source: string): number {
        return this.queues.get(source)?.length ?? 0;
    }

    run<T>(source: string, task: () => Promise<T>): Promise<T> {
        if (this.disposed) {
            return Promise.reject(new Error('Document scheduler is disposed'));
        }
        const queue = this.queues.get(source) ?? [];
        if (queue.length >= this.maxPendingPerSource) {
            return Promise.reject(new SchedulerBackpressureError(
                `Safe-sync ${source} queue reached ${this.maxPendingPerSource} pending tasks`,
            ));
        }
        const result = new Promise<T>((resolve, reject) => {
            queue.push({
                run: task,
                resolve: value => resolve(value as T),
                reject,
            });
        });
        this.queues.set(source, queue);
        void this.pump();
        return result;
    }

    dispose(): void {
        this.disposed = true;
        const error = new DocumentSchedulerRetiredError(
            'Document scheduler disposed with pending work',
        );
        for (const queue of this.queues.values()) {
            for (const task of queue) {
                task.reject(error);
            }
        }
        this.queues.clear();
        if (!this.pumping) {
            this.resolveRetirementWaiters();
        }
    }

    /** Stop accepting work and wait for the one currently running task. */
    async disposeAndWait(): Promise<void> {
        this.dispose();
        if (!this.pumping) {
            return;
        }
        await new Promise<void>(resolve => this.retirementWaiters.push(resolve));
    }

    private async pump(): Promise<void> {
        if (this.pumping || this.disposed) {
            return;
        }
        this.pumping = true;
        try {
            while (!this.disposed && this.pendingCount > 0) {
                const sources = [...this.queues.keys()];
                if (sources.length === 0) {
                    return;
                }
                this.sourceCursor %= sources.length;
                const source = sources[this.sourceCursor];
                this.sourceCursor = (this.sourceCursor + 1) % sources.length;
                const queue = this.queues.get(source)!;
                const task = queue.shift();
                if (queue.length === 0) {
                    this.queues.delete(source);
                }
                if (!task) {
                    continue;
                }
                try {
                    this.activeSource = source;
                    this.activeStartedAt = Date.now();
                    task.resolve(await task.run());
                } catch (error) {
                    task.reject(error instanceof Error ? error : new Error(String(error)));
                } finally {
                    this.activeSource = undefined;
                    this.activeStartedAt = undefined;
                }
                await new Promise<void>(resolve => setImmediate(resolve));
            }
        } finally {
            this.pumping = false;
            if (!this.disposed && this.pendingCount > 0) {
                void this.pump();
            } else {
                this.resolveRetirementWaiters();
            }
        }
    }

    private resolveRetirementWaiters(): void {
        for (const resolve of this.retirementWaiters.splice(0)) {
            resolve();
        }
    }
}
