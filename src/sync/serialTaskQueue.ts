/** A minimal promise tail that prevents overlapping state mutations. */
export class SerialTaskQueue {
    private tail: Promise<void> = Promise.resolve();
    private pendingTaskCount = 0;

    get isIdle(): boolean {
        return this.pendingTaskCount === 0;
    }

    run<T>(task: () => Promise<T>): Promise<T> {
        this.pendingTaskCount++;
        const result = this.tail.then(task);
        this.tail = result.then(
            () => {
                this.pendingTaskCount--;
            },
            () => {
                this.pendingTaskCount--;
            },
        );
        return result;
    }
}
