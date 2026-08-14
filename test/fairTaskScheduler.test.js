const assert = require('assert');
const {
    FairTaskScheduler,
    SchedulerBackpressureError,
} = require('../out/sync/fairTaskScheduler');

describe('fair document scheduler', () => {
    it('reports queued observations for one causal source', async () => {
        const scheduler = new FairTaskScheduler();
        let release;
        const gate = new Promise(resolve => { release = resolve; });
        const running = scheduler.run('remote', async () => gate);
        const editor = scheduler.run('editor', async () => {});
        assert.strictEqual(scheduler.isIdle, false);
        assert.strictEqual(scheduler.activeTask.source, 'remote');
        assert(scheduler.activeTask.durationMs >= 0);
        assert.strictEqual(scheduler.pendingCountFor('editor'), 1);
        assert.strictEqual(scheduler.pendingCountFor('remote'), 0);
        release();
        await Promise.all([running, editor]);
        assert.strictEqual(scheduler.pendingCountFor('editor'), 0);
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(scheduler.isIdle, true);
        assert.strictEqual(scheduler.activeTask, undefined);
    });

    it('does not drain one continuous source before another source', async () => {
        const scheduler = new FairTaskScheduler();
        const order = [];
        const tasks = [];
        for (let index = 0; index < 4; index++) {
            tasks.push(scheduler.run('remote', async () => { order.push(`remote-${index}`); }));
        }
        tasks.push(scheduler.run('editor', async () => { order.push('editor'); }));
        tasks.push(scheduler.run('ai', async () => { order.push('ai'); }));
        await Promise.all(tasks);
        assert(order.indexOf('editor') < order.indexOf('remote-3'));
        assert(order.indexOf('ai') < order.indexOf('remote-3'));
        scheduler.dispose();
    });

    it('applies bounded backpressure to one noisy source', async () => {
        const scheduler = new FairTaskScheduler(1);
        let release;
        const gate = new Promise(resolve => { release = resolve; });
        const first = scheduler.run('ai', async () => gate);
        const second = scheduler.run('ai', async () => {});
        await assert.rejects(
            () => scheduler.run('ai', async () => {}),
            error => error instanceof SchedulerBackpressureError,
        );
        release();
        await Promise.all([first, second]);
        scheduler.dispose();
    });

    it('waits for the running task while rejecting queued work during retirement', async () => {
        const scheduler = new FairTaskScheduler();
        let release;
        let runningFinished = false;
        const gate = new Promise(resolve => { release = resolve; });
        const running = scheduler.run('remote', async () => {
            await gate;
            runningFinished = true;
        });
        const queued = scheduler.run('editor', async () => {
            throw new Error('queued task must not run');
        });

        const retirement = scheduler.disposeAndWait();
        await assert.rejects(queued, /disposed/);
        assert.strictEqual(runningFinished, false);
        release();
        await Promise.all([running, retirement]);
        assert.strictEqual(runningFinished, true);
    });
});
