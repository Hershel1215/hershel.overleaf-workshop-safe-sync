const assert = require('assert');
const {
    FairTaskScheduler,
    SchedulerBackpressureError,
} = require('../out/sync/fairTaskScheduler');

describe('fair document scheduler', () => {
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
});
