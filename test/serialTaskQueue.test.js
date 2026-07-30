const assert = require('assert');
const { SerialTaskQueue } = require('../out/sync/serialTaskQueue');

describe('SerialTaskQueue', () => {
    it('never overlaps queued state mutations', async () => {
        const queue = new SerialTaskQueue();
        const events = [];
        let releaseFirst;
        const gate = new Promise(resolve => { releaseFirst = resolve; });

        assert.strictEqual(queue.isIdle, true);

        const first = queue.run(async () => {
            events.push('first-start');
            await gate;
            events.push('first-end');
        });
        const second = queue.run(async () => {
            events.push('second-start');
            events.push('second-end');
        });

        assert.strictEqual(queue.isIdle, false);
        await Promise.resolve();
        assert.deepStrictEqual(events, ['first-start']);
        assert.strictEqual(queue.isIdle, false);
        releaseFirst();
        await Promise.all([first, second]);
        assert.deepStrictEqual(events, [
            'first-start',
            'first-end',
            'second-start',
            'second-end',
        ]);
        assert.strictEqual(queue.isIdle, true);
    });

    it('continues after a failed task without overlapping it', async () => {
        const queue = new SerialTaskQueue();
        const failed = queue.run(async () => { throw new Error('expected'); });
        assert.strictEqual(queue.isIdle, false);
        await assert.rejects(failed, /expected/);
        assert.strictEqual(queue.isIdle, true);
        assert.strictEqual(await queue.run(async () => 'next'), 'next');
        assert.strictEqual(queue.isIdle, true);
    });
});
