const assert = require('assert');
const { BufferedDocumentInput } = require('../out/sync/bufferedDocumentInput');

describe('buffered editor input', () => {
    it('combines continuous snapshots into one journal commit', async () => {
        const commits = [];
        const input = new BufferedDocumentInput({
            idleCommitMs: 5,
            maxCommitMs: 50,
            onError: error => { throw error; },
            commit: async (ancestor, target, barrier) => {
                commits.push({ ancestor, target, barrier });
                return { content: target, acknowledgement: Promise.resolve(target) };
            },
        });
        input.update('a', 'ab');
        input.update('a', 'abc');
        input.update('a', 'abcd');
        await new Promise(resolve => setTimeout(resolve, 15));
        assert.deepStrictEqual(commits, [{ ancestor: 'a', target: 'abcd', barrier: false }]);
        input.dispose();
    });

    it('forces a commit at the hard limit during uninterrupted input', async () => {
        const commits = [];
        const input = new BufferedDocumentInput({
            idleCommitMs: 20,
            maxCommitMs: 30,
            onError: error => { throw error; },
            commit: async (ancestor, target) => {
                commits.push({ ancestor, target });
                return { content: target, acknowledgement: Promise.resolve(target) };
            },
        });
        input.update('', '1');
        await new Promise(resolve => setTimeout(resolve, 10));
        input.update('', '12');
        await new Promise(resolve => setTimeout(resolve, 10));
        input.update('', '123');
        await new Promise(resolve => setTimeout(resolve, 15));
        assert.deepStrictEqual(commits, [{ ancestor: '', target: '123' }]);
        input.dispose();
    });

    it('flushes buffered input at an explicit durable consistency barrier', async () => {
        const commits = [];
        const input = new BufferedDocumentInput({
            idleCommitMs: 1000,
            maxCommitMs: 2000,
            onError: error => { throw error; },
            commit: async (ancestor, target, barrier) => {
                commits.push({ ancestor, target, barrier });
                return { content: target, acknowledgement: Promise.resolve(target) };
            },
        });
        input.update('base', 'local');
        await input.flush(true);
        assert.deepStrictEqual(commits, [{ ancestor: 'base', target: 'local', barrier: true }]);
        assert.strictEqual(input.hasBufferedInput, false);
        input.dispose();
    });

    it('reports and awaits an active journal commit at a later barrier', async () => {
        let release;
        let announce;
        const started = new Promise(resolve => { announce = resolve; });
        const gate = new Promise(resolve => { release = resolve; });
        const input = new BufferedDocumentInput({
            idleCommitMs: 1000,
            maxCommitMs: 2000,
            onError: error => { throw error; },
            commit: async (_ancestor, target) => {
                announce();
                await gate;
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
        });
        input.update('base', 'local');
        const first = input.flush(false);
        await started;
        assert.strictEqual(input.hasBufferedInput, true);
        let barrierFinished = false;
        const barrier = input.flush(true).then(() => { barrierFinished = true; });
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(barrierFinished, false);
        release();
        await Promise.all([first, barrier]);
        assert.strictEqual(input.hasBufferedInput, false);
        input.dispose();
    });

    it('commits input buffered while an earlier non-barrier commit is active', async () => {
        const commits = [];
        let releaseFirst;
        let announceFirst;
        const firstStarted = new Promise(resolve => { announceFirst = resolve; });
        const firstGate = new Promise(resolve => { releaseFirst = resolve; });
        const input = new BufferedDocumentInput({
            idleCommitMs: 1000,
            maxCommitMs: 2000,
            onError: error => { throw error; },
            commit: async (ancestor, target) => {
                commits.push({ancestor, target});
                if (commits.length === 1) {
                    announceFirst();
                    await firstGate;
                }
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
        });
        input.update('base', 'local-1');
        const firstFlush = input.flush(false);
        await firstStarted;
        input.update('local-1', 'local-2');
        const secondFlush = input.flush(false);
        releaseFirst();
        await Promise.all([firstFlush, secondFlush]);

        assert.deepStrictEqual(commits, [
            {ancestor: 'base', target: 'local-1'},
            {ancestor: 'local-1', target: 'local-2'},
        ]);
        assert.strictEqual(input.hasBufferedInput, false);
        input.dispose();
    });

    it('does not compose editor transitions across an unrelated event ancestor', async () => {
        const commits = [];
        const input = new BufferedDocumentInput({
            idleCommitMs: 1000,
            maxCommitMs: 2000,
            onError: error => { throw error; },
            commit: async (ancestor, target, barrier) => {
                commits.push({ancestor, target, barrier});
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
        });
        await input.updateTransition('base-a', 'target-a');
        await input.updateTransition('base-b', 'target-b');
        await input.flush(true);

        assert.deepStrictEqual(commits, [
            {ancestor: 'base-a', target: 'target-a', barrier: true},
            {ancestor: 'base-b', target: 'target-b', barrier: true},
        ]);
        input.dispose();
    });
});
