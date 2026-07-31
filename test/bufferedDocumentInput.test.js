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

    it('turns a remote arrival into a durable commit barrier', async () => {
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
});
