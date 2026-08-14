const assert = require('assert');
const {
    CONFLICT_DIRECTORY,
    isEditableTextCandidate,
    isHardIgnored,
    readPastTruncationWindow,
    shouldIgnore,
} = require('../out/sync/syncPolicy');

function scriptedReader(reads) {
    const waits = [];
    let served = 0;
    return {
        waits,
        readCount: () => served,
        read: async () => reads[Math.min(served++, reads.length - 1)],
        wait: async milliseconds => {
            waits.push(milliseconds);
        },
    };
}

describe('safe sync policy', () => {
    it('hard-ignores conflict artifacts even with an empty user ignore list', () => {
        assert.strictEqual(isHardIgnored(`${CONFLICT_DIRECTORY}/unresolved/a.tex`), true);
        assert.strictEqual(shouldIgnore(`${CONFLICT_DIRECTORY}/unresolved/a.tex`, []), true);
    });

    it('retains the standard LaTeX build artifact ignores', () => {
        assert.strictEqual(shouldIgnore('paper.aux'), true);
        assert.strictEqual(shouldIgnore('.output/paper.pdf'), true);
        assert.strictEqual(shouldIgnore('.overleaf/settings.json', []), true);
        assert.strictEqual(shouldIgnore('.hershel-safe-sync/history/revision/main.tex', []), true);
        assert.strictEqual(shouldIgnore(
            '.hershel-safe-sync.staging-550e8400-e29b-41d4-a716-446655440000/.owner.json',
            [],
        ), true);
        assert.strictEqual(shouldIgnore('.hershel-safe-sync.staging-user-notes/file.tex', []), false);
        assert.strictEqual(shouldIgnore('.latexmkrc'), false);
        assert.strictEqual(shouldIgnore('figures/result.pdf'), false);
    });

    it('reads a rewrite-in-place truncation window as the bytes that follow it', async () => {
        const reader = scriptedReader(['', '', 'chapter one\nchapter two\n']);
        const content = await readPastTruncationWindow({
            read: reader.read,
            wait: reader.wait,
            previousContent: 'chapter one\n',
        });
        assert.strictEqual(content, 'chapter one\nchapter two\n');
        assert.strictEqual(reader.readCount(), 3);
        assert.deepStrictEqual(reader.waits, [40, 40]);
    });

    it('accepts an emptiness that holds still', async () => {
        const reader = scriptedReader(['']);
        const content = await readPastTruncationWindow({
            read: reader.read,
            wait: reader.wait,
            previousContent: 'chapter one\n',
            confirmations: 3,
        });
        assert.strictEqual(content, '');
        assert.strictEqual(reader.waits.length, 3);
    });

    it('does not re-read a document that was already empty', async () => {
        const reader = scriptedReader(['']);
        const content = await readPastTruncationWindow({
            read: reader.read,
            wait: reader.wait,
            previousContent: '',
        });
        assert.strictEqual(content, '');
        assert.deepStrictEqual(reader.waits, []);
    });

    it('reports a delete observed inside the window without waiting further', async () => {
        const reader = scriptedReader(['', undefined]);
        const content = await readPastTruncationWindow({
            read: reader.read,
            wait: reader.wait,
            previousContent: 'chapter one\n',
        });
        assert.strictEqual(content, undefined);
        assert.deepStrictEqual(reader.waits, [40]);
    });

    it('stops confirming once the replica it was reading for is gone', async () => {
        const reader = scriptedReader(['', 'chapter one\nchapter two\n']);
        const content = await readPastTruncationWindow({
            read: reader.read,
            wait: reader.wait,
            previousContent: 'chapter one\n',
            isCurrent: () => false,
        });
        assert.strictEqual(content, '');
        assert.deepStrictEqual(reader.waits, []);
    });

    it('classifies editable UTF-8 documents conservatively', () => {
        const encode = value => new TextEncoder().encode(value);
        assert.strictEqual(isEditableTextCandidate('sections/main.tex', encode('中文 😀')), true);
        assert.strictEqual(isEditableTextCandidate('notes.md', encode('# notes')), true);
        assert.strictEqual(isEditableTextCandidate('figure.svg', encode('<svg/>')), false);
        assert.strictEqual(isEditableTextCandidate('image.png', Uint8Array.from([0, 1, 2])), false);
    });
});
