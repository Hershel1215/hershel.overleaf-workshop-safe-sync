const assert = require('assert');
const {
    CONFLICT_DIRECTORY,
    isEditableTextCandidate,
    isHardIgnored,
    shouldIgnore,
} = require('../out/sync/syncPolicy');

describe('safe sync policy', () => {
    it('hard-ignores conflict artifacts even with an empty user ignore list', () => {
        assert.strictEqual(isHardIgnored(`${CONFLICT_DIRECTORY}/unresolved/a.tex`), true);
        assert.strictEqual(shouldIgnore(`${CONFLICT_DIRECTORY}/unresolved/a.tex`, []), true);
    });

    it('retains the standard LaTeX build artifact ignores', () => {
        assert.strictEqual(shouldIgnore('paper.aux'), true);
        assert.strictEqual(shouldIgnore('.output/paper.pdf'), true);
        assert.strictEqual(shouldIgnore('.overleaf/settings.json', []), true);
        assert.strictEqual(shouldIgnore('.latexmkrc'), false);
        assert.strictEqual(shouldIgnore('figures/result.pdf'), false);
    });

    it('classifies editable UTF-8 documents conservatively', () => {
        const encode = value => new TextEncoder().encode(value);
        assert.strictEqual(isEditableTextCandidate('sections/main.tex', encode('中文 😀')), true);
        assert.strictEqual(isEditableTextCandidate('notes.md', encode('# notes')), true);
        assert.strictEqual(isEditableTextCandidate('figure.svg', encode('<svg/>')), false);
        assert.strictEqual(isEditableTextCandidate('image.png', Uint8Array.from([0, 1, 2])), false);
    });
});
