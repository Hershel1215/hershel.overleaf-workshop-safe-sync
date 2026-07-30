const assert = require('assert');
const { TextOperation } = require('../out/sync/textOperation');

function makeEdit(base, start, deleteCount, insertion) {
    return new TextOperation()
        .retain(start)
        .remove(deleteCount)
        .insert(insertion)
        .retain(base.length - start - deleteCount);
}

function randomInt(maxExclusive) {
    return Math.floor(Math.random() * maxExclusive);
}

function randomText(maxLength = 6) {
    const alphabet = 'abcXYZ012';
    const length = randomInt(maxLength + 1);
    let result = '';
    for (let index = 0; index < length; index++) {
        result += alphabet[randomInt(alphabet.length)];
    }
    return result;
}

function randomOperation(base) {
    const operation = new TextOperation();
    let cursor = 0;
    while (cursor < base.length) {
        if (Math.random() < 0.35) {
            operation.insert(randomText(3));
        }
        const remaining = base.length - cursor;
        const count = 1 + randomInt(Math.min(3, remaining));
        if (Math.random() < 0.35) {
            operation.remove(count);
        } else {
            operation.retain(count);
        }
        cursor += count;
    }
    if (Math.random() < 0.5) {
        operation.insert(randomText(3));
    }
    return operation;
}

describe('TextOperation', () => {
    it('builds and applies normalized edits', () => {
        const operation = new TextOperation()
            .retain(2)
            .remove(2)
            .insert('XY')
            .retain(2);
        assert.strictEqual(operation.apply('abcdef'), 'abXYef');
        assert.strictEqual(operation.baseLength, 6);
        assert.strictEqual(operation.targetLength, 6);
    });

    it('round-trips snapshot diffs including Unicode and newlines', () => {
        const before = '第一行\nemoji: 😀\nlast';
        const after = '第一行已改\nemoji: 😀!\n末行';
        const operation = TextOperation.fromDiff(before, after);
        assert.strictEqual(operation.apply(before), after);
    });

    it('fails closed on unpaired UTF-16 input that Overleaf cannot encode', () => {
        assert.throws(
            () => TextOperation.fromDiff('valid', `invalid\ud800`),
            /unpaired UTF-16/,
        );
    });

    it('composes sequential operations', () => {
        const base = 'abcdef';
        const first = makeEdit(base, 2, 2, 'XY');
        const middle = first.apply(base);
        const second = makeEdit(middle, 1, 3, 'Q');
        assert.strictEqual(first.compose(second).apply(base), second.apply(middle));
    });

    it('transforms concurrent overlapping operations to one result', () => {
        const base = 'abcdef';
        const left = makeEdit(base, 1, 3, 'LEFT');
        const right = makeEdit(base, 2, 3, 'RIGHT');
        const [leftPrime, rightPrime] = TextOperation.transform(left, right);
        assert.strictEqual(
            leftPrime.apply(right.apply(base)),
            rightPrime.apply(left.apply(base)),
        );
    });

    it('uses deterministic left priority for concurrent inserts', () => {
        const base = 'ab';
        const left = makeEdit(base, 1, 0, 'L');
        const right = makeEdit(base, 1, 0, 'R');
        const [leftPrime, rightPrime] = TextOperation.transform(left, right);
        assert.strictEqual(leftPrime.apply(right.apply(base)), 'aLRb');
        assert.strictEqual(rightPrime.apply(left.apply(base)), 'aLRb');
    });

    it('converts to and from legacy positional edits', () => {
        const base = 'alpha βeta';
        const target = 'ALPHA β!';
        const operation = TextOperation.fromDiff(base, target);
        const edits = operation.toLegacyEdits(base);
        const restored = TextOperation.fromLegacyEdits(base, edits);
        assert.strictEqual(restored.apply(base), target);
        assert.strictEqual(restored.equals(operation), true);
    });

    it('preserves legacy edit anchors needed by later concurrent operations', () => {
        const base = 'abcdeRf';
        const operation = TextOperation.fromJSON([
            { kind: 'retain', count: 3 },
            { kind: 'insert', text: 'R' },
            { kind: 'delete', count: 2 },
            { kind: 'retain', count: 1 },
            { kind: 'delete', count: 1 },
        ]);
        const restored = TextOperation.fromLegacyEdits(base, operation.toLegacyEdits(base));
        assert.deepStrictEqual(restored.toJSON(), operation.toJSON());
    });

    it('rejects legacy deletions that do not match the base', () => {
        assert.throws(
            () => TextOperation.fromLegacyEdits('abc', [{ p: 1, d: 'x' }]),
            /does not match/,
        );
    });

    it('satisfies convergence for randomized operation pairs', () => {
        for (let iteration = 0; iteration < 500; iteration++) {
            const base = randomText(18);
            const left = randomOperation(base);
            const right = randomOperation(base);
            const [leftPrime, rightPrime] = TextOperation.transform(left, right);
            assert.strictEqual(
                leftPrime.apply(right.apply(base)),
                rightPrime.apply(left.apply(base)),
                `failed convergence on iteration ${iteration}`,
            );
        }
    });

    it('satisfies composition for randomized sequential operations', () => {
        for (let iteration = 0; iteration < 500; iteration++) {
            const base = randomText(18);
            const first = randomOperation(base);
            const middle = first.apply(base);
            const second = randomOperation(middle);
            assert.strictEqual(
                first.compose(second).apply(base),
                second.apply(middle),
                `failed composition on iteration ${iteration}`,
            );
        }
    });
});
