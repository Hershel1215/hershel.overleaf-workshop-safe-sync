const assert = require('assert');
const { ProjectionTracker } = require('../out/sync/projectionTracker');

function ids() {
    let next = 0;
    return () => `projection-${++next}`;
}

describe('projection provenance', () => {
    it('does not turn a matching plugin write into a user mutation', () => {
        const tracker = new ProjectionTracker(ids());
        const receipt = tracker.record('main.tex', { kind: 'change', hash: 'one' });
        assert.deepStrictEqual(
            tracker.observe('main.tex', { kind: 'change', hash: 'one' }),
            { kind: 'projection', projectionId: receipt.id },
        );
        assert.deepStrictEqual(
            tracker.observe('main.tex', { kind: 'change', hash: 'one' }),
            { kind: 'projection', projectionId: receipt.id },
        );
    });

    it('handles coalesced watcher events by matching the newest receipt', () => {
        const tracker = new ProjectionTracker(ids());
        const skipped = tracker.record('main.tex', { kind: 'change', hash: 'one' });
        const newest = tracker.record('main.tex', { kind: 'change', hash: 'two' });
        assert.deepStrictEqual(
            tracker.observe('main.tex', { kind: 'change', hash: 'two' }),
            { kind: 'projection', projectionId: newest.id },
        );
        assert.deepStrictEqual(tracker.serialize(), []);
        assert.strictEqual(tracker.hasReceipt(skipped.id), false);
        assert.strictEqual(tracker.hasReceipt(newest.id), true);
    });

    it('classifies content that differs from all receipts as a mutation', () => {
        const tracker = new ProjectionTracker(ids());
        tracker.record('main.tex', { kind: 'change', hash: 'projected' });
        assert.deepStrictEqual(
            tracker.observe('main.tex', { kind: 'change', hash: 'writer' }),
            { kind: 'mutation' },
        );
        assert.deepStrictEqual(tracker.serialize(), []);
    });

    it('restores pending projection evidence before observation begins', () => {
        const before = new ProjectionTracker(ids());
        const receipt = before.record('figure.png', {
            kind: 'create',
            hash: 'binary',
            identity: '7:9',
        });
        const after = new ProjectionTracker(ids());
        after.restore(before.serialize());
        assert.deepStrictEqual(
            after.observe('figure.png', {
                kind: 'create',
                hash: 'binary',
                identity: '7:9',
            }),
            { kind: 'projection', projectionId: receipt.id },
        );
    });

    it('matches a persisted OT projection rebased over later editor input', () => {
        const tracker = new ProjectionTracker(() => 'merge-receipt');
        tracker.recordTextMerge('editor:main.tex', 'ab', 'aRb');

        const classification = tracker.observe('editor:main.tex', {
            kind: 'change',
            hash: 'ignored-for-operation-receipt',
            ancestor: 'aLb',
            content: 'aRLb',
        });

        assert.deepStrictEqual(classification, {
            kind: 'projection',
            projectionId: 'merge-receipt',
        });
    });

    it('does not match an unrelated editor edit to an OT projection receipt', () => {
        const tracker = new ProjectionTracker(() => 'merge-receipt');
        tracker.recordTextMerge('editor:main.tex', 'ab', 'aRb');

        assert.deepStrictEqual(tracker.observe('editor:main.tex', {
            kind: 'change',
            ancestor: 'aLb',
            content: 'aXLb',
        }), {kind: 'mutation'});
    });
});
