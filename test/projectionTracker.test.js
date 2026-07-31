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
        tracker.record('main.tex', { kind: 'change', hash: 'one' });
        const newest = tracker.record('main.tex', { kind: 'change', hash: 'two' });
        assert.deepStrictEqual(
            tracker.observe('main.tex', { kind: 'change', hash: 'two' }),
            { kind: 'projection', projectionId: newest.id },
        );
        assert.deepStrictEqual(tracker.serialize(), []);
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
});
