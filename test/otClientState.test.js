const assert = require('assert');
const { OtClientState } = require('../out/sync/otClientState');
const { TextOperation } = require('../out/sync/textOperation');

function makeEdit(base, start, deleteCount, insertion) {
    return new TextOperation()
        .retain(start)
        .remove(deleteCount)
        .insert(insertion)
        .retain(base.length - start - deleteCount);
}

function ids() {
    let value = 0;
    return () => `op-${++value}`;
}

describe('OtClientState', () => {
    it('queues one outstanding operation and composes later local operations', () => {
        const state = new OtClientState(4, 'ab', 'ab', ids());
        const first = state.submitOperation(makeEdit('ab', 1, 0, 'L'));
        assert.strictEqual(first.send.revision, 4);
        assert.strictEqual(first.send.operation.apply('ab'), 'aLb');

        const second = state.submitOperation(makeEdit('aLb', 2, 0, 'X'));
        assert.strictEqual(second.send, undefined);
        assert.strictEqual(state.visibleContent, 'aLXb');

        const acknowledged = state.receive(first.send.operation, 4, true);
        assert.deepStrictEqual(acknowledged.acknowledgedIds, ['op-1']);
        assert.strictEqual(acknowledged.send.revision, 5);
        assert.strictEqual(acknowledged.send.operation.apply('aLb'), 'aLXb');

        state.receive(acknowledged.send.operation, 5, true);
        assert.strictEqual(state.acknowledgedContent, 'aLXb');
        assert.strictEqual(state.visibleContent, 'aLXb');
    });

    it('rebases outstanding and pending edits behind an authoritative remote edit', () => {
        const state = new OtClientState(0, 'ab', 'ab', ids());
        const first = state.submitOperation(makeEdit('ab', 1, 0, 'L'));
        state.submitOperation(makeEdit('aLb', 2, 0, 'X'));

        const remote = makeEdit('ab', 1, 0, 'R');
        const remoteResult = state.receive(remote, 0, false);
        assert.strictEqual(remoteResult.localChanged, true);
        assert.strictEqual(state.acknowledgedContent, 'aRb');
        assert.strictEqual(state.visibleContent, 'aRLXb');

        const transformedOutstanding = state.outstandingOperation;
        assert.strictEqual(transformedOutstanding.apply('aRb'), 'aRLb');
        const ack = state.receive(transformedOutstanding, 1, true);
        assert.deepStrictEqual(ack.acknowledgedIds, [first.id]);
        assert.strictEqual(ack.send.operation.apply('aRLb'), 'aRLXb');
        state.receive(ack.send.operation, 2, true);
        assert.strictEqual(state.acknowledgedContent, 'aRLXb');
        assert.strictEqual(state.visibleContent, 'aRLXb');
    });

    it('restores a valid serialized operation queue', () => {
        const state = new OtClientState(7, 'base', 'base', ids());
        state.submitOperation(makeEdit('base', 4, 0, '-one'));
        state.submitOperation(makeEdit('base-one', 8, 0, '-two'));
        const restored = OtClientState.restore(state.serialize(), ids());
        assert.strictEqual(restored.currentRevision, 7);
        assert.strictEqual(restored.acknowledgedContent, 'base');
        assert.strictEqual(restored.visibleContent, 'base-one-two');
    });

    it('recovers an unacknowledged operation without duplicating it', () => {
        const state = new OtClientState(2, 'ab', 'ab', ids());
        state.submitOperation(makeEdit('ab', 1, 0, 'L'));
        const saved = state.serialize();

        const alreadyApplied = OtClientState.recover('aLb', 3, saved, ids());
        assert.strictEqual(alreadyApplied.send, undefined);
        assert.strictEqual(alreadyApplied.state.visibleContent, 'aLb');

        const needsReplay = OtClientState.recover('ab', 2, saved, ids());
        assert.strictEqual(needsReplay.send.operation.apply('ab'), 'aLb');
    });

    it('fails closed on concurrent recovery without authoritative catch-up operations', () => {
        const state = new OtClientState(0, 'ab', 'ab', ids());
        state.submitOperation(makeEdit('ab', 1, 0, 'L'));
        assert.throws(
            () => OtClientState.recover('aRb', 1, state.serialize(), ids()),
            /Cannot prove whether the saved local operation/,
        );
    });

    it('fails closed when the server revision moves backwards', () => {
        const state = new OtClientState(3, 'abc', 'abc', ids());
        assert.throws(
            () => OtClientState.recover('abc', 2, state.serialize(), ids()),
            /moved backwards/,
        );
    });
});
