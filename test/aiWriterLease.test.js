const assert = require('assert');
const {
    AiWriterLeaseHeldError,
    AiWriterLeaseManager,
    InvalidAiWriterLeaseError,
} = require('../out/sync/aiWriterLease');

function ids() {
    let next = 0;
    return () => `lease-${++next}`;
}

describe('exclusive AI writer lease', () => {
    it('rejects a second AI transaction for the same document', () => {
        const manager = new AiWriterLeaseManager(1000, ids());
        manager.acquire('doc-1', 'codex', 4, 100);
        assert.throws(
            () => manager.acquire('doc-1', 'claude', 4, 101),
            error => error instanceof AiWriterLeaseHeldError,
        );
        assert.doesNotThrow(() => manager.acquire('doc-2', 'claude', 1, 101));
    });

    it('fences a stale writer after lease expiry and reacquisition', () => {
        const manager = new AiWriterLeaseManager(10, ids());
        const stale = manager.acquire('doc-1', 'codex', 4, 100);
        const current = manager.acquire('doc-1', 'claude', 5, 111);
        assert(current.fencingToken > stale.fencingToken);
        assert.throws(
            () => manager.assertCanCommit(
                stale.documentId,
                stale.leaseId,
                stale.fencingToken,
                112,
            ),
            error => error instanceof InvalidAiWriterLeaseError,
        );
    });

    it('covers the read-edit-commit transaction and supports heartbeat', () => {
        const manager = new AiWriterLeaseManager(10, ids());
        const lease = manager.acquire('doc-1', 'codex', 7, 100);
        const renewed = manager.heartbeat('doc-1', lease.leaseId, lease.fencingToken, 105);
        assert.strictEqual(renewed.expiresAt, 115);
        assert.strictEqual(
            manager.assertCanCommit('doc-1', lease.leaseId, lease.fencingToken, 114).baseRevision,
            7,
        );
        manager.release('doc-1', lease.leaseId, lease.fencingToken);
        assert.doesNotThrow(() => manager.acquire('doc-1', 'claude', 8, 115));
    });

    it('restores fencing state without allowing ABA reuse', () => {
        const before = new AiWriterLeaseManager(1000, ids());
        const lease = before.acquire('doc-1', 'codex', 2, 100);
        const state = before.serialize();
        const after = new AiWriterLeaseManager(1000, ids());
        after.restore(state);
        after.release('doc-1', lease.leaseId, lease.fencingToken);
        const next = after.acquire('doc-1', 'claude', 3, 101);
        assert(next.fencingToken > lease.fencingToken);
    });
});
