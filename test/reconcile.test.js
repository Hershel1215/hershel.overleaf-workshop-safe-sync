const assert = require('assert');
const { decideReconciliation, mergeConcurrentText } = require('../out/sync/reconcile');

function baseline(hash = 'base', fileType = 'doc', text = 'ab') {
    return { entityId: 'entity', fileType, hash, text: fileType === 'doc' ? text : undefined };
}

function snapshot(side, hash, fileType = 'doc', text = 'ab') {
    return {
        path: '/main.tex',
        entityId: side === 'remote' ? 'entity' : undefined,
        fileType,
        hash,
        text: fileType === 'doc' ? text : undefined,
    };
}

describe('reconciliation decisions', () => {
    it('propagates one-sided changes from a common baseline', () => {
        assert.strictEqual(
            decideReconciliation(
                baseline(),
                snapshot('local', 'base'),
                snapshot('remote', 'remote', 'doc', 'remote'),
            ).action,
            'pull',
        );
        assert.strictEqual(
            decideReconciliation(
                baseline(),
                snapshot('local', 'local', 'doc', 'local'),
                snapshot('remote', 'base'),
            ).action,
            'push',
        );
    });

    it('distinguishes causal deletes from delete/edit conflicts', () => {
        assert.strictEqual(
            decideReconciliation(baseline(), undefined, snapshot('remote', 'base')).action,
            'delete-remote',
        );
        assert.strictEqual(
            decideReconciliation(baseline(), undefined, snapshot('remote', 'changed')).action,
            'conflict',
        );
        assert.strictEqual(
            decideReconciliation(baseline(), snapshot('local', 'base'), undefined).action,
            'delete-local',
        );
        assert.strictEqual(
            decideReconciliation(baseline(), snapshot('local', 'changed'), undefined).action,
            'conflict',
        );
    });

    it('uses folder tree hashes to protect delete/edit overlaps', () => {
        const folderBaseline = {
            entityId: 'folder-1',
            fileType: 'folder',
            hash: 'folder',
            treeHash: 'old-tree',
        };
        const changedRemote = {
            path: '/chapters',
            entityId: 'folder-1',
            fileType: 'folder',
            hash: 'folder',
            treeHash: 'new-tree',
        };
        assert.strictEqual(
            decideReconciliation(folderBaseline, undefined, changedRemote).action,
            'conflict',
        );
    });

    it('OT-merges concurrent text with remote ordering priority', () => {
        const decision = decideReconciliation(
            baseline('base', 'doc', 'ab'),
            snapshot('local', 'local', 'doc', 'aLb'),
            snapshot('remote', 'remote', 'doc', 'aRb'),
        );
        assert.strictEqual(decision.action, 'merge-text');
        assert.strictEqual(decision.content, 'aRLb');
    });

    it('rebases a local edit that arrives while the first merge is being acknowledged', () => {
        const firstMerge = mergeConcurrentText('ab', 'aLb', 'aRb');
        assert.strictEqual(firstMerge, 'aRLb');
        const withLateLocalEdit = mergeConcurrentText('aLb', 'aLXb', firstMerge);
        assert.strictEqual(withLateLocalEdit, 'aRLXb');
    });

    it('never tries to merge concurrent binary changes', () => {
        const decision = decideReconciliation(
            baseline('base', 'file'),
            snapshot('local', 'local', 'file'),
            snapshot('remote', 'remote', 'file'),
        );
        assert.strictEqual(decision.action, 'conflict');
    });

    it('requires explicit resolution for unrelated same-path creations', () => {
        assert.strictEqual(
            decideReconciliation(
                undefined,
                snapshot('local', 'local'),
                snapshot('remote', 'remote'),
            ).action,
            'conflict',
        );
    });
});
