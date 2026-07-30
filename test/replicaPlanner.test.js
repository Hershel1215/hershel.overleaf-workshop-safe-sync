const assert = require('assert');
const {
    attachTreeHashes,
    findReplicaMove,
    refreshConvergedBaseline,
    validatePendingRecovery,
} = require('../out/sync/replicaPlanner');

function snap(path, hash, fileType = 'doc', entityId, localIdentity) {
    return { path, hash, fileType, entityId, localIdentity, text: fileType === 'doc' ? hash : undefined };
}

function state() {
    return {
        schemaVersion: 1,
        replicaId: 'replica',
        initialized: true,
        updatedAt: new Date(0).toISOString(),
        entries: {},
        conflicts: {},
    };
}

describe('replica planning', () => {
    it('computes folder hashes from relative descendant paths', () => {
        const first = new Map([
            ['chapters', snap('chapters', 'folder', 'folder')],
            ['chapters/a.tex', snap('chapters/a.tex', 'a')],
        ]);
        const renamed = new Map([
            ['moved', snap('moved', 'folder', 'folder')],
            ['moved/a.tex', snap('moved/a.tex', 'a')],
        ]);
        attachTreeHashes(first);
        attachTreeHashes(renamed);
        assert.strictEqual(first.get('chapters').treeHash, renamed.get('moved').treeHash);
    });

    it('tracks an authoritative remote rename by entity id', () => {
        const baseline = {
            'old.tex': {
                entityId: 'doc-1', fileType: 'doc', hash: 'same', text: 'same', localIdentity: '1:2',
            },
        };
        const local = new Map([['old.tex', snap('old.tex', 'edited', 'doc', undefined, '1:2')]]);
        const remote = new Map([['new.tex', snap('new.tex', 'same', 'doc', 'doc-1')]]);
        assert.deepStrictEqual(findReplicaMove(baseline, local, remote), {
            action: 'move-local', oldPath: 'old.tex', newPath: 'new.tex', entityId: 'doc-1',
        });
    });

    it('tracks a local rename by persisted filesystem identity', () => {
        const baseline = {
            'old.tex': {
                entityId: 'doc-1', fileType: 'doc', hash: 'same', text: 'same', localIdentity: '1:2',
            },
        };
        const local = new Map([['new.tex', snap('new.tex', 'edited', 'doc', undefined, '1:2')]]);
        const remote = new Map([['old.tex', snap('old.tex', 'same', 'doc', 'doc-1')]]);
        assert.deepStrictEqual(findReplicaMove(baseline, local, remote), {
            action: 'move-remote', oldPath: 'old.tex', newPath: 'new.tex', entityId: 'doc-1',
        });
    });

    it('only refreshes baselines for converged, unconflicted paths', () => {
        const current = state();
        current.entries['blocked.tex'] = { entityId: 'blocked', fileType: 'doc', hash: 'old', text: 'old' };
        current.conflicts['blocked.tex'] = {
            path: 'blocked.tex', artifactDirectory: 'artifact', reason: 'test', createdAt: new Date(0).toISOString(),
        };
        const local = new Map([
            ['main.tex', snap('main.tex', 'same', 'doc', undefined, '1:3')],
            ['blocked.tex', snap('blocked.tex', 'local')],
        ]);
        const remote = new Map([
            ['main.tex', snap('main.tex', 'same', 'doc', 'doc-1')],
            ['blocked.tex', snap('blocked.tex', 'remote', 'doc', 'blocked')],
        ]);
        refreshConvergedBaseline(current, local, remote);
        assert.strictEqual(current.entries['main.tex'].entityId, 'doc-1');
        assert.strictEqual(current.entries['main.tex'].localIdentity, '1:3');
        assert.strictEqual(current.entries['blocked.tex'].hash, 'old');
    });

    it('fails closed when a pending action has an unrelated endpoint state', () => {
        const pending = {
            id: 'action', kind: 'push', path: 'main.tex', startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'target', expectedRemoteHash: 'base', targetHash: 'target',
        };
        assert.throws(
            () => validatePendingRecovery(
                pending,
                new Map([['main.tex', snap('main.tex', 'target')]]),
                new Map([['main.tex', snap('main.tex', 'unrelated', 'doc', 'doc-1')]]),
            ),
            /Cannot prove recovery ancestry/,
        );
    });

    it('accepts only the journaled pre-state or post-state for a pending push', () => {
        const pending = {
            id: 'action', kind: 'push', path: 'main.tex', startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'target', expectedRemoteHash: 'base', targetHash: 'target',
        };
        assert.doesNotThrow(() => validatePendingRecovery(
            pending,
            new Map([['main.tex', snap('main.tex', 'target')]]),
            new Map([['main.tex', snap('main.tex', 'base', 'doc', 'doc-1')]]),
        ));
        assert.doesNotThrow(() => validatePendingRecovery(
            pending,
            new Map([['main.tex', snap('main.tex', 'target')]]),
            new Map([['main.tex', snap('main.tex', 'target', 'doc', 'doc-1')]]),
        ));
        assert.throws(() => validatePendingRecovery(
            pending,
            new Map(),
            new Map([['main.tex', snap('main.tex', 'base', 'doc', 'doc-1')]]),
        ));
    });
});
