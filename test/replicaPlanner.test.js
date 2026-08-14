const assert = require('assert');
const {
    attachTreeHashes,
    findReplicaMove,
    refreshConvergedBaseline,
    validatePendingRecovery,
} = require('../out/sync/replicaPlanner');
const { decideReconciliation } = require('../out/sync/reconcile');

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

    it('accepts a completed move with changed content when both target identities match', () => {
        const pending = {
            id: 'move-after-crash',
            kind: 'move-remote',
            path: 'old.tex',
            targetPath: 'new.tex',
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'common-base',
            expectedLocalIdentity: '1:2',
            expectedRemoteHash: 'common-base',
            targetHash: 'common-base',
            entityId: 'doc-1',
        };
        const local = new Map([[
            'new.tex',
            snap('new.tex', 'concurrent-local-edit', 'doc', undefined, '1:2'),
        ]]);
        const remote = new Map([[
            'new.tex',
            snap('new.tex', 'concurrent-remote-edit', 'doc', 'doc-1'),
        ]]);

        assert.deepStrictEqual(
            validatePendingRecovery(pending, local, remote),
            {kind: 'strict'},
        );
    });

    it('rejects a move target whose local inode or remote entity was replaced', () => {
        const pending = {
            id: 'move-after-crash',
            kind: 'move-remote',
            path: 'old.tex',
            targetPath: 'new.tex',
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'common-base',
            expectedLocalIdentity: '1:2',
            expectedRemoteHash: 'common-base',
            targetHash: 'common-base',
            entityId: 'doc-1',
        };
        const ownedLocal = new Map([[
            'new.tex',
            snap('new.tex', 'common-base', 'doc', undefined, '1:2'),
        ]]);
        const ownedRemote = new Map([[
            'new.tex',
            snap('new.tex', 'common-base', 'doc', 'doc-1'),
        ]]);

        assert.throws(
            () => validatePendingRecovery(
                pending,
                new Map([[
                    'new.tex',
                    snap('new.tex', 'common-base', 'doc', undefined, '9:9'),
                ]]),
                ownedRemote,
            ),
            /Cannot prove recovery ancestry for pending move/,
        );
        assert.throws(
            () => validatePendingRecovery(
                pending,
                ownedLocal,
                new Map([[
                    'new.tex',
                    snap('new.tex', 'common-base', 'doc', 'replacement-doc'),
                ]]),
            ),
            /Cannot prove recovery ancestry for pending move/,
        );
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

    it('keeps the last path mapping while its immutable document WAL is pending', () => {
        const current = state();
        current.entries['sections/main.tex'] = {
            entityId: 'pending-doc', fileType: 'doc', hash: 'base', text: 'BASE',
        };

        refreshConvergedBaseline(
            current,
            new Map(),
            new Map(),
            new Set(['pending-doc']),
        );

        assert.strictEqual(current.entries['sections/main.tex'].entityId, 'pending-doc');
    });

    it('does not duplicate a protected document mapping after an immutable-id move', () => {
        const current = state();
        current.entries['old/main.tex'] = {
            entityId: 'pending-doc', fileType: 'doc', hash: 'base', text: 'BASE',
        };
        const local = new Map([[
            'new/main.tex',
            snap('new/main.tex', 'moved', 'doc', undefined, '1:5'),
        ]]);
        const remote = new Map([[
            'new/main.tex',
            snap('new/main.tex', 'moved', 'doc', 'pending-doc'),
        ]]);

        refreshConvergedBaseline(current, local, remote, new Set(['pending-doc']));

        assert.strictEqual(current.entries['old/main.tex'], undefined);
        assert.strictEqual(current.entries['new/main.tex'].entityId, 'pending-doc');
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

    it('keeps a merge push pending after the remote write but before local writeback', () => {
        const pending = {
            id: 'merge-push', kind: 'push', path: 'main.tex', startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'local-branch', expectedRemoteHash: 'remote-branch',
            targetHash: 'merged-target',
        };
        const outcome = validatePendingRecovery(
            pending,
            new Map([['main.tex', snap('main.tex', 'local-branch')]]),
            new Map([['main.tex', snap('main.tex', 'merged-target', 'doc', 'doc-1')]]),
        );

        assert.deepStrictEqual(outcome, { kind: 'strict' });
        assert.notStrictEqual(pending.expectedLocalHash, pending.targetHash);
    });

    it('commits an exact landed direct push before treating a newer same-inode edit as a new delta', () => {
        const baseline = {
            entityId: 'doc-1', fileType: 'doc', hash: 'base', text: 'base text', localIdentity: '1:2',
        };
        const pending = {
            id: 'direct-push', kind: 'push', path: 'main.tex', startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'pushed', expectedRemoteHash: 'base', targetHash: 'pushed',
            entityId: 'doc-1',
        };
        const newerLocal = {
            ...snap('main.tex', 'newer-local', 'doc', undefined, '1:2'),
            text: 'pushed text plus another local edit',
        };
        const landedRemote = {
            ...snap('main.tex', 'pushed', 'doc', 'doc-1'),
            text: 'pushed text',
        };

        const recovery = validatePendingRecovery(
            pending,
            new Map([['main.tex', newerLocal]]),
            new Map([['main.tex', landedRemote]]),
            baseline,
        );

        assert.deepStrictEqual(recovery, {
            kind: 'recovered-push',
            baseline: {
                entityId: 'doc-1',
                fileType: 'doc',
                hash: 'pushed',
                text: 'pushed text',
                treeHash: undefined,
                localIdentity: '1:2',
            },
        });
        assert.deepStrictEqual(
            decideReconciliation(recovery.baseline, newerLocal, landedRemote),
            { action: 'push' },
        );
    });

    it('does not claim a push completed when only local remains at target and remote advanced', () => {
        const pending = {
            id: 'push', kind: 'push', path: 'main.tex', startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'target', expectedRemoteHash: 'base', targetHash: 'target',
        };
        assert.throws(() => validatePendingRecovery(
            pending,
            new Map([['main.tex', snap('main.tex', 'target')]]),
            new Map([['main.tex', snap('main.tex', 'remote-third', 'doc', 'doc-1')]]),
        ), /Cannot prove recovery ancestry/);
    });

    it('commits a landed document pull before treating a newer remote revision as a new delta', () => {
        const current = state();
        current.entries['sections/appendix.tex'] = {
            entityId: 'doc-1', fileType: 'doc', hash: 'base', text: 'base text', localIdentity: '1:2',
        };
        const pending = {
            id: 'pull-1', kind: 'pull', path: 'sections/appendix.tex',
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'base', expectedRemoteHash: 'pulled', targetHash: 'pulled',
        };
        const localTarget = {
            ...snap('sections/appendix.tex', 'pulled', 'doc', undefined, '1:2'),
            text: 'pulled text',
        };
        const remoteThird = {
            ...snap('sections/appendix.tex', 'third', 'doc', 'doc-1'),
            text: 'third text',
        };
        const local = new Map([['sections/appendix.tex', localTarget]]);
        const remote = new Map([['sections/appendix.tex', remoteThird]]);

        const recovery = validatePendingRecovery(
            pending, local, remote, current.entries['sections/appendix.tex'],
        );
        assert.strictEqual(recovery.kind, 'recovered-pull');
        current.entries['sections/appendix.tex'] = recovery.baseline;

        assert.deepStrictEqual(current.entries['sections/appendix.tex'], {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'pulled',
            text: 'pulled text',
            treeHash: undefined,
            localIdentity: '1:2',
        });
        assert.deepStrictEqual(decideReconciliation(
            current.entries['sections/appendix.tex'], localTarget, remoteThird,
        ), { action: 'pull' });
    });

    it('commits a landed binary pull before treating newer remote bytes as a new delta', () => {
        const current = state();
        current.entries['figures/result.png'] = {
            entityId: 'file-1', fileType: 'file', hash: 'base', localIdentity: '2:3',
        };
        const pending = {
            id: 'pull-binary', kind: 'pull', path: 'figures/result.png',
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'base', expectedRemoteHash: 'pulled', targetHash: 'pulled',
        };
        const localTarget = snap('figures/result.png', 'pulled', 'file', undefined, '2:3');
        const remoteThird = snap('figures/result.png', 'third', 'file', 'file-1');
        const local = new Map([['figures/result.png', localTarget]]);
        const remote = new Map([['figures/result.png', remoteThird]]);

        const recovery = validatePendingRecovery(
            pending, local, remote, current.entries['figures/result.png'],
        );
        assert.strictEqual(recovery.kind, 'recovered-pull');
        current.entries['figures/result.png'] = recovery.baseline;

        assert.deepStrictEqual(current.entries['figures/result.png'], {
            entityId: 'file-1',
            fileType: 'file',
            hash: 'pulled',
            text: undefined,
            treeHash: undefined,
            localIdentity: '2:3',
        });
        assert.deepStrictEqual(decideReconciliation(
            current.entries['figures/result.png'], localTarget, remoteThird,
        ), { action: 'pull' });
    });

    it('still fails closed when the pull destination is neither its pre-state nor target', () => {
        const baseline = {
            entityId: 'doc-1', fileType: 'doc', hash: 'base', text: 'base text',
        };
        const pending = {
            id: 'pull-1', kind: 'pull', path: 'main.tex',
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'base', expectedRemoteHash: 'target', targetHash: 'target',
        };
        assert.throws(() => validatePendingRecovery(
            pending,
            new Map([['main.tex', snap('main.tex', 'unrelated')]]),
            new Map([['main.tex', snap('main.tex', 'target', 'doc', 'doc-1')]]),
            baseline,
        ), /Cannot prove recovery ancestry/);
    });

    it('fails closed when a landed pull now points at a replacement remote entity', () => {
        const baseline = {
            entityId: 'doc-1', fileType: 'doc', hash: 'base', text: 'base text',
        };
        const pending = {
            id: 'pull-1', kind: 'pull', path: 'main.tex',
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'base', expectedRemoteHash: 'target', targetHash: 'target',
        };
        assert.throws(() => validatePendingRecovery(
            pending,
            new Map([['main.tex', snap('main.tex', 'target')]]),
            new Map([['main.tex', snap('main.tex', 'third', 'doc', 'replacement-doc')]]),
            baseline,
        ), /Cannot prove recovery ancestry/);
    });

    it('recovers the persisted UniDFKD appendix pull journal without weakening identity checks', () => {
        const baseline = {
            entityId: '6a6aeb82ee2d179ed3303093',
            fileType: 'doc',
            hash: '28f30d53ab4180a883937a38e6587e8526d5b549b64ae9a8b5f6a2620edc4071',
            text: 'old appendix',
        };
        const pending = {
            id: '635525d7-0a1b-4b40-8728-5673b3db6393',
            kind: 'pull',
            path: 'sections/appendix.tex',
            startedAt: '2026-07-30T16:43:29.205Z',
            expectedLocalHash: baseline.hash,
            expectedRemoteHash: 'bd7bcc89b36a3e80a26b8d09dd29598abff1a499b980c930ac4d480d3752f61c',
            targetHash: 'bd7bcc89b36a3e80a26b8d09dd29598abff1a499b980c930ac4d480d3752f61c',
        };
        const local = new Map([['sections/appendix.tex', {
            ...snap('sections/appendix.tex', pending.targetHash, 'doc', undefined, '66307:105260143'),
            text: 'landed appendix',
        }]]);
        const remote = new Map([['sections/appendix.tex', {
            ...snap('sections/appendix.tex', 'later-overleaf-hash', 'doc', baseline.entityId),
            text: 'later appendix',
        }]]);

        const recovery = validatePendingRecovery(pending, local, remote, baseline);
        assert.strictEqual(recovery.kind, 'recovered-pull');
        assert.strictEqual(recovery.baseline.hash, pending.targetHash);
        assert.strictEqual(recovery.baseline.text, 'landed appendix');
    });
});
