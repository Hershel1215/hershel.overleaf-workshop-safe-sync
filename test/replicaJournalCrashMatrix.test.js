const assert = require('assert');
const { validatePendingRecovery } = require('../out/sync/replicaPlanner');

const STRICT = { kind: 'strict' };
const STARTED_AT = new Date(0).toISOString();

function snapshot(path, hash, entityId = 'entity-1') {
    return {
        path,
        hash,
        fileType: 'doc',
        entityId,
        text: hash,
    };
}

function entries(...snapshots) {
    return new Map(snapshots.map(item => [item.path, item]));
}

function pending(kind, fields) {
    return {
        id: `pending-${kind}`,
        kind,
        path: 'source.tex',
        startedAt: STARTED_AT,
        ...fields,
    };
}

function assertAllowed(action, local, remote) {
    assert.deepStrictEqual(validatePendingRecovery(action, local, remote), STRICT);
}

function assertRejected(action, local, remote) {
    assert.throws(
        () => validatePendingRecovery(action, local, remote),
        /Cannot prove recovery ancestry/,
    );
}

describe('replica journal crash recovery matrix', () => {
    it('accepts only push pre/post states, including a remote-absent create', () => {
        const action = pending('push', {
            expectedLocalHash: 'local-before',
            expectedRemoteHash: 'remote-before',
            targetHash: 'target',
        });

        assertAllowed(
            action,
            entries(snapshot('source.tex', 'local-before')),
            entries(snapshot('source.tex', 'remote-before')),
        );
        assertAllowed(
            action,
            entries(snapshot('source.tex', 'target')),
            entries(snapshot('source.tex', 'target')),
        );
        assertAllowed(
            action,
            entries(snapshot('source.tex', 'local-before')),
            entries(snapshot('source.tex', 'target')),
        );
        assertRejected(
            action,
            entries(snapshot('source.tex', 'local-before')),
            entries(snapshot('source.tex', 'unrelated')),
        );
        assertRejected(
            action,
            entries(snapshot('source.tex', 'unrelated')),
            entries(snapshot('source.tex', 'target')),
        );

        const create = pending('push', {
            expectedLocalHash: 'created',
            expectedRemoteHash: undefined,
            targetHash: 'created',
        });
        assertAllowed(create, entries(snapshot('source.tex', 'created')), new Map());
        assertAllowed(
            create,
            entries(snapshot('source.tex', 'created')),
            entries(snapshot('source.tex', 'created')),
        );
        assertRejected(
            create,
            entries(snapshot('source.tex', 'created')),
            entries(snapshot('source.tex', 'unrelated')),
        );
    });

    it('accepts only pull pre/post states, including a local-absent create', () => {
        const action = pending('pull', {
            expectedLocalHash: 'local-before',
            expectedRemoteHash: 'target',
            targetHash: 'target',
        });

        assertAllowed(
            action,
            entries(snapshot('source.tex', 'local-before')),
            entries(snapshot('source.tex', 'target')),
        );
        assertAllowed(
            action,
            entries(snapshot('source.tex', 'target')),
            entries(snapshot('source.tex', 'target')),
        );
        assertRejected(
            action,
            entries(snapshot('source.tex', 'unrelated')),
            entries(snapshot('source.tex', 'target')),
        );

        const create = pending('pull', {
            expectedLocalHash: undefined,
            expectedRemoteHash: 'created',
            targetHash: 'created',
        });
        assertAllowed(create, new Map(), entries(snapshot('source.tex', 'created')));
        assertAllowed(
            create,
            entries(snapshot('source.tex', 'created')),
            entries(snapshot('source.tex', 'created')),
        );
        assertRejected(
            create,
            entries(snapshot('source.tex', 'unrelated')),
            entries(snapshot('source.tex', 'created')),
        );
    });

    it('accepts delete-local before and after the local endpoint becomes absent', () => {
        const action = pending('delete-local', {
            expectedLocalHash: 'victim',
            expectedRemoteHash: undefined,
        });

        assertAllowed(action, entries(snapshot('source.tex', 'victim')), new Map());
        assertAllowed(action, new Map(), new Map());
        assertRejected(action, entries(snapshot('source.tex', 'unrelated')), new Map());
        assertRejected(action, new Map(), entries(snapshot('source.tex', 'unrelated')));
    });

    it('accepts delete-remote before and after the remote endpoint becomes absent', () => {
        const action = pending('delete-remote', {
            expectedLocalHash: undefined,
            expectedRemoteHash: 'victim',
        });

        assertAllowed(action, new Map(), entries(snapshot('source.tex', 'victim')));
        assertAllowed(action, new Map(), new Map());
        assertRejected(action, new Map(), entries(snapshot('source.tex', 'unrelated')));
        assertRejected(action, entries(snapshot('source.tex', 'unrelated')), new Map());
    });

    it('checks both source and target around a move-local crash', () => {
        const action = pending('move-local', {
            targetPath: 'target.tex',
            expectedLocalHash: 'local-source',
            expectedRemoteHash: 'remote-target',
            targetHash: 'local-source',
            entityId: 'entity-1',
        });
        const remote = entries(snapshot('target.tex', 'remote-target'));

        assertAllowed(action, entries(snapshot('source.tex', 'local-source')), remote);
        assertAllowed(action, entries(snapshot('target.tex', 'local-source')), remote);
        assertRejected(action, entries(snapshot('source.tex', 'unrelated')), remote);
        assertRejected(action, entries(snapshot('target.tex', 'unrelated')), remote);
    });

    it('checks both source and target around a move-remote crash', () => {
        const action = pending('move-remote', {
            targetPath: 'target.tex',
            expectedLocalHash: 'local-target',
            expectedRemoteHash: 'remote-source',
            targetHash: 'local-target',
            entityId: 'entity-1',
        });
        const local = entries(snapshot('target.tex', 'local-target'));

        assertAllowed(action, local, entries(snapshot('source.tex', 'remote-source')));
        assertAllowed(action, local, entries(snapshot('target.tex', 'remote-source')));
        assertRejected(action, local, entries(snapshot('source.tex', 'unrelated')));
        assertRejected(action, local, entries(snapshot('target.tex', 'unrelated')));
    });

    it('accepts converge-local before and after local writeback only', () => {
        const action = pending('converge-local', {
            expectedLocalHash: 'local-variants',
            expectedRemoteHash: 'remote-stable',
            targetHash: 'merged-local',
        });

        assertAllowed(
            action,
            entries(snapshot('source.tex', 'local-variants')),
            entries(snapshot('source.tex', 'remote-stable')),
        );
        assertAllowed(
            action,
            entries(snapshot('source.tex', 'merged-local')),
            entries(snapshot('source.tex', 'remote-stable')),
        );
        assertRejected(
            action,
            entries(snapshot('source.tex', 'unrelated')),
            entries(snapshot('source.tex', 'remote-stable')),
        );
        assertRejected(
            action,
            entries(snapshot('source.tex', 'merged-local')),
            entries(snapshot('source.tex', 'unrelated')),
        );
    });

    it('accepts resolve-conflict before, during, and after target convergence', () => {
        const action = pending('resolve-conflict', {
            expectedLocalHash: 'local-before',
            expectedRemoteHash: 'remote-before',
            targetHash: 'resolved',
        });

        assertAllowed(
            action,
            entries(snapshot('source.tex', 'local-before')),
            entries(snapshot('source.tex', 'remote-before')),
        );
        assertAllowed(
            action,
            entries(snapshot('source.tex', 'local-before')),
            entries(snapshot('source.tex', 'resolved')),
        );
        assertAllowed(
            action,
            entries(snapshot('source.tex', 'resolved')),
            entries(snapshot('source.tex', 'resolved')),
        );
        assertRejected(
            action,
            entries(snapshot('source.tex', 'unrelated')),
            entries(snapshot('source.tex', 'resolved')),
        );
        assertRejected(
            action,
            entries(snapshot('source.tex', 'resolved')),
            entries(snapshot('source.tex', 'unrelated')),
        );
    });
});
