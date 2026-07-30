const assert = require('assert');
const { createHash } = require('crypto');
const { mkdir, mkdtemp, rm, writeFile } = require('fs/promises');
const os = require('os');
const path = require('path');
const { ReplicaLock, ReplicaLockedError } = require('../out/sync/replicaLock');
const { ReplicaStateStore } = require('../out/sync/replicaStateStore');

describe('replica persistence', () => {
    let root;

    beforeEach(async () => {
        root = await mkdtemp(path.join(os.tmpdir(), 'hershel-safe-sync-'));
    });

    afterEach(async () => {
        await rm(root, { recursive: true, force: true });
    });

    it('atomically saves and restores replica baseline state', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.initialized = true;
        state.entries['/main.tex'] = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'hash',
            text: 'content',
        };
        state.pendingAction = {
            id: 'action-1',
            kind: 'push',
            path: '/main.tex',
            startedAt: new Date(0).toISOString(),
            expectedRemoteHash: 'hash',
            targetHash: 'new-hash',
        };
        await store.save(state);

        const restored = await store.load();
        assert.strictEqual(restored.replicaId, state.replicaId);
        assert.deepStrictEqual(restored.entries, state.entries);
        assert.deepStrictEqual(restored.pendingAction, state.pendingAction);
    });

    it('allows only one active writer for a replica key', async () => {
        const first = await ReplicaLock.acquire(root, '/workspace', {
            heartbeatMs: 100000,
        });
        await assert.rejects(
            () => ReplicaLock.acquire(root, '/workspace', { heartbeatMs: 100000 }),
            ReplicaLockedError,
        );
        await first.release();

        const second = await ReplicaLock.acquire(root, '/workspace', {
            heartbeatMs: 100000,
        });
        await second.release();
    });

    it('takes over an expired writer lease without accepting an active one', async () => {
        const key = '/stale-workspace';
        const digest = createHash('sha256').update(key).digest('hex');
        const lockDirectory = path.join(root, 'safe-sync', 'locks', `${digest}.lock`);
        await mkdir(lockDirectory, { recursive: true });
        await writeFile(path.join(lockDirectory, 'owner.json'), JSON.stringify({
            instanceId: 'dead-owner', pid: 999999, heartbeat: 0,
        }));

        const replacement = await ReplicaLock.acquire(root, key, {
            ttlMs: 10,
            heartbeatMs: 100000,
        });
        replacement.releaseSync();
    });

    it('fails closed instead of replacing a corrupt durable baseline', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        await mkdir(path.dirname(store.statePath), { recursive: true });
        await writeFile(store.statePath, '{not-json');
        await assert.rejects(() => store.load(), SyntaxError);
    });
});
