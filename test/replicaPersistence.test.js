const assert = require('assert');
const { createHash } = require('crypto');
const fsPromises = require('fs/promises');
const { lstat, mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } = fsPromises;
const os = require('os');
const path = require('path');
const { ReplicaLock, ReplicaLockedError } = require('../out/sync/replicaLock');
const {DurableWal} = require('../out/sync/durableWal');
const {
    MAX_COMPLETED_AI_COMMITS,
    MAX_REPLICA_STATE_WAL_BYTES,
    MAX_REPLICA_STATE_WAL_RECORDS,
    ReplicaStateStore,
    recordCompletedAiCommit,
} = require('../out/sync/replicaStateStore');

function baselineDirectory(root) {
    const key = createHash('sha256')
        .update(`server\0project\0${path.resolve('/workspace')}`)
        .digest('hex');
    return path.join(root, 'safe-sync', 'baselines', key);
}

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
        state.detachedDocumentRecoveries = {
            'legacy-doc': {
                documentId: 'legacy-doc',
                artifactDirectory: '.overleaf/conflicts/unresolved/detached-wal-test',
                reason: 'path unavailable',
                createdAt: new Date(0).toISOString(),
                visibleFile: '.overleaf/conflicts/unresolved/detached-wal-test/local.txt',
                acknowledgedFile: '.overleaf/conflicts/unresolved/detached-wal-test/base.txt',
                visibleHash: '1'.repeat(64),
                acknowledgedHash: '2'.repeat(64),
                revision: 3,
                pendingOperationIds: ['legacy-op'],
                epoch: 0,
                snapshotHash: '3'.repeat(64),
                walRetired: true,
            },
        };
        await store.save(state);

        const restored = await store.load();
        assert.strictEqual(restored.replicaId, state.replicaId);
        assert.deepStrictEqual(restored.entries, state.entries);
        assert.deepStrictEqual(restored.pendingAction, state.pendingAction);
        assert.deepStrictEqual(
            restored.detachedDocumentRecoveries,
            state.detachedDocumentRecoveries,
        );
        await store.close();
    });

    it('reopens a cached reader to observe another store append', async () => {
        const writer = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const reader = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await writer.load();
        await reader.load();

        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'acknowledged',
            text: 'acknowledged text',
        };
        await writer.save(state);

        const refreshed = await reader.reload();
        assert.strictEqual(refreshed.entries['main.tex'].text, 'acknowledged text');
        await Promise.all([writer.close(), reader.close()]);
    });

    it('stores high-frequency projection receipts outside the full-state WAL', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1', fileType: 'doc', hash: 'base', text: 'base',
        };
        await store.save(state);
        const walPath = store.statePath.replace(/\.json$/, '.wal');
        const walBefore = await readFile(walPath);

        for (let index = 0; index < 20; index++) {
            state.projectionReceipts = [{
                id: 'editor-main',
                path: 'editor:main.tex',
                kind: 'change',
                expectedHash: `projection-${index}`,
            }];
            await store.save(state);
        }

        assert((await readFile(walPath)).equals(walBefore));
        assert.strictEqual(
            JSON.parse(await readFile(store.statePath, 'utf8')).projectionReceipts,
            undefined,
        );
        const recovered = await store.reload();
        assert.strictEqual(recovered.projectionReceipts[0].expectedHash, 'projection-19');
        await store.close();
    });

    it('fails closed on a corrupted projection receipt snapshot', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.initialized = true;
        state.projectionReceipts = [{
            id: 'disk-main', path: 'disk:main.tex', kind: 'change', expectedHash: 'hash',
        }];
        await store.save(state);
        await writeFile(store.projectionStatePath, '{"schemaVersion":1,"receipts":[],"hash":"bad"}');

        const reader = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        await assert.rejects(() => reader.reload(), /checksum is invalid/);
        await Promise.all([store.close(), reader.close()]);
    });

    it('reads a published snapshot without repairing a live WAL tail', async () => {
        const writer = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await writer.load();
        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1', fileType: 'doc', hash: 'published', text: 'published',
        };
        await writer.save(state);
        await writer.close();
        const walPath = writer.statePath.replace(/\.json$/, '.wal');
        await fsPromises.appendFile(walPath, '{"schemaVersion":1');
        const before = await readFile(walPath);

        const reader = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const published = await reader.reload();

        assert.strictEqual(published.entries['main.tex'].text, 'published');
        assert((await readFile(walPath)).equals(before), 'read-only reload truncated the WAL');
        await reader.close();
    });

    it('retires a broken full-snapshot chain only when its valid tail was published', async () => {
        const writer = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await writer.load();
        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1', fileType: 'doc', hash: 'one', text: 'one',
        };
        await writer.save(state);
        state.entries['main.tex'] = {...state.entries['main.tex'], hash: 'two', text: 'two'};
        await writer.save(state);
        state.entries['main.tex'] = {...state.entries['main.tex'], hash: 'three', text: 'three'};
        await writer.save(state);
        await writer.close();
        const walPath = writer.statePath.replace(/\.json$/, '.wal');
        const records = (await readFile(walPath, 'utf8')).trimEnd().split('\n');
        await writeFile(walPath, `${records[0]}\n${records[2]}\n`);

        const recoveredStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const recovered = await recoveredStore.load();

        assert.strictEqual(recovered.entries['main.tex'].text, 'three');
        assert.strictEqual(await readFile(walPath, 'utf8'), '');
        await recoveredStore.close();
    });

    it('fails closed when a broken WAL tail does not match the published snapshot', async () => {
        const writer = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await writer.load();
        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1', fileType: 'doc', hash: 'one', text: 'one',
        };
        await writer.save(state);
        state.entries['main.tex'] = {...state.entries['main.tex'], hash: 'two', text: 'two'};
        await writer.save(state);
        state.entries['main.tex'] = {...state.entries['main.tex'], hash: 'three', text: 'three'};
        await writer.save(state);
        await writer.close();
        const walPath = writer.statePath.replace(/\.json$/, '.wal');
        const records = (await readFile(walPath, 'utf8')).trimEnd().split('\n');
        await writeFile(walPath, `${records[0]}\n${records[2]}\n`);
        await writeFile(writer.statePath, JSON.stringify(JSON.parse(records[0]).payload));

        const recoveredStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        await assert.rejects(() => recoveredStore.load(), /Invalid WAL chain at sequence 2/);
        assert.notStrictEqual(await readFile(walPath, 'utf8'), '');
        await recoveredStore.close();
    });

    it('recovers a published snapshot when a crash cuts WAL reset before replacement', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'before-replacement',
            text: 'durable snapshot',
        };
        for (let index = 0; index < MAX_REPLICA_STATE_WAL_RECORDS - 1; index++) {
            state.entries['main.tex'].hash = `seed-before-${index}`;
            await store.save(state);
        }
        state.entries['main.tex'].hash = 'before-replacement';
        const replaceWithEmpty = DurableWal.replaceWithEmpty;
        try {
            DurableWal.replaceWithEmpty = async () => {
                throw new Error('crash-before-empty-wal-rename');
            };
            await assert.rejects(
                () => store.save(state),
                /crash-before-empty-wal-rename/,
            );
        } finally {
            DurableWal.replaceWithEmpty = replaceWithEmpty;
        }

        const recoveredStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const recovered = await recoveredStore.load();
        assert.strictEqual(recovered.entries['main.tex'].text, 'durable snapshot');
        await Promise.all([store.close(), recoveredStore.close()]);
    });

    it('recovers a published snapshot when a crash cuts WAL reset after replacement', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'after-replacement',
            text: 'checkpoint fallback',
        };
        for (let index = 0; index < MAX_REPLICA_STATE_WAL_RECORDS - 1; index++) {
            state.entries['main.tex'].hash = `seed-after-${index}`;
            await store.save(state);
        }
        state.entries['main.tex'].hash = 'after-replacement';
        const replaceWithEmpty = DurableWal.replaceWithEmpty;
        try {
            DurableWal.replaceWithEmpty = async walPath => {
                await replaceWithEmpty.call(DurableWal, walPath);
                throw new Error('crash-after-empty-wal-rename');
            };
            await assert.rejects(
                () => store.save(state),
                /crash-after-empty-wal-rename/,
            );
        } finally {
            DurableWal.replaceWithEmpty = replaceWithEmpty;
        }

        const recoveredStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const recovered = await recoveredStore.load();
        assert.strictEqual(recovered.entries['main.tex'].text, 'checkpoint fallback');
        await Promise.all([store.close(), recoveredStore.close()]);
    });

    it('keeps the full-snapshot WAL bounded across thousands of durable saves', async function () {
        this.timeout(20000);
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.initialized = true;
        state.entries['main.tex'] = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'snapshot-0',
            text: 'snapshot-0',
        };
        for (let index = 0; index < 1000; index++) {
            state.entries['main.tex'].hash = `snapshot-${index}`;
            state.entries['main.tex'].text = `snapshot-${index}`;
            await store.save(state);
        }

        const walPath = store.statePath.replace(/\.json$/, '.wal');
        const wal = await readFile(walPath, 'utf8');
        const recordCount = wal.length === 0 ? 0 : wal.trimEnd().split('\n').length;
        assert(recordCount < MAX_REPLICA_STATE_WAL_RECORDS);
        assert(Buffer.byteLength(wal) < MAX_REPLICA_STATE_WAL_BYTES);
        const recovered = await store.reload();
        assert.strictEqual(recovered.entries['main.tex'].text, 'snapshot-999');
        const reopenedWal = await readFile(walPath, 'utf8');
        assert.strictEqual(
            reopenedWal.length === 0 ? 0 : reopenedWal.trimEnd().split('\n').length,
            recordCount,
        );
        await store.close();
    });

    it('compacts a large full-snapshot WAL at its byte bound', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.initialized = true;
        state.conflicts['large.tex'] = {
            path: 'large.tex',
            artifactDirectory: '/conflicts/large',
            reason: 'x'.repeat(512 * 1024),
            createdAt: new Date(0).toISOString(),
        };
        for (let index = 0; index < 12; index++) {
            state.conflicts['large.tex'].createdAt = new Date(index).toISOString();
            await store.save(state);
        }

        const walPath = store.statePath.replace(/\.json$/, '.wal');
        const wal = await readFile(walPath, 'utf8');
        const records = wal.length === 0 ? 0 : wal.trimEnd().split('\n').length;
        assert(records < 12, 'byte bound did not compact before the record bound');
        assert(Buffer.byteLength(wal) < MAX_REPLICA_STATE_WAL_BYTES);
        const recovered = await store.reload();
        assert.strictEqual(
            recovered.conflicts['large.tex'].createdAt,
            new Date(11).toISOString(),
        );
        await store.close();
    });

    it('bounds durable AI commit receipts while retaining the newest retries', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        for (let index = 0; index < MAX_COMPLETED_AI_COMMITS + 2; index++) {
            const leaseId = `lease-${String(index).padStart(3, '0')}`;
            recordCompletedAiCommit(state, {
                leaseId,
                fencingToken: index + 1,
                path: 'main.tex',
                entityId: 'doc-1',
                targetHash: createHash('sha256').update(`target-${index}`).digest('hex'),
                receiptContentHash: createHash('sha256')
                    .update(`accepted-${index}`).digest('hex'),
                receipt: {
                    opId: `op-${index}`,
                    path: 'main.tex',
                    journaled: true,
                    remoteAcknowledged: false,
                },
                completedAt: new Date(index).toISOString(),
            });
        }
        await store.save(state);

        const recovered = await store.reload();
        assert.strictEqual(
            Object.keys(recovered.aiCompletedCommits).length,
            MAX_COMPLETED_AI_COMMITS,
        );
        assert.strictEqual(recovered.aiCompletedCommits['lease-000'], undefined);
        assert.strictEqual(recovered.aiCompletedCommits['lease-001'], undefined);
        assert.strictEqual(
            recovered.aiCompletedCommits[`lease-${String(MAX_COMPLETED_AI_COMMITS + 1).padStart(3, '0')}`]
                .receipt.opId,
            `op-${MAX_COMPLETED_AI_COMMITS + 1}`,
        );
        recordCompletedAiCommit(recovered, {
            leaseId: 'lease-after-clock-rollback',
            fencingToken: MAX_COMPLETED_AI_COMMITS + 3,
            path: 'main.tex',
            entityId: 'doc-1',
            targetHash: createHash('sha256').update('clock-rollback-target').digest('hex'),
            receiptContentHash: createHash('sha256')
                .update('clock-rollback-content').digest('hex'),
            receipt: {
                opId: 'clock-rollback-op',
                path: 'main.tex',
                journaled: true,
                remoteAcknowledged: false,
            },
            completedAt: new Date(0).toISOString(),
        });
        assert.strictEqual(
            recovered.aiCompletedCommits['lease-after-clock-rollback'].receipt.opId,
            'clock-rollback-op',
        );
        assert.strictEqual(
            Object.keys(recovered.aiCompletedCommits).length,
            MAX_COMPLETED_AI_COMMITS,
        );
        await store.save(recovered);
        const afterClockRollback = await store.reload();
        assert.strictEqual(
            afterClockRollback.aiCompletedCommits['lease-after-clock-rollback'].receipt.opId,
            'clock-rollback-op',
        );
        await store.close();
    });

    it('migrates legacy inline AI receipts to content-addressed baselines', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        state.aiCompletedCommits = {
            'legacy-lease': {
                leaseId: 'legacy-lease',
                fencingToken: 3,
                path: 'main.tex',
                entityId: 'doc-1',
                targetContent: 'legacy target',
                receipt: {
                    opId: 'legacy-op',
                    path: 'main.tex',
                    content: 'legacy accepted content',
                    journaled: true,
                    remoteAcknowledged: false,
                },
                completedAt: new Date(0).toISOString(),
            },
        };
        await store.save(state);
        const loaded = await store.reload();

        assert.strictEqual(await store.normalizeCompletedAiCommits(loaded), 1);

        const migrated = loaded.aiCompletedCommits['legacy-lease'];
        assert.strictEqual(migrated.targetContent, undefined);
        assert.strictEqual(migrated.receipt.content, undefined);
        assert.strictEqual(
            Buffer.from(await store.readBaseline(migrated.targetHash)).toString(),
            'legacy target',
        );
        assert.strictEqual(
            Buffer.from(await store.readBaseline(migrated.receiptContentHash)).toString(),
            'legacy accepted content',
        );
        const recovered = await store.reload();
        assert.strictEqual(recovered.aiCompletedCommits['legacy-lease'].targetHash, migrated.targetHash);
        await store.close();
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

    it('publishes owner metadata atomically before another acquisition can inspect the lock', async () => {
        const key = '/atomic-owner-publication';
        const digest = createHash('sha256').update(key).digest('hex');
        const lockDirectory = path.join(root, 'safe-sync', 'locks', `${digest}.lock`);
        const originalRename = fsPromises.rename;
        let releaseFirstRename;
        let announceFirstRename;
        const firstRenameStarted = new Promise(resolve => { announceFirstRename = resolve; });
        const firstRenameGate = new Promise(resolve => { releaseFirstRename = resolve; });
        let blockFirstPublication = true;
        let second;
        try {
            fsPromises.rename = async (source, destination) => {
                if (
                    blockFirstPublication
                    && path.resolve(String(destination)) === lockDirectory
                    && String(source).endsWith('.acquire')
                ) {
                    blockFirstPublication = false;
                    announceFirstRename();
                    await firstRenameGate;
                }
                return originalRename(source, destination);
            };
            const firstOutcome = ReplicaLock.acquire(root, key, {
                heartbeatMs: 100000,
            }).then(value => ({value}), error => ({error}));
            await firstRenameStarted;
            second = await ReplicaLock.acquire(root, key, {heartbeatMs: 100000});
            releaseFirstRename();
            const first = await firstOutcome;

            assert(first.error instanceof ReplicaLockedError);
            assert.strictEqual(
                JSON.parse(await readFile(path.join(lockDirectory, 'owner.json'), 'utf8')).instanceId,
                second.instanceId,
            );
        } finally {
            releaseFirstRename?.();
            fsPromises.rename = originalRename;
            await second?.release().catch(() => {});
        }
    });

    it('waits for an in-flight heartbeat before releasing ownership', async () => {
        const key = '/heartbeat-release-race';
        const digest = createHash('sha256').update(key).digest('hex');
        const ownerPath = path.join(root, 'safe-sync', 'locks', `${digest}.lock`, 'owner.json');
        const originalReadFile = fsPromises.readFile;
        let unblockHeartbeatRead;
        const heartbeatReadBlocked = new Promise(resolve => {
            unblockHeartbeatRead = resolve;
        });
        let heartbeatReadStarted;
        const heartbeatEnteredRead = new Promise(resolve => {
            heartbeatReadStarted = resolve;
        });
        let blockNextOwnerRead = true;
        let first;
        let second;

        try {
            first = await ReplicaLock.acquire(root, key, { heartbeatMs: 100000 });
            fsPromises.readFile = async (...args) => {
                const contents = await originalReadFile(...args);
                if (blockNextOwnerRead && path.resolve(String(args[0])) === ownerPath) {
                    blockNextOwnerRead = false;
                    heartbeatReadStarted();
                    await heartbeatReadBlocked;
                }
                return contents;
            };

            const heartbeat = first.heartbeat();
            await heartbeatEnteredRead;

            let releaseSettled = false;
            const release = first.release().finally(() => {
                releaseSettled = true;
            });
            await new Promise(resolve => setTimeout(resolve, 5));
            assert.strictEqual(releaseSettled, false, 'release returned before the heartbeat completed');
            await assert.rejects(
                () => ReplicaLock.acquire(root, key, {
                    ttlMs: 1,
                    heartbeatMs: 100000,
                }),
                ReplicaLockedError,
            );

            unblockHeartbeatRead();
            await Promise.all([heartbeat, release]);
            second = await ReplicaLock.acquire(root, key, { heartbeatMs: 100000 });
            assert.strictEqual(
                JSON.parse(await readFile(ownerPath, 'utf8')).instanceId,
                second.instanceId,
            );
        } finally {
            unblockHeartbeatRead?.();
            fsPromises.readFile = originalReadFile;
            await first?.release().catch(() => {});
            await second?.release().catch(() => {});
        }
    });

    it('does not let a second takeover pass a live recovery owner', async () => {
        const key = '/recovery-owner-race';
        const digest = createHash('sha256').update(key).digest('hex');
        const lockDirectory = path.join(root, 'safe-sync', 'locks', `${digest}.lock`);
        const ownerPath = path.join(lockDirectory, 'owner.json');
        await mkdir(lockDirectory, {recursive: true});
        await writeFile(ownerPath, JSON.stringify({
            instanceId: 'dead-owner', pid: 999999, heartbeat: 0,
        }));
        const originalReadFile = fsPromises.readFile;
        let unblockOwnerRead;
        const ownerReadBlocked = new Promise(resolve => { unblockOwnerRead = resolve; });
        let ownerReadStarted;
        const ownerEnteredRead = new Promise(resolve => { ownerReadStarted = resolve; });
        let blockNextOwnerRead = true;
        let first;
        try {
            fsPromises.readFile = async (...args) => {
                const contents = await originalReadFile(...args);
                if (blockNextOwnerRead && path.resolve(String(args[0])) === ownerPath) {
                    blockNextOwnerRead = false;
                    ownerReadStarted();
                    await ownerReadBlocked;
                }
                return contents;
            };
            const firstAcquire = ReplicaLock.acquire(root, key, {
                ttlMs: 1,
                heartbeatMs: 100000,
            });
            await ownerEnteredRead;

            await assert.rejects(
                () => ReplicaLock.acquire(root, key, {
                    ttlMs: 1,
                    heartbeatMs: 100000,
                }),
                ReplicaLockedError,
            );
            unblockOwnerRead();
            first = await firstAcquire;
        } finally {
            unblockOwnerRead?.();
            fsPromises.readFile = originalReadFile;
            await first?.release().catch(() => {});
        }
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
        await replacement.release();
    });

    it('fails closed instead of replacing a corrupt durable baseline', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        await mkdir(path.dirname(store.statePath), { recursive: true });
        await writeFile(store.statePath, '{not-json');
        await assert.rejects(() => store.load(), SyntaxError);
        await store.close();
    });

    it('keeps content-addressed common bases outside the replica tree', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const content = Buffer.from('common binary base');
        const hash = await store.saveBaseline(content);
        assert.strictEqual(hash, createHash('sha256').update(content).digest('hex'));
        assert.deepStrictEqual(
            Buffer.from(await store.readBaseline(hash)),
            content,
        );
        assert.strictEqual(await store.readBaseline('0'.repeat(64)), undefined);
        await store.close();
    });

    it('retains only the latest binary baseline across 200 durable updates', async function () {
        this.timeout(20000);
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        let latestHash;
        let latestContent;
        for (let index = 0; index < 200; index++) {
            latestContent = Buffer.from(`binary revision ${index}`);
            latestHash = await store.saveBaseline(latestContent);
            state.entries['figure.bin'] = {
                entityId: 'file-1',
                fileType: 'file',
                hash: latestHash,
            };
            await store.save(state);
        }

        const blobs = (await readdir(baselineDirectory(root), {withFileTypes: true}))
            .filter(entry => entry.isFile() && /^[a-f0-9]{64}$/.test(entry.name))
            .map(entry => entry.name);
        assert.deepStrictEqual(blobs, [latestHash]);
        const recovered = await store.reload();
        assert.strictEqual(recovered.entries['figure.bin'].hash, latestHash);
        assert(Buffer.from(await store.readBaseline(latestHash)).equals(latestContent));
        await store.close();
    });

    it('keeps duplicate-hash reservations until each writer reaches a durable reference', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        const content = Buffer.from('shared concurrent binary');
        const [firstHash, secondHash] = await Promise.all([
            store.saveBaseline(content),
            store.saveBaseline(content),
        ]);
        assert.strictEqual(firstHash, secondHash);

        state.entries['first.bin'] = {
            entityId: 'file-1', fileType: 'file', hash: firstHash,
        };
        await store.save(state);
        delete state.entries['first.bin'];
        state.conflicts.unrelated = {
            path: 'unrelated',
            artifactDirectory: '/conflicts/unrelated',
            reason: 'unrelated durable state transition',
            createdAt: new Date(0).toISOString(),
        };
        await store.save(state);
        assert(await store.readBaseline(secondHash), 'second reservation was released by first commit');

        state.entries['second.bin'] = {
            entityId: 'file-2', fileType: 'file', hash: secondHash,
        };
        await store.save(state);
        await store.saveBaseline(content);
        await store.save(state); // stable payload must consume the new reservation
        delete state.entries['second.bin'];
        state.conflicts.unrelated.createdAt = new Date(1).toISOString();
        await store.save(state);
        assert.strictEqual(await store.readBaseline(secondHash), undefined);
        await store.close();
    });

    it('keeps pending and AI content while reclaiming an obsolete binary at runtime', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        const contents = {
            oldBinary: Buffer.from('old binary'),
            nextBinary: Buffer.from('next binary'),
            ancestor: Buffer.from('pending ancestor'),
            requested: Buffer.from('pending target'),
            aiBase: Buffer.from('AI base'),
            aiTarget: Buffer.from('AI target'),
            completedTarget: Buffer.from('completed AI target'),
            completedReceipt: Buffer.from('completed AI receipt'),
        };
        const hashes = {};
        for (const [name, content] of Object.entries(contents)) {
            hashes[name] = await store.saveBaseline(content);
        }
        state.entries['figure.bin'] = {
            entityId: 'file-1', fileType: 'file', hash: hashes.oldBinary,
        };
        state.pendingAction = {
            id: 'pending-document-operation',
            kind: 'push',
            path: 'main.tex',
            startedAt: new Date(0).toISOString(),
            entityId: 'doc-1',
            documentOperationId: 'operation-1',
            documentOperationPhase: 'prepared',
            documentAncestorHash: hashes.ancestor,
            documentRequestedTargetHash: hashes.requested,
        };
        state.aiCommitIntents = {
            'lease-1': {
                leaseId: 'lease-1',
                fencingToken: 1,
                path: 'main.tex',
                entityId: 'doc-1',
                baseHash: hashes.aiBase,
                targetHash: hashes.aiTarget,
                operationId: 'operation-2',
                createdAt: new Date(0).toISOString(),
            },
        };
        state.aiCompletedCommits = {
            'lease-2': {
                leaseId: 'lease-2',
                fencingToken: 2,
                path: 'other.tex',
                entityId: 'doc-2',
                targetHash: hashes.completedTarget,
                receiptContentHash: hashes.completedReceipt,
                receipt: {
                    opId: 'operation-3',
                    path: 'other.tex',
                    journaled: true,
                    remoteAcknowledged: false,
                },
                completedAt: new Date(0).toISOString(),
            },
        };
        await store.save(state);

        state.entries['figure.bin'].hash = hashes.nextBinary;
        await store.save(state);

        assert.strictEqual(await store.readBaseline(hashes.oldBinary), undefined);
        for (const name of [
            'nextBinary',
            'ancestor',
            'requested',
            'aiBase',
            'aiTarget',
            'completedTarget',
            'completedReceipt',
        ]) {
            assert(await store.readBaseline(hashes[name]), `runtime cleanup removed ${name}`);
        }
        await store.close();
    });

    it('releases a baseline reservation when its write fails', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const content = Buffer.from('failed baseline');
        const hash = createHash('sha256').update(content).digest('hex');
        const runBaselineMutation = store.runBaselineMutation;
        try {
            store.runBaselineMutation = async () => {
                throw new Error('injected baseline write failure');
            };
            await assert.rejects(
                () => store.saveBaseline(content),
                /injected baseline write failure/,
            );
            assert.strictEqual(store.uncommittedBaselineReservations.has(hash), false);
        } finally {
            store.runBaselineMutation = runBaselineMutation;
        }
        await store.close();
    });

    it('keeps only live binary baselines and removes inline document blobs at startup', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        const documentHash = await store.saveBaseline(Buffer.from('live document base'));
        const fileHash = await store.saveBaseline(Buffer.from('live binary base'));
        const orphanHash = await store.saveBaseline(Buffer.from('orphan base'));
        const folderOnlyHash = await store.saveBaseline(Buffer.from('not a folder baseline'));
        state.entries = {
            'main.tex': {
                entityId: 'doc-1', fileType: 'doc', hash: documentHash,
                text: 'live document base',
            },
            'figure.png': {entityId: 'file-1', fileType: 'file', hash: fileHash},
            figures: {
                entityId: 'folder-1', fileType: 'folder', hash: folderOnlyHash,
                treeHash: folderOnlyHash,
            },
        };

        const removed = await store.garbageCollectBaselinesAtStartup(state);

        assert.strictEqual(removed, 3);
        assert.strictEqual(await store.readBaseline(documentHash), undefined);
        assert(Buffer.from(await store.readBaseline(fileHash)).equals(
            Buffer.from('live binary base')));
        assert.strictEqual(await store.readBaseline(orphanHash), undefined);
        assert.strictEqual(await store.readBaseline(folderOnlyHash), undefined);
        await store.close();
    });

    it('keeps pending document and AI receipt baselines during startup collection', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        const ancestorHash = await store.saveBaseline(Buffer.from('pending ancestor'));
        const requestedHash = await store.saveBaseline(Buffer.from('pending target'));
        const aiBaseHash = await store.saveBaseline(Buffer.from('AI base'));
        const aiTargetHash = await store.saveBaseline(Buffer.from('AI target'));
        const completedTargetHash = await store.saveBaseline(Buffer.from('completed AI target'));
        const completedReceiptHash = await store.saveBaseline(Buffer.from('completed AI receipt'));
        const orphanHash = await store.saveBaseline(Buffer.from('unreferenced'));
        state.pendingAction = {
            id: 'pending-document-operation',
            kind: 'push',
            path: 'main.tex',
            startedAt: new Date(0).toISOString(),
            entityId: 'doc-1',
            documentOperationId: 'operation-1',
            documentOperationPhase: 'prepared',
            documentAncestorHash: ancestorHash,
            documentRequestedTargetHash: requestedHash,
        };
        state.aiCommitIntents = {
            'lease-1': {
                leaseId: 'lease-1',
                fencingToken: 1,
                path: 'main.tex',
                entityId: 'doc-1',
                baseHash: aiBaseHash,
                targetHash: aiTargetHash,
                operationId: 'operation-2',
                createdAt: new Date(0).toISOString(),
            },
        };
        state.aiCompletedCommits = {
            'lease-2': {
                leaseId: 'lease-2',
                fencingToken: 2,
                path: 'other.tex',
                entityId: 'doc-2',
                targetHash: completedTargetHash,
                receiptContentHash: completedReceiptHash,
                receipt: {
                    opId: 'operation-3',
                    path: 'other.tex',
                    journaled: true,
                    remoteAcknowledged: false,
                },
                completedAt: new Date(0).toISOString(),
            },
        };

        const removed = await store.garbageCollectBaselinesAtStartup(state);

        assert.strictEqual(removed, 1);
        for (const hash of [
            ancestorHash,
            requestedHash,
            aiBaseHash,
            aiTargetHash,
            completedTargetHash,
            completedReceiptHash,
        ]) {
            assert(await store.readBaseline(hash), `startup collection removed live hash ${hash}`);
        }
        assert.strictEqual(await store.readBaseline(orphanHash), undefined);
        await store.close();
    });

    it('removes strict interrupted writes but preserves unknown and non-file entries', async () => {
        const store = new ReplicaStateStore(root, 'server', 'project', '/workspace');
        const state = await store.load();
        const orphanHash = await store.saveBaseline(Buffer.from('orphan valid blob'));
        const directory = baselineDirectory(root);
        const unknownPath = path.join(directory, 'future-index.json');
        const temporaryPath = path.join(
            directory,
            `${orphanHash}.00000000-0000-4000-8000-000000000000.tmp`,
        );
        const unknownTemporaryPath = path.join(
            directory,
            `${orphanHash}.interrupted.tmp`,
        );
        const hashDirectory = path.join(directory, 'f'.repeat(64));
        const hashSymlink = path.join(directory, 'e'.repeat(64));
        await writeFile(unknownPath, 'unknown metadata');
        await writeFile(temporaryPath, 'partial write');
        await writeFile(unknownTemporaryPath, 'unknown partial write');
        await mkdir(hashDirectory);
        await symlink(unknownPath, hashSymlink);

        const removed = await store.garbageCollectBaselinesAtStartup(state);

        assert.strictEqual(removed, 2);
        assert.strictEqual(await store.readBaseline(orphanHash), undefined);
        await assert.rejects(() => stat(temporaryPath), {code: 'ENOENT'});
        assert((await stat(unknownPath)).isFile());
        assert((await stat(unknownTemporaryPath)).isFile());
        assert((await stat(hashDirectory)).isDirectory());
        assert((await lstat(hashSymlink)).isSymbolicLink());
        await store.close();
    });
});
