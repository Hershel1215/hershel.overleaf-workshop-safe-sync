const assert = require('assert');
const {mkdtemp, rm} = require('fs/promises');
const Module = require('module');
const os = require('os');
const path = require('path');
const {decideReconciliation} = require('../out/sync/reconcile');
const {contentHash} = require('../out/sync/syncPolicy');
const {TextReplicaStateMachine} = require('../out/sync/textReplicaStateMachine');
const {DocumentSessionRetiredError} = require('../out/sync/durableDocumentSession');
const {FairTaskScheduler} = require('../out/sync/fairTaskScheduler');
const {SerialTaskQueue} = require('../out/sync/serialTaskQueue');
const {AiWriterLeaseManager} = require('../out/sync/aiWriterLease');
const {ReplicaStateStore} = require('../out/sync/replicaStateStore');

class MockConcurrentRemoteResourceError extends Error {}
class MockRetainedRemoteCandidateError extends MockConcurrentRemoteResourceError {
    constructor(message, uri = {path: '/hershel-e2e-stale'}) {
        super(message);
        this.uri = uri;
    }
}
class MockDocumentRecoveryIsolationError extends Error {
    constructor(documentId, path, reason) {
        super(`Safe synchronization isolated ${path}: ${reason}`);
        this.documentId = documentId;
        this.path = path;
        this.reason = reason;
    }
}

function loadProvider(vscodeOverride) {
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return vscodeOverride ?? {
                l10n: {t: value => value},
                ThemeIcon: class ThemeIcon {},
                window: {
                    createOutputChannel: () => ({
                        appendLine() {},
                        clear() {},
                        dispose() {},
                    }),
                },
            };
        }
        if (request === '../core/remoteFileSystemProvider') {
            return {
                ConcurrentRemoteResourceError: MockConcurrentRemoteResourceError,
                DocumentRecoveryIsolationError: MockDocumentRecoveryIsolationError,
                EMPTY_REMOTE_FOLDER_TREE_HASH: contentHash(Buffer.from('[]')),
                REMOTE_SAFE_SYNC_DIRECTORY: '.hershel-safe-sync',
                RetainedRemoteCandidateError: MockRetainedRemoteCandidateError,
            };
        }
        if (request === '../utils/eventBus') {
            return {EventBus: {fire() {}}};
        }
        if (request === '../utils/globalStateManager') {
            return {GlobalStateManager: {getServerUrl: () => undefined}};
        }
        return originalLoad.apply(this, arguments);
    };
    try {
        delete require.cache[require.resolve('../out/scm/localReplicaSCM')];
        delete require.cache[require.resolve('../out/scm/index')];
        return require('../out/scm/localReplicaSCM').LocalReplicaSCMProvider;
    } finally {
        Module._load = originalLoad;
    }
}

function loadVirtualFileSystem(deleteResponse) {
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return {
                Disposable: class Disposable {},
                l10n: {t: value => value},
                FileChangeType: {Deleted: 3},
                workspace: {
                    getConfiguration: () => ({get: (_key, fallback) => fallback}),
                },
            };
        }
        if (request === '../utils/globalStateManager') {
            return {
                GlobalStateManager: {
                    authenticate: async () => ({csrfToken: 'test', cookies: 'test'}),
                },
            };
        }
        if (request === '../collaboration/clientManager') {
            return {ClientManager: class ClientManager {}};
        }
        if (request === '../utils/eventBus') {
            return {EventBus: {fire() {}}};
        }
        if (request === '../scm/scmCollectionProvider') {
            return {SCMCollectionProvider: class SCMCollectionProvider {}};
        }
        if (request === '../sync/documentSession') {
            return {DocumentSession: class DocumentSession {}};
        }
        if (request === '../sync/documentStateStore') {
            return {DocumentStateStore: class DocumentStateStore {}};
        }
        return originalLoad.apply(this, arguments);
    };
    try {
        delete require.cache[require.resolve('../out/core/remoteFileSystemProvider')];
        const {VirtualFileSystem} = require('../out/core/remoteFileSystemProvider');
        const vfs = Object.create(VirtualFileSystem.prototype);
        const parentFolder = {docs: [], fileRefs: [], folders: []};
        const fileEntity = {_id: 'remote-id', name: 'fixture.tex'};
        parentFolder.docs.push(fileEntity);
        vfs._resolveUri = async () => ({
            parentFolder,
            fileType: 'doc',
            fileEntity,
        });
        vfs.context = {};
        vfs.serverName = 'test-server';
        vfs.projectId = 'test-project';
        vfs.api = {deleteEntity: async () => deleteResponse};
        vfs.notify = () => {};
        return {vfs, parentFolder};
    } finally {
        Module._load = originalLoad;
    }
}

function snapshot(pathname, fileType = 'folder') {
    return {
        path: pathname,
        fileType,
        hash: fileType === 'folder' ? 'folder' : `${pathname}-hash`,
        entityId: `${pathname}-id`,
    };
}

function createProvider(LocalReplicaSCMProvider, basePath, events, failRemote = false) {
    const provider = new LocalReplicaSCMProvider(
        {
            serverName: 'test-server',
            projectId: 'test-project',
            remove: async () => {
                events.push('remote-delete');
                if (failRemote) {
                    throw new Error('remote delete rejected');
                }
            },
            walk: () => [],
            inspectPendingDocumentRecoveries: async () => [],
        },
        {fsPath: basePath, toString: () => `file://${basePath}`},
        {globalStorageUri: {fsPath: '/tmp/hershel-cleanup-state'}},
    );
    provider.disposed = false;
    provider.lockHeld = true;
    provider.state = {
        schemaVersion: 1,
        replicaId: 'replica',
        initialized: true,
        updatedAt: new Date().toISOString(),
        entries: {
            'hershel-e2e-stale': snapshot('hershel-e2e-stale'),
            'hershel-e2e-stale/file.tex': snapshot('hershel-e2e-stale/file.tex', 'doc'),
            'kept.tex': snapshot('kept.tex', 'doc'),
        },
        conflicts: {},
    };
    const before = {
        local: new Map([
            ['hershel-e2e-stale', snapshot('hershel-e2e-stale')],
            ['hershel-e2e-stale/file.tex', snapshot('hershel-e2e-stale/file.tex', 'doc')],
        ]),
        remote: new Map([
            ['hershel-e2e-stale', snapshot('hershel-e2e-stale')],
            ['hershel-e2e-stale/file.tex', snapshot('hershel-e2e-stale/file.tex', 'doc')],
        ]),
    };
    let scans = 0;
    provider.scanBoth = async () => {
        events.push(`scan-${++scans}`);
        return scans === 1 ? before : {local: new Map(), remote: new Map()};
    };
    provider.remoteUri = pathname => ({path: `/${pathname}`});
    provider.deleteLocalSnapshot = async () => events.push('local-delete');
    provider.stateStore.save = async () => events.push('state-save');
    provider.renderReport = () => events.push('report');
    LocalReplicaSCMProvider.activeInstances.set(path.resolve(basePath), provider);
    return provider;
}

describe('Local Replica reserved namespace safety', () => {
    it('fails before observation when the remote-only namespace exists locally', async () => {
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            workspace: {fs: {readDirectory: async () => [['.hershel-safe-sync', 2]]}},
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.localUri = filePath => ({path: filePath});
        provider.isFileNotFound = () => false;

        await assert.rejects(
            provider.assertNoLocalReservedNamespace(),
            /\.hershel-safe-sync is reserved/,
        );
    });

    it('accepts an absent local remote-only namespace', async () => {
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            workspace: {fs: {readDirectory: async () => [['main.tex', 1]]}},
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.localUri = filePath => ({path: filePath});
        await provider.assertNoLocalReservedNamespace();
    });
});

describe('Local Replica reconcile queue depth', () => {
    const LocalReplicaSCMProvider = loadProvider();

    function createReconcilingProvider(passes) {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.reconcileQueue = new SerialTaskQueue();
        provider.activeReconcileCount = 0;
        provider.reconcileStarts = 0;
        provider.reconcileUntilStable = async reason => {
            passes.push(reason);
            await new Promise(resolve => setImmediate(resolve));
        };
        provider.handleReconcileError = async error => { throw error; };
        return provider;
    }

    it('collapses requests that arrive while a pass is still waiting', async () => {
        const passes = [];
        const provider = createReconcilingProvider(passes);
        let releaseRunning;
        const running = provider.reconcileQueue.run(
            () => new Promise(resolve => { releaseRunning = resolve; }),
        );
        await new Promise(resolve => setImmediate(resolve));

        // Everything asking while the queue is blocked joins one pass and is
        // answered by it, so no caller has to know it was merged.
        const joined = [
            provider.enqueueReconcile('remote change: a.tex'),
            provider.enqueueReconcile('remote change: b.tex'),
            provider.enqueueReconcile('remote change: a.tex'),
        ];
        releaseRunning();
        await running;
        await Promise.all(joined);

        assert.deepStrictEqual(passes, ['remote change: a.tex, remote change: b.tex']);
        assert.strictEqual(provider.reconcileStarts, 1);
        assert.strictEqual(provider.queuedReconcile, undefined);
    });

    it('still runs a pass for a request that arrives while one is running', async () => {
        const passes = [];
        const provider = createReconcilingProvider(passes);
        let observedDuringRun;
        provider.reconcileUntilStable = async reason => {
            passes.push(reason);
            if (passes.length === 1) {
                // A change reported now may have landed after this pass
                // scanned, so it is owed a scan of its own.
                observedDuringRun = provider.enqueueReconcile('local change: c.tex');
            }
        };

        await provider.enqueueReconcile('startup');
        await observedDuringRun;

        assert.deepStrictEqual(passes, ['startup', 'local change: c.tex']);
        assert.strictEqual(provider.reconcileStarts, 2);
    });
});

describe('Local Replica exclusive E2E cleanup', () => {
    const LocalReplicaSCMProvider = loadProvider();

    it('runs namespace cleanup after the current reconcile queue task', async () => {
        const events = [];
        const basePath = '/tmp/hershel-cleanup-serialized';
        const provider = createProvider(LocalReplicaSCMProvider, basePath, events);
        let releaseReconcile;
        const reconcileGate = new Promise(resolve => { releaseReconcile = resolve; });
        const reconcile = provider.reconcileQueue.run(async () => {
            events.push('reconcile-start');
            await reconcileGate;
            events.push('reconcile-end');
        });

        const cleanup = LocalReplicaSCMProvider.cleanupNamespace(
            basePath,
            'hershel-e2e-stale',
        );
        await new Promise(resolve => setImmediate(resolve));
        assert.deepStrictEqual(events, ['reconcile-start']);

        releaseReconcile();
        await reconcile;
        const result = await cleanup;
        assert.deepStrictEqual(events, [
            'reconcile-start',
            'reconcile-end',
            'scan-1',
            'remote-delete',
            'local-delete',
            'scan-2',
            'state-save',
            'report',
        ]);
        assert.deepStrictEqual(result, {
            localRemoved: true,
            remoteRemoved: true,
            removedBaselineEntries: 2,
        });
        assert.deepStrictEqual(Object.keys(provider.state.entries), ['kept.tex']);
        LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
    });

    it('preserves local data and baseline when the remote delete fails', async () => {
        const events = [];
        const basePath = '/tmp/hershel-cleanup-remote-failure';
        const provider = createProvider(LocalReplicaSCMProvider, basePath, events, true);
        await assert.rejects(
            LocalReplicaSCMProvider.cleanupNamespace(basePath, 'hershel-e2e-stale'),
            /remote delete rejected/,
        );
        assert.deepStrictEqual(events, ['scan-1', 'remote-delete']);
        assert(provider.state.entries['hershel-e2e-stale/file.tex']);
        LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
    });

    it('finishes E2E cleanup when a changed remote candidate is safely retained', async () => {
        const events = [];
        const basePath = '/tmp/hershel-cleanup-retained-candidate';
        const provider = createProvider(LocalReplicaSCMProvider, basePath, events);
        provider.vfs.remove = async () => {
            events.push('remote-delete');
            throw new MockRetainedRemoteCandidateError(
                'Changed remote candidate retained-folder was retained',
            );
        };

        const result = await LocalReplicaSCMProvider.cleanupNamespace(
            basePath,
            'hershel-e2e-stale',
        );

        assert.deepStrictEqual(events, [
            'scan-1',
            'remote-delete',
            'scan-2',
            'local-delete',
            'scan-3',
            'state-save',
            'report',
        ]);
        assert.deepStrictEqual(result, {
            localRemoved: true,
            remoteRemoved: true,
            removedBaselineEntries: 2,
        });
        assert.deepStrictEqual(Object.keys(provider.state.entries), ['kept.tex']);
        LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
    });

    it('keeps local E2E data when a replacement remote namespace remains visible', async () => {
        const events = [];
        const basePath = '/tmp/hershel-cleanup-retained-replacement';
        const provider = createProvider(LocalReplicaSCMProvider, basePath, events);
        provider.relativeRemotePath = uri => uri.path.replace(/^\//, '');
        const remoteSnapshot = {
            local: new Map([
                ['hershel-e2e-stale', snapshot('hershel-e2e-stale')],
            ]),
            remote: new Map([
                ['hershel-e2e-stale', {
                    ...snapshot('hershel-e2e-stale'),
                    entityId: 'replacement-folder',
                }],
            ]),
        };
        let scans = 0;
        provider.scanBoth = async () => {
            events.push(`scan-${++scans}`);
            return remoteSnapshot;
        };
        provider.vfs.remove = async () => {
            events.push('remote-delete');
            throw new MockRetainedRemoteCandidateError(
                'Changed remote candidate retained-folder was retained',
            );
        };

        await assert.rejects(
            LocalReplicaSCMProvider.cleanupNamespace(basePath, 'hershel-e2e-stale'),
            /Changed remote candidate retained-folder was retained/,
        );

        assert.deepStrictEqual(events, ['scan-1', 'remote-delete', 'scan-2']);
        assert(provider.state.entries['hershel-e2e-stale/file.tex']);
        LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
    });

    it('continues recursive cleanup after one changed descendant was retained', async () => {
        const events = [];
        const basePath = '/tmp/hershel-cleanup-retained-descendant';
        const provider = createProvider(LocalReplicaSCMProvider, basePath, events);
        provider.relativeRemotePath = uri => uri.path.replace(/^\//, '');
        const before = {
            local: new Map([
                ['hershel-e2e-stale', snapshot('hershel-e2e-stale')],
                ['hershel-e2e-stale/move-b', snapshot('hershel-e2e-stale/move-b')],
            ]),
            remote: new Map([
                ['hershel-e2e-stale', snapshot('hershel-e2e-stale')],
                ['hershel-e2e-stale/move-b', snapshot('hershel-e2e-stale/move-b')],
            ]),
        };
        const afterRetain = {
            local: before.local,
            remote: new Map([
                ['hershel-e2e-stale', snapshot('hershel-e2e-stale')],
            ]),
        };
        let scans = 0;
        provider.scanBoth = async () => {
            events.push(`scan-${++scans}`);
            if (scans === 1) return before;
            if (scans === 2) return afterRetain;
            return {local: new Map(), remote: new Map()};
        };
        let removes = 0;
        provider.vfs.remove = async () => {
            events.push('remote-delete');
            if (removes++ === 0) {
                throw new MockRetainedRemoteCandidateError(
                    'Changed remote descendant was retained',
                    {path: '/hershel-e2e-stale/move-b'},
                );
            }
        };

        const result = await LocalReplicaSCMProvider.cleanupNamespace(
            basePath,
            'hershel-e2e-stale',
        );

        assert.deepStrictEqual(events, [
            'scan-1',
            'remote-delete',
            'scan-2',
            'remote-delete',
            'local-delete',
            'scan-3',
            'state-save',
            'report',
        ]);
        assert.strictEqual(result.remoteRemoved, true);
        assert.strictEqual(result.localRemoved, true);
        LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
    });

    it('refuses recursive cleanup while a descendant document WAL is pending', async () => {
        const events = [];
        const basePath = '/tmp/hershel-cleanup-pending-wal';
        const provider = createProvider(LocalReplicaSCMProvider, basePath, events);
        provider.vfs.walk = () => [{
            entity: {_id: 'fixture-doc', _type: 'doc'},
            path: '/hershel-e2e-stale/file.tex',
        }];
        provider.vfs.inspectPendingDocumentRecoveries = async () => [{
            documentId: 'fixture-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 1,
            pendingOperationIds: ['pending-op'],
            lastKnownPath: 'hershel-e2e-stale/file.tex',
            epoch: 0,
            hash: 'snapshot',
        }];

        await assert.rejects(
            LocalReplicaSCMProvider.cleanupNamespace(basePath, 'hershel-e2e-stale'),
            /pending document WAL at hershel-e2e-stale\/file\.tex/,
        );

        assert.deepStrictEqual(events, []);
        assert(provider.state.entries['hershel-e2e-stale/file.tex']);
        LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
    });

    it('does not let an unrelated path conflict block namespace cleanup', async () => {
        const events = [];
        const basePath = '/tmp/hershel-cleanup-unrelated-conflict';
        const provider = createProvider(LocalReplicaSCMProvider, basePath, events);
        provider.state.conflicts['kept.tex'] = {
            path: 'kept.tex',
            reason: 'preserved audit record',
            artifactDirectory: '.overleaf/conflicts/unresolved/kept',
            createdAt: new Date(0).toISOString(),
        };

        const result = await LocalReplicaSCMProvider.cleanupNamespace(
            basePath,
            'hershel-e2e-stale',
        );

        assert.strictEqual(result.localRemoved, true);
        assert(provider.state.conflicts['kept.tex']);
        LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
    });

    it('rejects paths outside the reserved top-level E2E namespace', () => {
        assert.throws(
            () => LocalReplicaSCMProvider.cleanupNamespace('/tmp/unused', '../hershel-e2e-run'),
            /Refusing to clean non-E2E namespace/,
        );
        assert.throws(
            () => LocalReplicaSCMProvider.cleanupNamespace('/tmp/unused', 'sections'),
            /Refusing to clean non-E2E namespace/,
        );
        assert.throws(
            () => LocalReplicaSCMProvider.resolveE2EConflictUsingRemote(
                '/tmp/unused', '../hershel-e2e-run/fixture.png',
            ),
            /Refusing to resolve non-E2E conflict/,
        );
        assert.throws(
            () => LocalReplicaSCMProvider.resolveE2EConflictUsingRemote(
                '/tmp/unused', 'sections/figure.png',
            ),
            /Refusing to resolve non-E2E conflict/,
        );
        assert.throws(
            () => LocalReplicaSCMProvider.detachE2ETextReplica(
                '/tmp/unused', 'sections/main.tex',
            ),
            /Refusing to detach non-E2E document/,
        );
    });

    it('allows remote resolution of an E2E namespace-root conflict', async () => {
        const basePath = '/tmp/hershel-root-conflict';
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const calls = [];
        provider.enqueueConflictResolution = async (filePath, resolution) => {
            calls.push({filePath, resolution});
            return 'resolved-artifact';
        };
        LocalReplicaSCMProvider.activeInstances.set(path.resolve(basePath), provider);
        try {
            const resolved = await LocalReplicaSCMProvider.resolveE2EConflictUsingRemote(
                basePath,
                'hershel-e2e-interrupted-run',
            );
            assert.strictEqual(resolved, 'resolved-artifact');
            assert.deepStrictEqual(calls, [{
                filePath: 'hershel-e2e-interrupted-run',
                resolution: 'remote',
            }]);
        } finally {
            LocalReplicaSCMProvider.activeInstances.delete(path.resolve(basePath));
        }
    });
});

describe('Local Replica AI scheduling', () => {
    it('enters the document queue after attaching the first AI request', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const events = [];
        const scheduler = {
            run: async (source, task) => {
                events.push(`schedule-${source}`);
                return task();
            },
        };
        provider.observationReady = true;
        provider.disposed = false;
        provider.handoffInProgress = false;
        provider.activeAiGatewayRequests = new Set();
        provider.state = {initialized: true};
        provider.stateStore = {save: async () => {}};
        provider.pendingEditorObservations = new Map();
        provider.editorObservationDrains = new Map();
        provider.textSchedulers = new Map();
        provider.isIgnored = () => false;
        provider.ensureTextMachineForGateway = async filePath => {
            events.push(`attach-${filePath}`);
            provider.textSchedulers.set(filePath, scheduler);
        };
        provider.handleAiGatewayRequestInternal = async request => {
            events.push(`handle-${request.method}`);
            return 'accepted';
        };

        const result = await provider.handleAiGatewayRequest({
            method: 'begin',
            path: 'sections/method.tex',
        });

        assert.strictEqual(result, 'accepted');
        assert.deepStrictEqual(events, [
            'attach-sections/method.tex',
            'schedule-ai',
            'handle-begin',
        ]);
    });

    it('does not isolate a journaled AI edit when its transport session retires', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.disposed = false;
        provider.handoffInProgress = false;
        provider.aiTransactions = new Map([['lease-1', {
            entityId: 'doc-1',
            path: 'main.tex',
            lease: {leaseId: 'lease-1', fencingToken: 1},
        }]]);
        provider.aiLeases = {release() {}};
        provider.persistAiLeases = async () => {};
        provider.handleTextReplicaError = () => {
            throw new Error('recoverable retirement was isolated');
        };

        await assert.doesNotReject(() => provider.finishAiTransaction(
            'lease-1',
            new DocumentSessionRetiredError('session retired'),
        ));
        assert.strictEqual(provider.aiTransactions.size, 0);
    });

    it('returns one journal receipt when an AI commit request is retried', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        let acceptCount = 0;
        const acknowledgement = new Promise(() => {});
        const baselines = new Map();
        provider.observationReady = true;
        provider.state = {initialized: true};
        provider.stateStore = {
            save: async () => {},
            saveBaseline: async bytes => {
                const hash = contentHash(bytes);
                baselines.set(hash, Uint8Array.from(bytes));
                return hash;
            },
            readBaseline: async hash => baselines.get(hash),
        };
        provider.isIgnored = () => false;
        provider.ensureTextMachineForGateway = async () => {};
        provider.textReplicas = new Map([['main.tex', {
            isIsolated: false,
            remoteEntityId: 'doc-1',
            onCanonicalChanged: async () => {},
        }]]);
        provider.vfs = {
            readDocumentProjection: async () => ({
                entityId: 'doc-1',
                revision: 4,
                content: 'AX',
            }),
            inspectDocumentOperation: async () => ({
                disposition: 'unknown',
                content: 'A',
            }),
            acceptDocumentSnapshotAgainstAncestor: async (
                _uri, _base, _target, _barrier, _entityId, opId,
            ) => {
                acceptCount += 1;
                return {opId, content: 'AX', acknowledgement};
            },
        };
        provider.isIgnored = () => false;
        provider.remoteUri = filePath => ({path: `/${filePath}`});
        provider.aiLeases = {assertCanCommit() {}};
        provider.aiTransactions = new Map([['lease-1', {
            entityId: 'doc-1',
            path: 'main.tex',
            baseContent: 'A',
            lease: {leaseId: 'lease-1', fencingToken: 7},
        }]]);
        const request = {
            method: 'commit',
            path: 'main.tex',
            leaseId: 'lease-1',
            fencingToken: 7,
            content: 'AX',
        };

        const first = await provider.handleAiGatewayRequestInternal(request);
        const retried = await provider.handleAiGatewayRequestInternal(request);
        assert.deepStrictEqual(retried, first);
        assert.strictEqual(acceptCount, 1);
        await assert.rejects(
            () => provider.handleAiGatewayRequestInternal({...request, content: 'AY'}),
            /does not match its durable transaction/,
        );
        assert.strictEqual(acceptCount, 1);
    });

    it('returns a durable receipt after restart despite later move, delete, or replacement', async () => {
        const root = await mkdtemp(path.join(os.tmpdir(), 'hershel-ai-receipt-'));
        let firstStore;
        let secondStore;
        try {
            const LocalReplicaSCMProvider = loadProvider();
            let acceptCount = 0;
            let acknowledge;
            const acknowledgement = new Promise(resolve => { acknowledge = resolve; });
            const createFixture = (state, stateStore, entityId = 'doc-1') => {
                const provider = Object.create(LocalReplicaSCMProvider.prototype);
                provider.observationReady = true;
                provider.state = state;
                provider.stateStore = stateStore;
                provider.isIgnored = () => false;
                provider.ensureTextMachineForGateway = async () => {};
                provider.textReplicas = new Map([['main.tex', {
                    isIsolated: false,
                    remoteEntityId: entityId,
                    onCanonicalChanged: async () => {},
                }]]);
                provider.vfs = {
                    readDocumentProjection: async (_uri, expectedEntityId) => {
                        assert.strictEqual(expectedEntityId, entityId);
                        return {entityId, revision: 4, content: 'AX'};
                    },
                    inspectDocumentOperation: async () => ({
                        disposition: 'unknown',
                        content: 'A',
                    }),
                    acceptDocumentSnapshotAgainstAncestor: async (
                        _uri, _base, _target, _barrier, _entityId, opId,
                    ) => {
                        acceptCount += 1;
                        return {opId, content: 'AX', acknowledgement};
                    },
                };
                provider.remoteUri = filePath => ({path: `/${filePath}`});
                provider.aiLeases = {assertCanCommit() {}};
                provider.aiTransactions = new Map();
                provider.persistAiLeases = async () => {};
                return provider;
            };
            const request = {
                method: 'commit',
                path: 'main.tex',
                leaseId: 'lease-1',
                fencingToken: 7,
                content: 'AX',
            };

            firstStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
            const initial = await firstStore.load();
            initial.initialized = true;
            const first = createFixture(initial, firstStore);
            first.aiTransactions.set('lease-1', {
                entityId: 'doc-1',
                path: 'main.tex',
                baseContent: 'A',
                lease: {leaseId: 'lease-1', fencingToken: 7},
            });

            const acceptedReceipt = await first.handleAiGatewayRequestInternal(request);
            acknowledge('AX');
            while (first.aiTransactions.size > 0) {
                await new Promise(resolve => setImmediate(resolve));
            }
            await firstStore.close();
            firstStore = undefined;

            secondStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
            const recovered = await secondStore.load();
            const restarted = createFixture(recovered, secondStore);
            const retriedReceipt = await restarted.handleAiGatewayRequestInternal(request);

            assert.deepStrictEqual(retriedReceipt, acceptedReceipt);
            assert.strictEqual(acceptCount, 1, 'restart retry applied the AI edit twice');
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({...request, content: 'AY'}),
                /does not match its durable transaction/,
            );
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({...request, fencingToken: 8}),
                /does not match its durable transaction/,
            );
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({...request, path: 'other.tex'}),
                /does not match its durable transaction/,
            );
            restarted.textReplicas.clear();
            restarted.ensureTextMachineForGateway = async () => {
                throw new Error('historical receipt consulted live document state');
            };
            restarted.vfs.readDocumentProjection = async () => {
                throw new Error('historical receipt consulted live projection');
            };
            for (const laterState of ['moved', 'deleted', 'replaced']) {
                assert.deepStrictEqual(
                    await restarted.handleAiGatewayRequestInternal(request),
                    acceptedReceipt,
                    `lost completed receipt after document was ${laterState}`,
                );
            }
            restarted.isIgnored = () => true;
            assert.deepStrictEqual(
                await restarted.handleAiGatewayRequestInternal(request),
                acceptedReceipt,
            );
            assert.strictEqual(acceptCount, 1);
        } finally {
            await firstStore?.close();
            await secondStore?.close();
            await rm(root, {recursive: true, force: true});
        }
    });

    it('recovers the fixed AI opId after WAL acceptance but before receipt publication', async () => {
        const root = await mkdtemp(path.join(os.tmpdir(), 'hershel-ai-intent-cut-'));
        let firstStore;
        let secondStore;
        try {
            const LocalReplicaSCMProvider = loadProvider();
            const operation = {id: undefined, disposition: 'unknown', content: 'A'};
            const acceptAttempts = [];
            let logicalAcceptCount = 0;
            const acknowledgement = new Promise(() => {});
            const createFixture = (state, stateStore, entityId = 'doc-1') => {
                const provider = Object.create(LocalReplicaSCMProvider.prototype);
                provider.observationReady = true;
                provider.state = state;
                provider.stateStore = stateStore;
                provider.isIgnored = () => false;
                provider.ensureTextMachineForGateway = async () => {};
                provider.textReplicas = new Map([['main.tex', {
                    isIsolated: false,
                    remoteEntityId: entityId,
                    flushInput: async () => {},
                    onCanonicalChanged: async () => {},
                }]]);
                provider.vfs = {
                    readDocumentProjection: async () => ({
                        entityId,
                        revision: 4,
                        content: operation.content,
                    }),
                    inspectDocumentOperation: async (_uri, expectedEntityId, opId) => {
                        assert.strictEqual(expectedEntityId, entityId);
                        if (operation.id === undefined) {
                            return {disposition: 'unknown', content: operation.content};
                        }
                        assert.strictEqual(opId, operation.id);
                        return {
                            disposition: operation.disposition,
                            content: operation.content,
                        };
                    },
                    acceptDocumentSnapshotAgainstAncestor: async (
                        _uri, base, target, barrier, expectedEntityId, opId,
                    ) => {
                        acceptAttempts.push(opId);
                        assert.strictEqual(base, 'A');
                        assert.strictEqual(target, 'AX');
                        assert.strictEqual(barrier, true);
                        assert.strictEqual(expectedEntityId, entityId);
                        if (operation.id === undefined) {
                            operation.id = opId;
                            operation.disposition = 'pending';
                            operation.content = 'AX';
                            logicalAcceptCount += 1;
                        } else {
                            assert.strictEqual(opId, operation.id);
                        }
                        return {opId, content: operation.content, acknowledgement};
                    },
                };
                provider.remoteUri = filePath => ({path: `/${filePath}`});
                provider.aiLeases = {
                    assertCanCommit() {},
                    acquire() { throw new Error('pending intent did not fence begin'); },
                };
                provider.aiTransactions = new Map();
                provider.persistAiLeases = async () => {};
                provider.failProject = () => {};
                return provider;
            };
            const request = {
                method: 'commit',
                path: 'main.tex',
                leaseId: 'lease-1',
                fencingToken: 7,
                content: 'AX',
            };

            firstStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
            const initial = await firstStore.load();
            initial.initialized = true;
            let stateSaveCount = 0;
            const crashStore = {
                saveBaseline: bytes => firstStore.saveBaseline(bytes),
                readBaseline: hash => firstStore.readBaseline(hash),
                save: async state => {
                    stateSaveCount += 1;
                    if (stateSaveCount === 2) {
                        throw new Error('crash before completed receipt save');
                    }
                    await firstStore.save(state);
                },
            };
            const first = createFixture(initial, crashStore);
            first.aiTransactions.set('lease-1', {
                entityId: 'doc-1',
                path: 'main.tex',
                baseContent: 'A',
                lease: {leaseId: 'lease-1', fencingToken: 7},
            });

            await assert.rejects(
                () => first.handleAiGatewayRequestInternal(request),
                /crash before completed receipt save/,
            );
            const fixedOpId = initial.aiCommitIntents['lease-1'].operationId;
            assert.strictEqual(operation.id, fixedOpId);
            assert.strictEqual(logicalAcceptCount, 1);
            assert.deepStrictEqual(acceptAttempts, [fixedOpId]);
            await firstStore.close();
            firstStore = undefined;

            secondStore = new ReplicaStateStore(root, 'server', 'project', '/workspace');
            const recovered = await secondStore.load();
            const restarted = createFixture(recovered, secondStore);
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({...request, content: 'AY'}),
                /does not match its durable intent/,
            );
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({...request, fencingToken: 8}),
                /does not match its durable intent/,
            );
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({...request, path: 'other.tex'}),
                /does not match its durable intent/,
            );
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({...request, entityId: 'doc-2'}),
                /does not match its durable intent/,
            );
            await assert.rejects(
                () => restarted.handleAiGatewayRequestInternal({
                    method: 'begin', path: 'main.tex', holder: 'second-ai',
                }),
                /pending AI commit already owns document/,
            );

            restarted.textReplicas.clear();
            restarted.ensureTextMachineForGateway = async () => {
                throw new Error('pending retry consulted its stale path');
            };
            const receipt = await restarted.handleAiGatewayRequestInternal(request);
            assert.strictEqual(receipt.opId, fixedOpId);
            assert.strictEqual(logicalAcceptCount, 1, 'retry created a second logical OT operation');
            assert.deepStrictEqual(acceptAttempts, [fixedOpId, fixedOpId]);
            assert.strictEqual(recovered.aiCommitIntents, undefined);
            assert.strictEqual(
                recovered.aiCompletedCommits['lease-1'].receipt.opId,
                fixedOpId,
            );
            const afterCompletion = await secondStore.reload();
            assert.strictEqual(afterCompletion.aiCommitIntents, undefined);
            assert.strictEqual(afterCompletion.aiCompletedCommits['lease-1'].receipt.opId, fixedOpId);
        } finally {
            await firstStore?.close();
            await secondStore?.close();
            await rm(root, {recursive: true, force: true});
        }
    });

    it('finishes unknown and settled durable AI intents before startup observation', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        for (const disposition of ['unknown', 'settled']) {
            const baseBytes = Buffer.from('A');
            const targetBytes = Buffer.from('AX');
            const baseHash = contentHash(baseBytes);
            const targetHash = contentHash(targetBytes);
            const baselines = new Map([[baseHash, baseBytes], [targetHash, targetBytes]]);
            const intent = {
                leaseId: `lease-${disposition}`,
                fencingToken: 7,
                path: 'main.tex',
                entityId: 'doc-1',
                baseHash,
                targetHash,
                operationId: `fixed-${disposition}`,
                createdAt: new Date(0).toISOString(),
            };
            const provider = Object.create(LocalReplicaSCMProvider.prototype);
            provider.state = {
                initialized: true,
                aiCommitIntents: {[intent.leaseId]: intent},
            };
            provider.stateStore = {
                readBaseline: async hash => baselines.get(hash),
                saveBaseline: async bytes => {
                    const hash = contentHash(bytes);
                    baselines.set(hash, Uint8Array.from(bytes));
                    return hash;
                },
                save: async () => {},
            };
            provider.remoteUri = filePath => ({path: `/${filePath}`});
            provider.aiTransactions = new Map();
            provider.failProject = error => { throw error; };
            let acceptCount = 0;
            provider.vfs = {
                inspectDocumentOperation: async (_uri, entityId, opId) => {
                    assert.strictEqual(entityId, intent.entityId);
                    assert.strictEqual(opId, intent.operationId);
                    return {disposition, content: disposition === 'settled' ? 'AX' : 'A'};
                },
                acceptDocumentSnapshotAgainstAncestor: async (
                    _uri, base, target, barrier, entityId, opId,
                ) => {
                    acceptCount += 1;
                    assert.deepStrictEqual(
                        {base, target, barrier, entityId, opId},
                        {
                            base: 'A', target: 'AX', barrier: true,
                            entityId: 'doc-1', opId: intent.operationId,
                        },
                    );
                    return {
                        opId,
                        content: 'AX',
                        acknowledgement: Promise.resolve('AX'),
                    };
                },
            };

            await provider.recoverPendingAiCommitIntents();

            assert.strictEqual(acceptCount, disposition === 'unknown' ? 1 : 0);
            assert.strictEqual(provider.state.aiCommitIntents, undefined);
            assert.strictEqual(
                provider.state.aiCompletedCommits[intent.leaseId].receipt.opId,
                intent.operationId,
            );
        }
    });

    it('isolates deleted, replaced, or invalid-history AI intents without project pause', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        for (const scenario of ['deleted', 'replaced', 'history']) {
            const baseBytes = Buffer.from('AI base');
            const targetBytes = Buffer.from('AI target');
            const baseHash = contentHash(baseBytes);
            const targetHash = contentHash(targetBytes);
            const intent = {
                leaseId: `lease-${scenario}`,
                fencingToken: 9,
                path: 'main.tex',
                entityId: 'old-doc',
                baseHash,
                targetHash,
                operationId: `fixed-${scenario}`,
                createdAt: new Date(0).toISOString(),
            };
            const provider = Object.create(LocalReplicaSCMProvider.prototype);
            provider.state = {
                initialized: true,
                entries: {},
                conflicts: {},
                aiCommitIntents: {[intent.leaseId]: intent},
            };
            const saved = [];
            const artifacts = new Map();
            provider.stateStore = {
                readBaseline: async hash => (
                    hash === baseHash ? baseBytes : hash === targetHash ? targetBytes : undefined
                ),
                save: async state => saved.push(JSON.parse(JSON.stringify(state))),
            };
            provider.remoteUri = filePath => ({path: `/${filePath}`});
            provider.writeArtifact = async (filePath, content) => {
                artifacts.set(filePath, Buffer.from(content));
            };
            provider.isFileNotFound = error => error?.code === 'ENOENT';
            provider.textIsolationEvents = 0;
            provider.output = {appendLine() {}};
            provider.aiTransactions = new Map();
            provider.aiLeases = {
                release() {},
                serialize: () => ({nextFencingToken: 10, leases: []}),
            };
            const notFound = Object.assign(new Error('missing'), {code: 'ENOENT'});
            provider.vfs = {
                inspectDocumentOperation: async () => {
                    if (scenario === 'history') {
                        throw new MockDocumentRecoveryIsolationError(
                            'old-doc',
                            'main.tex',
                            'OT history invariant failed',
                        );
                    }
                    throw new MockConcurrentRemoteResourceError('/main.tex');
                },
                readAuthoritativeDocumentByIdWithoutRecovery: async () => (
                    scenario === 'history'
                        ? {
                            content: Buffer.from('moved current remote'),
                            entityId: 'old-doc',
                            fileType: 'doc',
                            revision: 14,
                            path: '/moved/main.tex',
                        }
                        : undefined
                ),
                readAuthoritativeFileWithoutRecovery: async () => {
                    if (scenario === 'deleted') {
                        throw notFound;
                    }
                    return {
                        content: Buffer.from('replacement remote'),
                        entityId: 'replacement-doc',
                        fileType: 'doc',
                        revision: 12,
                    };
                },
            };

            await provider.recoverPendingAiCommitIntents();

            assert.strictEqual(provider.state.aiCommitIntents, undefined);
            const conflict = provider.state.conflicts['main.tex'];
            assert(conflict, `${scenario} intent was not isolated`);
            assert.strictEqual(conflict.localHash, targetHash);
            assert.strictEqual(conflict.baseHash, baseHash);
            assert.strictEqual(
                conflict.remoteEntityId,
                scenario === 'replaced'
                    ? 'replacement-doc'
                    : scenario === 'history' ? 'old-doc' : undefined,
            );
            assert.strictEqual(
                conflict.remoteRevision,
                scenario === 'replaced' ? 12 : scenario === 'history' ? 14 : undefined,
            );
            assert.strictEqual(saved.length, 2);
            assert.strictEqual(saved[0].aiCommitIntents, undefined);
            assert(saved[0].conflicts['main.tex']);
            assert.deepStrictEqual(saved[1].aiLeaseState.leases, []);
            assert([...artifacts.keys()].some(filePath => filePath.includes('/base/main.tex')));
            assert([...artifacts.keys()].some(filePath => (
                filePath.includes('/local-candidate/main.tex')
            )));
            const manifest = JSON.parse(
                [...artifacts.entries()].find(([filePath]) => (
                    filePath.endsWith('/ai-intent/') === false
                    && filePath.includes('/ai-intent/')
                    && filePath.endsWith('/manifest.json')
                ))[1].toString(),
            );
            assert.strictEqual(manifest.operationId, intent.operationId);
            assert.strictEqual(manifest.entityId, intent.entityId);
            assert.match(manifest.reason, /could not prove recovery ancestry/);
        }
    });

    it('retires the live AI writer after a runtime pending retry is isolated', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const basePath = '/tmp/hershel-runtime-ai-isolation';
        const baseBytes = Buffer.from('runtime base');
        const targetBytes = Buffer.from('runtime target');
        const manager = new AiWriterLeaseManager(30000, () => 'runtime-lease');
        const lease = manager.acquire('runtime-doc', 'codex', 4);
        const intent = {
            leaseId: lease.leaseId,
            fencingToken: lease.fencingToken,
            path: 'main.tex',
            entityId: lease.documentId,
            baseHash: contentHash(baseBytes),
            targetHash: contentHash(targetBytes),
            operationId: 'runtime-fixed-op',
            createdAt: new Date(0).toISOString(),
        };
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.observationReady = true;
        provider.disposed = false;
        provider.handoffInProgress = false;
        provider.lockHeld = true;
        provider.startupSettled = true;
        provider.pausedError = undefined;
        provider.activeReconcileCount = 0;
        provider.reconcileQueue = {isIdle: true};
        provider.reconcileTimer = undefined;
        provider.pendingReasons = new Set();
        provider.activeAiGatewayRequests = new Set();
        provider.pendingEditorObservations = new Map();
        provider.editorObservationDrains = new Map();
        provider.textConflictTasks = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.textRetirements = new Map();
        provider.textSchedulers = new Map();
        provider.textReplicas = new Map();
        provider.state = {
            initialized: true,
            entries: {},
            conflicts: {},
            aiCommitIntents: {[intent.leaseId]: intent},
            aiLeaseState: manager.serialize(),
        };
        const saved = [];
        provider.stateStore = {
            readBaseline: async hash => (
                hash === intent.baseHash ? baseBytes : targetBytes
            ),
            save: async state => saved.push(JSON.parse(JSON.stringify(state))),
        };
        provider.aiLeases = manager;
        provider.aiTransactions = new Map([[intent.leaseId, {
            lease,
            path: intent.path,
            baseContent: 'runtime base',
            entityId: intent.entityId,
        }]]);
        provider.isFileNotFound = error => error?.code === 'ENOENT';
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.remoteUri = filePath => ({path: `/${filePath}`});
        provider.textIsolationEvents = 0;
        provider.output = {appendLine() {}};
        provider.vfs = {
            inspectDocumentOperation: async () => {
                throw new MockConcurrentRemoteResourceError('/main.tex');
            },
            readAuthoritativeDocumentByIdWithoutRecovery: async () => undefined,
            readAuthoritativeFileWithoutRecovery: async () => {
                throw Object.assign(new Error('missing'), {code: 'ENOENT'});
            },
            isSafeSyncConnectionReady: true,
            hasPendingDocumentOperations: false,
        };

        await assert.rejects(
            () => provider.handleAiGatewayRequestInternal({
                method: 'commit',
                path: intent.path,
                leaseId: intent.leaseId,
                fencingToken: intent.fencingToken,
                content: 'runtime target',
            }),
            /recovery was isolated/,
        );

        assert.strictEqual(provider.aiTransactions.size, 0);
        assert.deepStrictEqual(manager.serialize().leases, []);
        assert.deepStrictEqual(saved.at(-1).aiLeaseState.leases, []);
        assert(provider.state.conflicts['main.tex']);
        LocalReplicaSCMProvider.activeInstances.set(basePath, provider);
        try {
            assert.strictEqual(
                LocalReplicaSCMProvider.getRuntimeStatus(basePath).settled,
                true,
            );
        } finally {
            LocalReplicaSCMProvider.activeInstances.delete(basePath);
        }

        const leaseWithoutTransaction = manager.acquire('second-doc', 'claude', 5);
        provider.state.aiLeaseState = manager.serialize();
        await provider.retireIsolatedAiWriter({
            ...intent,
            leaseId: leaseWithoutTransaction.leaseId,
            fencingToken: leaseWithoutTransaction.fencingToken,
            entityId: leaseWithoutTransaction.documentId,
            operationId: 'second-fixed-op',
        });
        assert.deepStrictEqual(manager.serialize().leases, []);
        assert.deepStrictEqual(saved.at(-1).aiLeaseState.leases, []);
    });

    it('fails closed when isolated writer cleanup cannot be persisted', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const manager = new AiWriterLeaseManager(30000, () => 'cleanup-failure-lease');
        const lease = manager.acquire('doc-1', 'codex', 1);
        const intent = {
            leaseId: lease.leaseId,
            fencingToken: lease.fencingToken,
            path: 'main.tex',
            entityId: lease.documentId,
            baseHash: contentHash(Buffer.from('base')),
            targetHash: contentHash(Buffer.from('target')),
            operationId: 'cleanup-failure-op',
            createdAt: new Date(0).toISOString(),
        };
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            initialized: true,
            entries: {},
            conflicts: {'main.tex': {path: 'main.tex'}},
            aiLeaseState: manager.serialize(),
        };
        provider.aiLeases = manager;
        provider.aiTransactions = new Map([[intent.leaseId, {
            lease,
            path: intent.path,
            baseContent: 'base',
            entityId: intent.entityId,
        }]]);
        provider.stateStore = {save: async () => { throw new Error('lease state disk full'); }};
        provider.failProject = error => { provider.pausedError = error; };

        await assert.rejects(
            () => provider.retireIsolatedAiWriter(intent),
            /lease state disk full/,
        );

        assert.match(provider.pausedError.message, /lease state disk full/);
        assert.strictEqual(provider.aiTransactions.size, 1);
    });

    it('keeps the durable AI intent when its conflict fence cannot commit', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const baseBytes = Buffer.from('base');
        const targetBytes = Buffer.from('target');
        const intent = {
            leaseId: 'lease-save-failure',
            fencingToken: 10,
            path: 'main.tex',
            entityId: 'missing-doc',
            baseHash: contentHash(baseBytes),
            targetHash: contentHash(targetBytes),
            operationId: 'fixed-save-failure',
            createdAt: new Date(0).toISOString(),
        };
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            initialized: true,
            entries: {},
            conflicts: {},
            aiCommitIntents: {[intent.leaseId]: intent},
        };
        provider.stateStore = {
            readBaseline: async hash => (
                hash === intent.baseHash ? baseBytes : targetBytes
            ),
            save: async () => { throw new Error('state disk full'); },
        };
        provider.remoteUri = filePath => ({path: `/${filePath}`});
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.isFileNotFound = error => error?.code === 'ENOENT';
        const notFound = Object.assign(new Error('missing'), {code: 'ENOENT'});
        provider.vfs = {
            readAuthoritativeDocumentByIdWithoutRecovery: async () => undefined,
            readAuthoritativeFileWithoutRecovery: async () => { throw notFound; },
        };

        await assert.rejects(
            () => provider.isolateAiCommitIntent(intent, new MockConcurrentRemoteResourceError()),
            /state disk full/,
        );

        assert.strictEqual(provider.state.aiCommitIntents[intent.leaseId], intent);
        assert.deepStrictEqual(provider.state.conflicts, {});
    });
});

describe('Local Replica extension-host ownership handoff', () => {
    it('durably flushes but retains ownership until the old window really closes', async () => {
        const commands = [];
        let saveCount = 0;
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            window: {
                createOutputChannel: () => ({appendLine() {}, clear() {}, dispose() {}}),
            },
            workspace: {
                saveAll: async () => { saveCount++; return true; },
                textDocuments: [],
            },
            commands: {
                executeCommand: async command => { commands.push(command); },
            },
        });
        const events = [];
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.baseUri = {fsPath: '/tmp/hershel-handoff-fixture'};
        provider.observationReady = true;
        provider.disposed = false;
        provider.pausedError = undefined;
        provider.handoffDeferredEditorObservations = new Map();
        provider.activeAiGatewayRequests = new Set();
        provider.textConflictTasks = new Map();
        provider.textRetirements = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.pendingReasons = new Set();
        provider.lifecycleDisposables = [];
        provider.textDocuments = [];
        provider.relativeLocalPath = () => undefined;
        provider.isIgnored = () => false;
        provider.output = {
            appendLine: message => events.push(message),
            dispose: () => { events.push('output-dispose'); },
        };
        provider.vfs = {retireForReplicaHandoff: async () => { events.push('vfs-retire'); }};
        provider.textReplicas = new Map([[
            'main.tex',
            {
                prepareForHandoff: async () => { events.push('flush'); },
                disposeAndWait: async () => { events.push('machine-dispose'); },
            },
        ]]);
        provider.textTask = async (_path, _source, task) => task();
        provider.dispose = () => { events.push('dispose'); };
        provider.textSchedulers = new Map();
        provider.reconcileQueue = new SerialTaskQueue();
        provider.unregisterRuntime = () => {};
        provider.lock = {release: async () => { events.push('lock-release'); }};
        provider.lockHeld = true;
        provider.stateStore = {close: async () => { events.push('state-close'); }};

        let releaseReconcile;
        const reconcileGate = new Promise(resolve => { releaseReconcile = resolve; });
        const activeReconcile = provider.reconcileQueue.run(async () => {
            events.push('reconcile-start');
            await reconcileGate;
            events.push('reconcile-end');
        });
        let releaseAiRequest;
        let activeAiRequest;
        activeAiRequest = new Promise(resolve => {
            releaseAiRequest = () => {
                events.push('ai-end');
                provider.activeAiGatewayRequests.delete(activeAiRequest);
                resolve();
            };
        });
        provider.activeAiGatewayRequests.add(activeAiRequest);
        let releaseTextRetirement;
        const textRetirement = new Promise(resolve => {
            releaseTextRetirement = resolve;
        }).finally(() => provider.textRetirements.delete('retiring.tex'));
        provider.textRetirements.set('retiring.tex', textRetirement);
        let releaseIsolatedRevision;
        const isolatedRevision = new Promise(resolve => {
            releaseIsolatedRevision = resolve;
        }).finally(() => provider.isolatedRevisionTasks.delete('isolated.tex'));
        provider.isolatedRevisionTasks.set('isolated.tex', isolatedRevision);
        const handoff = provider.handleAiGatewayRequest({
            method: 'handoff',
            replicaPath: '/tmp/hershel-handoff-fixture',
            requesterPid: process.pid + 1000,
        });
        await new Promise(resolve => setImmediate(resolve));
        assert(!events.includes('flush'), 'handoff passed an active reconcile task');
        releaseReconcile();
        await activeReconcile;
        await new Promise(resolve => setImmediate(resolve));
        assert(!events.includes('flush'), 'handoff passed an active AI gateway request');
        releaseAiRequest();
        await new Promise(resolve => setImmediate(resolve));
        assert(!events.includes('flush'), 'handoff passed a retiring text owner');
        releaseTextRetirement();
        await new Promise(resolve => setImmediate(resolve));
        assert(!events.includes('flush'), 'handoff passed an isolated revision archive');
        releaseIsolatedRevision();
        const result = await handoff;
        await new Promise(resolve => setTimeout(resolve, 10));

        assert.deepStrictEqual(result, {
            closing: true,
            previousOwnerPid: process.pid,
        });
        assert(events.includes('flush'));
        assert(!events.includes('machine-dispose'));
        assert(!events.includes('vfs-retire'));
        assert(!events.includes('state-close'));
        assert(!events.includes('lock-release'));
        assert(events.indexOf('ai-end') < events.indexOf('flush'));
        assert(events.indexOf('reconcile-end') < events.indexOf('flush'));
        assert.strictEqual(saveCount, 0);
        assert.deepStrictEqual(commands, ['workbench.action.closeWindow']);
        assert.strictEqual(provider.disposed, false);
        assert.strictEqual(provider.handoffInProgress, false);
        assert.strictEqual(provider.observationReady, true);
        assert.strictEqual(provider.lockHeld, true);
    });

    it('refuses to close a window with dirty isolated bytes', async () => {
        const document = {
            uri: {fsPath: '/tmp/hershel-handoff-fixture/main.tex'},
            isDirty: true,
        };
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            workspace: {textDocuments: [document]},
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.baseUri = {fsPath: '/tmp/hershel-handoff-fixture'};
        provider.observationReady = true;
        provider.disposed = false;
        provider.handoffDeferredEditorObservations = new Map();
        provider.textReplicas = new Map([['main.tex', {isIsolated: true}]]);
        provider.relativeLocalPath = () => 'main.tex';
        provider.isIgnored = () => false;

        await assert.rejects(
            () => provider.handleAiGatewayRequest({
                method: 'handoff',
                replicaPath: '/tmp/hershel-handoff-fixture',
                requesterPid: process.pid + 1000,
            }),
            /untracked or isolated dirty document/,
        );
        assert.strictEqual(provider.observationReady, true);
        assert.strictEqual(provider.handoffInProgress, false);
    });

    it('does not turn expected retirement cancellation into a late conflict', () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.disposed = false;
        provider.handoffInProgress = true;
        provider.createTextConflict = () => {
            throw new Error('retirement tried to persist a conflict');
        };

        assert.doesNotThrow(() => provider.handleTextReplicaError(
            'main.tex',
            new Error('Document scheduler disposed with pending work'),
        ));
    });
});

describe('Local Replica owner retirement', () => {
    function retirementProvider(LocalReplicaSCMProvider, events, failStateClose = false) {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.disposed = false;
        provider.handoffInProgress = false;
        provider.retirementPromise = undefined;
        provider.observationReady = true;
        provider.lockOwnershipLost = false;
        provider.activeAiGatewayRequests = new Set();
        provider.textConflictTasks = new Map();
        provider.textRetirements = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.pendingReasons = new Set();
        provider.reconcileQueue = new SerialTaskQueue();
        provider.lifecycleDisposables = [{dispose: () => events.push('listeners-stop')}];
        provider.textReplicas = new Map([[
            'main.tex',
            {
                prepareForHandoff: async () => events.push('editor-flush'),
                disposeAndWait: async () => events.push('machine-stop'),
            },
        ]]);
        provider.textSchedulers = new Map([[
            'main.tex',
            {disposeAndWait: async () => events.push('scheduler-stop')},
        ]]);
        provider.textTask = async (_path, _source, task) => task();
        provider.stopAiGateway = async () => events.push('gateway-stop');
        provider.vfs = {retireForReplicaHandoff: async () => events.push('vfs-stop')};
        provider.stateStore = {
            close: async () => {
                events.push('state-close');
                if (failStateClose) throw new Error('state close failed');
            },
        };
        provider.lock = {
            release: async () => events.push('lock-release'),
            abandon: () => events.push('lock-abandon'),
        };
        provider.unregisterRuntime = () => events.push('runtime-unregister');
        provider.editorObservationHeads = new Map();
        provider.output = {
            appendLine: message => events.push(message),
            dispose: () => events.push('output-dispose'),
        };
        return provider;
    }

    it('does not report settled while a text owner or isolated revision is draining', () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const basePath = '/tmp/hershel-runtime-retirement';
        provider.disposed = false;
        provider.lockHeld = true;
        provider.observationReady = true;
        provider.handoffInProgress = false;
        provider.startupSettled = true;
        provider.pausedError = undefined;
        provider.activeReconcileCount = 0;
        provider.reconcileQueue = {isIdle: true};
        provider.reconcileTimer = undefined;
        provider.pendingReasons = new Set();
        provider.activeAiGatewayRequests = new Set();
        provider.aiTransactions = new Map();
        provider.pendingEditorObservations = new Map();
        provider.editorObservationDrains = new Map();
        provider.textConflictTasks = new Map();
        provider.isolatedRevisionTasks = new Map([['main.tex', new Promise(() => {})]]);
        provider.textRetirements = new Map([['other.tex', new Promise(() => {})]]);
        provider.textSchedulers = new Map();
        provider.textReplicas = new Map();
        provider.vfs = {
            isSafeSyncConnectionReady: true,
            hasPendingDocumentOperations: false,
        };
        provider.state = {initialized: true, conflicts: {}};
        provider.status = {status: 'idle'};
        LocalReplicaSCMProvider.activeInstances.set(basePath, provider);
        try {
            assert.strictEqual(
                LocalReplicaSCMProvider.getRuntimeStatus(basePath).settled,
                false,
            );
            provider.isolatedRevisionTasks.clear();
            provider.textRetirements.clear();
            assert.strictEqual(
                LocalReplicaSCMProvider.getRuntimeStatus(basePath).settled,
                true,
            );
        } finally {
            LocalReplicaSCMProvider.activeInstances.delete(basePath);
        }
    });

    it('releases the writer lock only after every durable owner closes', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const events = [];
        const provider = retirementProvider(LocalReplicaSCMProvider, events);

        provider.dispose();
        await provider.retirementPromise;

        assert(events.indexOf('lock-release') > events.indexOf('state-close'));
        assert(!events.includes('lock-abandon'));
        assert.deepStrictEqual(events.slice(0, 3), [
            'listeners-stop',
            'editor-flush',
            'scheduler-stop',
        ]);
    });

    it('retires a text owner attached by the reconcile already in flight', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const events = [];
        const provider = retirementProvider(LocalReplicaSCMProvider, events);
        provider.textReplicas.clear();
        provider.textSchedulers.clear();
        let releaseReconcile;
        const gate = new Promise(resolve => { releaseReconcile = resolve; });
        const activeReconcile = provider.reconcileQueue.run(async () => {
            events.push('reconcile-start');
            await gate;
            provider.textReplicas.set('late.tex', {
                prepareForHandoff: async () => events.push('late-editor-flush'),
                disposeAndWait: async () => events.push('late-machine-stop'),
            });
            provider.textSchedulers.set('late.tex', {
                run: async (_source, task) => task(),
                disposeAndWait: async () => events.push('late-scheduler-stop'),
            });
            events.push('reconcile-end');
        });

        provider.dispose();
        await new Promise(resolve => setImmediate(resolve));
        assert(!events.includes('lock-release'));
        releaseReconcile();
        await Promise.all([activeReconcile, provider.retirementPromise]);

        assert(events.includes('late-machine-stop'));
        assert(events.includes('late-scheduler-stop'));
        assert(events.indexOf('lock-release') > events.indexOf('late-machine-stop'));
    });

    it('does not release ownership while a path retirement is still draining', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const events = [];
        const provider = retirementProvider(LocalReplicaSCMProvider, events);
        let releaseRetirement;
        const retirement = new Promise(resolve => {
            releaseRetirement = resolve;
        }).finally(() => provider.textRetirements.delete('retiring.tex'));
        provider.textRetirements.set('retiring.tex', retirement);

        provider.dispose();
        await new Promise(resolve => setImmediate(resolve));
        assert(!events.includes('lock-release'));

        releaseRetirement();
        await provider.retirementPromise;
        assert(events.indexOf('lock-release') > events.indexOf('state-close'));
    });

    it('drains a path retirement that starts inside the shutdown barrier', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const events = [];
        const provider = retirementProvider(LocalReplicaSCMProvider, events);
        let releaseLateRetirement;
        provider.textSchedulers.set('main.tex', {
            disposeAndWait: async () => {
                events.push('scheduler-stop');
                const retirement = new Promise(resolve => {
                    releaseLateRetirement = resolve;
                }).finally(() => provider.textRetirements.delete('late.tex'));
                provider.textRetirements.set('late.tex', retirement);
            },
        });

        provider.dispose();
        while (!releaseLateRetirement) {
            await new Promise(resolve => setImmediate(resolve));
        }
        assert(!events.includes('lock-release'));

        releaseLateRetirement();
        await provider.retirementPromise;
        assert(events.indexOf('lock-release') > events.indexOf('state-close'));
    });

    it('retains ownership instead of admitting a successor after incomplete retirement', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const events = [];
        const provider = retirementProvider(LocalReplicaSCMProvider, events, true);

        provider.dispose();
        await provider.retirementPromise;

        assert(!events.includes('lock-abandon'));
        assert(!events.includes('lock-release'));
        assert(events.includes('vfs-stop'));
        assert(events.includes('machine-stop'));
    });
});

describe('remote delete failure semantics', () => {
    it('throws and keeps the cached entity when the API rejects deletion', async () => {
        const {vfs, parentFolder} = loadVirtualFileSystem({
            type: 'error',
            message: '403: forbidden',
        });
        await assert.rejects(
            vfs.remove({path: '/fixture.tex'}, true),
            /Could not delete remote doc \/fixture\.tex: 403: forbidden/,
        );
        assert.strictEqual(parentFolder.docs.length, 1);
    });

    it('removes the cached entity only after API success', async () => {
        const {vfs, parentFolder} = loadVirtualFileSystem({type: 'success'});
        await vfs.remove({path: '/fixture.tex'}, true);
        assert.strictEqual(parentFolder.docs.length, 0);
    });
});

describe('document recovery retirement', () => {
    it('keeps a terminal fence and the WAL when session close fails', async () => {
        const {vfs} = loadVirtualFileSystem({type: 'success'});
        const {DocumentJournal} = require('../out/sync/documentJournal');
        const removeJournal = DocumentJournal.remove;
        let journalRemoved = false;
        let legacyRemoved = false;
        DocumentJournal.remove = async () => { journalRemoved = true; };
        vfs.documentSessions = new Map([['remote-id', Promise.resolve({
            waitForTransportIdle: async () => {},
            close: async () => { throw new Error('close failed'); },
        })]]);
        vfs.documentSessionClosures = new Set();
        vfs.documentRecoveryFences = new Set();
        vfs.documentRecoveryDiscardTasks = new Map();
        vfs.logicalDocumentAcknowledgements = new Map();
        vfs.documentAcceptAdmissions = new Map();
        vfs.documentJournalEpochs = new Map();
        vfs.documentJournalDirectory = '/tmp/safe-sync-test';
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.legacyDocumentStateStore = {
            remove: async () => { legacyRemoved = true; },
        };
        vfs.socket = {leaveDoc: async () => {}};

        try {
            await assert.rejects(
                () => vfs.discardDocumentRecovery({path: '/fixture.tex'}),
                /close failed/,
            );
            assert.strictEqual(vfs.documentSessions.has('remote-id'), false);
            assert.strictEqual(vfs.documentRecoveryFences.has('remote-id'), true);
            assert.strictEqual(journalRemoved, false);
            assert.strictEqual(legacyRemoved, false);
        } finally {
            DocumentJournal.remove = removeJournal;
        }
    });
});

describe('remote tree event semantics', () => {
    it('applies duplicate remote move events idempotently', () => {
        const originalLoad = Module._load;
        Module._load = function(request) {
            if (request === 'vscode') {
                return {
                    Disposable: class Disposable {},
                    l10n: {t: value => value},
                    FileChangeType: {Created: 1, Deleted: 3},
                };
            }
            if (request === '../utils/globalStateManager') {
                return {GlobalStateManager: {}};
            }
            if (request === '../collaboration/clientManager') {
                return {ClientManager: class ClientManager {}};
            }
            if (request === '../utils/eventBus') {
                return {EventBus: {fire() {}}};
            }
            if (request === '../scm/scmCollectionProvider') {
                return {SCMCollectionProvider: class SCMCollectionProvider {}};
            }
            if (request === '../sync/documentSession') {
                return {DocumentSession: class DocumentSession {}};
            }
            if (request === '../sync/documentStateStore') {
                return {DocumentStateStore: class DocumentStateStore {}};
            }
            return originalLoad.apply(this, arguments);
        };
        let VirtualFileSystem;
        try {
            delete require.cache[require.resolve('../out/core/remoteFileSystemProvider')];
            ({VirtualFileSystem} = require('../out/core/remoteFileSystemProvider'));
        } finally {
            Module._load = originalLoad;
        }

        const document = {_id: 'doc-id', name: 'moved.tex'};
        const source = {
            _id: 'source-id', name: 'source', docs: [document], fileRefs: [], folders: [],
        };
        const destination = {
            _id: 'destination-id', name: 'destination', docs: [], fileRefs: [], folders: [],
        };
        const rootFolder = {
            _id: 'root-id', name: 'root', docs: [], fileRefs: [], folders: [source, destination],
        };
        const notifications = [];
        const vfs = Object.create(VirtualFileSystem.prototype);
        vfs.root = {rootFolder: [rootFolder]};
        vfs.documentSessions = new Map();
        vfs.notify = events => notifications.push(events);
        vfs.pathToUri = (...parts) => parts.join('/');

        vfs.applyRemoteMove(document._id, destination._id);
        vfs.applyRemoteMove(document._id, destination._id);

        assert.deepStrictEqual(source.docs, []);
        assert.deepStrictEqual(destination.docs, [document]);
        assert.strictEqual(notifications.length, 1);
    });
});

describe('Local Replica watcher snapshot semantics', () => {
    const LocalReplicaSCMProvider = loadProvider();

    it('projects every direct OT revision despite one coalesced filesystem event', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const observed = [];
        const scheduler = new FairTaskScheduler();
        const machine = {
            remoteEntityId: 'doc-1',
            onCanonicalChanged: async content => observed.push(content),
        };
        provider.observationReady = true;
        provider.textReplicas = new Map([['streaming.tex', machine]]);
        provider.textSchedulers = new Map([['streaming.tex', scheduler]]);
        provider.vfs = {hasLiveDocumentProjection: () => true};
        provider.relativeRemotePath = () => 'streaming.tex';
        provider.isIgnored = () => false;
        provider.readRemoteText = async () => {
            assert.fail('the coalesced watcher must not reread an active text projection');
        };
        provider.handleTextReplicaError = error => { throw error; };
        provider.scheduleReconcile = () => {};

        provider.onRemoteDocumentProjection({
            documentId: 'doc-1',
            path: '/streaming.tex',
            content: 'R1',
            revision: 1,
        });
        provider.onRemoteDocumentProjection({
            documentId: 'doc-1',
            path: '/streaming.tex',
            content: 'R2',
            revision: 2,
        });
        provider.onRemoteFileEvent({}, 'change');

        for (let attempt = 0; attempt < 20 && !scheduler.isIdle; attempt += 1) {
            await new Promise(resolve => setImmediate(resolve));
        }

        assert.strictEqual(scheduler.isIdle, true);
        assert.deepStrictEqual(observed, ['R1', 'R2']);
    });

    it('keeps the last same-document snapshot across a coalesced version jump', () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.editorObservationHeads = new Map();
        let content = 'before reload';
        const document = {
            version: 4,
            isDirty: false,
            getText: () => content,
        };
        provider.captureEditorObservation('fixture.tex', document);

        content = 'after external reload';
        document.version = 9;
        const aggregate = provider.captureEditorObservation('fixture.tex', document);
        assert.deepStrictEqual(aggregate, {
            content: 'after external reload',
            observedAncestor: 'before reload',
            editorDirty: false,
        });

        const reopened = {
            version: 10,
            isDirty: false,
            getText: () => 'new document instance',
        };
        assert.deepStrictEqual(
            provider.captureEditorObservation('fixture.tex', reopened),
            {content: 'new document instance', observedAncestor: undefined, editorDirty: false},
        );
    });

    it('keeps causal editor snapshots when VS Code versions stay equal or reset', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.editorObservationHeads = new Map();
        let content = 'base';
        const document = {
            version: 8,
            isDirty: false,
            getText: () => content,
        };
        provider.captureEditorObservation('fixture.tex', document);

        content = 'base1';
        const first = provider.captureEditorObservation('fixture.tex', document);
        document.version = 7;
        content = 'base12';
        const second = provider.captureEditorObservation('fixture.tex', document);

        assert.deepStrictEqual(first, {
            content: 'base1',
            observedAncestor: 'base',
            editorDirty: false,
        });
        assert.deepStrictEqual(second, {
            content: 'base12',
            observedAncestor: 'base1',
            editorDirty: false,
        });

        const commits = [];
        const isolations = [];
        const machine = new TextReplicaStateMachine({
            path: 'fixture.tex',
            canonicalContent: 'base',
            editorContent: 'base',
            diskContent: 'base',
        }, {
            commit: async (ancestor, target) => {
                commits.push({ancestor, target});
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
            projectEditor: async () => {},
            projectDisk: async () => {},
            isolate: async record => { isolations.push(record); },
        });
        await machine.onEditorChanged(first.content, first.observedAncestor, true);
        await machine.onEditorChanged(second.content, second.observedAncestor, true);
        await machine.flushInput(true);

        assert.deepStrictEqual(commits, [{ancestor: 'base', target: 'base12'}]);
        assert.deepStrictEqual(isolations, []);
        machine.dispose();
    });

    it('journals the editor snapshot captured at the change-event boundary', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const observed = [];
        let runQueuedTask;
        const machine = {
            onEditorChanged: async (content, ancestor) => observed.push({content, ancestor}),
        };
        provider.observationReady = true;
        provider.textReplicas = new Map([['fixture.tex', machine]]);
        provider.textTask = (_path, _source, task) => new Promise((resolve, reject) => {
            runQueuedTask = () => task().then(resolve, reject);
        });
        provider.status = {};
        provider.handleTextReplicaError = error => { throw error; };

        const pending = provider.onLocalTextDocumentChange(
            'fixture.tex',
            'event-time bytes',
            'event-time ancestor',
        );
        runQueuedTask();
        await pending;

        assert.deepStrictEqual(observed, [{
            content: 'event-time bytes',
            ancestor: 'event-time ancestor',
        }]);
    });

    it('coalesces queued human snapshots but preserves a plugin projection boundary', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const observed = [];
        const scheduled = [];
        let resumedProjections = 0;
        const machine = {
            isExpectedEditorProjection: content => content === 'remote+local',
            onEditorChanged: async (content, ancestor) => observed.push({content, ancestor}),
            isSettled: false,
            resumeProjection: async () => { resumedProjections += 1; },
        };
        provider.observationReady = true;
        provider.textReplicas = new Map([['fixture.tex', machine]]);
        provider.pendingEditorObservations = new Map();
        provider.editorObservationDrains = new Map();
        provider.textTask = (_path, _source, task) => new Promise((resolve, reject) => {
            scheduled.push(() => task().then(resolve, reject));
        });
        provider.status = {};
        provider.handleTextReplicaError = error => { throw error; };

        provider.enqueueLocalTextDocumentChange('fixture.tex', 'a', '', true);
        provider.enqueueLocalTextDocumentChange('fixture.tex', 'ab', 'a', true);
        provider.enqueueLocalTextDocumentChange('fixture.tex', 'remote+local', 'ab', true);
        provider.enqueueLocalTextDocumentChange('fixture.tex', 'remote+local!', 'remote+local', true);
        await new Promise(resolve => setImmediate(resolve));

        while (scheduled.length > 0 || provider.editorObservationDrains.size > 0) {
            scheduled.shift()?.();
            await new Promise(resolve => setImmediate(resolve));
        }

        assert.deepStrictEqual(observed, [
            {content: 'ab', ancestor: ''},
            {content: 'remote+local', ancestor: 'ab'},
            {content: 'remote+local!', ancestor: 'remote+local'},
        ]);
        // Draining the queue must re-enter any projection it deferred.
        assert.strictEqual(resumedProjections, 1);
    });

    it('does not admit a later editor snapshot ahead of an earlier queued event', () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const buffered = [];
        const queued = [];
        provider.textReplicas = new Map([['fixture.tex', {
            tryBufferEditorChange: (...args) => {
                buffered.push(args);
                return true;
            },
        }]]);
        provider.pendingEditorObservations = new Map([['fixture.tex', [{
            content: 'projection',
            observedAncestor: 'base',
            editorDirty: true,
            projectionBoundary: true,
        }]]]);
        provider.editorObservationDrains = new Map();
        provider.enqueueLocalTextDocumentChange = (...args) => queued.push(args);
        provider.status = {};

        provider.admitEditorObservation('fixture.tex', 'projectionX', 'projection', true);

        assert.deepStrictEqual(buffered, []);
        assert.deepStrictEqual(queued, [[
            'fixture.tex', 'projectionX', 'projection', true,
        ]]);
    });

    it('keeps projection provenance when a keystroke lands after a successful editor CAS', async () => {
        let text = 'base';
        const document = {
            uri: {fsPath: '/main.tex', toString: () => 'file:///main.tex'},
            getText: () => text,
            positionAt: offset => offset,
        };
        const LocalReplica = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            Range: class Range {
                constructor(start, end) {
                    this.start = start;
                    this.end = end;
                }
            },
            workspace: {textDocuments: [document]},
            window: {
                createOutputChannel: () => ({appendLine() {}, clear() {}, dispose() {}}),
                visibleTextEditors: [{
                    document,
                    edit: async callback => {
                        const builder = {
                            insert: (offset, replacement) => {
                                text = `${text.slice(0, offset)}${replacement}${text.slice(offset)}`;
                            },
                            replace: (range, replacement) => {
                                text = `${text.slice(0, range.start)}${replacement}`
                                    + `${text.slice(range.end)}`;
                            },
                        };
                        callback(builder);
                        text = `${text}X`;
                        return true;
                    },
                }],
            },
        });
        const provider = Object.create(LocalReplica.prototype);
        provider.localUri = () => document.uri;

        const applied = await provider.projectEditorCas('main.tex', 'base', 'remote-base');

        assert.strictEqual(applied, true);
        assert.strictEqual(text, 'remote-baseX');
    });

    it('reclassifies a queued change event whose current snapshot is deleted', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const observed = [];
        const reconciles = [];
        let dropped;
        const machine = {
            onDiskObserved: async content => observed.push(content),
        };
        provider.observationReady = true;
        provider.textReplicas = new Map([['fixture.tex', machine]]);
        provider.relativeLocalPath = () => 'fixture.tex';
        provider.isIgnored = () => false;
        provider.textTask = async (_path, _source, task) => task();
        provider.readLocalText = async () => undefined;
        provider.dropTextReplica = filePath => {
            dropped = filePath;
            provider.textReplicas.delete(filePath);
            return Promise.resolve();
        };
        provider.scheduleReconcile = (reason, delayMs) => reconciles.push({reason, delayMs});
        provider.handleTextReplicaError = error => { throw error; };

        provider.onLocalFileEvent({}, 'change');
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(observed, []);
        assert.strictEqual(dropped, 'fixture.tex');
        assert.deepStrictEqual(reconciles, [{
            reason: 'local delete observed after change: fixture.tex',
            delayMs: 0,
        }]);
    });

    it('does not drop a text owner for a transient delete emitted by projection CAS', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const observed = [];
        const machine = {onDiskObserved: async content => observed.push(content)};
        provider.observationReady = true;
        provider.textReplicas = new Map([['fixture.tex', machine]]);
        provider.relativeLocalPath = () => 'fixture.tex';
        provider.isIgnored = () => false;
        provider.textTask = async (_path, _source, task) => task();
        provider.readLocalText = async () => 'REMOTE';
        provider.dropTextReplica = () => assert.fail('transient delete retired the text owner');
        provider.scheduleReconcile = () => {};
        provider.handleTextReplicaError = error => { throw error; };

        provider.onLocalFileEvent({}, 'delete');
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(observed, ['REMOTE']);
        assert.strictEqual(provider.textReplicas.get('fixture.tex'), machine);
    });

    it('retries an absent snapshot when its watcher event crossed a projection epoch', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const retries = [];
        const machine = {onDiskObserved: async () => {}};
        provider.observationReady = true;
        provider.textReplicas = new Map([['fixture.tex', machine]]);
        provider.relativeLocalPath = () => 'fixture.tex';
        provider.isIgnored = () => false;
        provider.localProjectionCas = {observationEpoch: 1, hasActiveTransition: true};
        provider.textTask = async (_path, _source, task) => {
            provider.localProjectionCas = {observationEpoch: 2, hasActiveTransition: false};
            return task();
        };
        provider.readLocalText = async () => undefined;
        provider.retryLocalFileObservation = (uri, kind) => retries.push({uri, kind});
        provider.dropTextReplica = () => assert.fail('projection absence retired the text owner');
        provider.handleTextReplicaError = error => { throw error; };
        const uri = {fsPath: '/fixture.tex'};

        provider.onLocalFileEvent(uri, 'delete');
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(retries, [{uri, kind: 'delete'}]);
        assert.strictEqual(provider.textReplicas.get('fixture.tex'), machine);
    });

    it('coalesces watcher invalidations behind the preceding save boundary', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const observed = [];
        let latestDisk = 'FIRST';
        let releaseSave;
        let scheduledTasks = 0;
        const saveBoundary = new Promise(resolve => { releaseSave = resolve; });
        const machine = {onDiskObserved: async content => observed.push(content)};
        provider.observationReady = true;
        provider.disposed = false;
        provider.textReplicas = new Map([['fixture.tex', machine]]);
        provider.relativeLocalPath = () => 'fixture.tex';
        provider.isIgnored = () => false;
        provider.localProjectionCas = {observationEpoch: 1, hasActiveTransition: false};
        provider.pendingEditorSaveBoundaries = new Map();
        provider.deferredLocalFileObservations = new Map();
        provider.deferredLocalFileBoundaryWaits = new Map();
        provider.textTask = async (_path, _source, task) => {
            scheduledTasks += 1;
            return task();
        };
        provider.readLocalText = async () => latestDisk;
        provider.handleTextReplicaError = error => { throw error; };
        const uri = {fsPath: '/fixture.tex'};
        provider.trackEditorSaveBoundary('fixture.tex', saveBoundary);

        provider.onLocalFileEvent(uri, 'change');
        latestDisk = 'LATEST';
        provider.onLocalFileEvent(uri, 'change');
        assert.strictEqual(scheduledTasks, 0);

        releaseSave();
        await new Promise(resolve => setImmediate(resolve));
        await new Promise(resolve => setImmediate(resolve));

        assert.strictEqual(scheduledTasks, 1);
        assert.deepStrictEqual(observed, ['LATEST']);
    });

    it('does not let an old document owner consume a later path snapshot', async () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        let releaseRead;
        const readGate = new Promise(resolve => { releaseRead = resolve; });
        const observed = [];
        const oldMachine = {
            onDiskObserved: async content => observed.push(content),
        };
        const replacementMachine = {};
        provider.observationReady = true;
        provider.textReplicas = new Map([['fixture.tex', oldMachine]]);
        provider.relativeLocalPath = () => 'fixture.tex';
        provider.isIgnored = () => false;
        provider.textTask = async (_path, _source, task) => task();
        provider.readLocalText = async () => {
            await readGate;
            return 'replacement bytes';
        };
        provider.dropTextReplica = () => assert.fail('old owner tried to delete replacement');
        provider.scheduleReconcile = () => {};
        provider.handleTextReplicaError = error => { throw error; };

        provider.onLocalFileEvent({}, 'change');
        provider.textReplicas.set('fixture.tex', replacementMachine);
        releaseRead();
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(observed, []);
        assert.strictEqual(provider.textReplicas.get('fixture.tex'), replacementMachine);
    });
});

describe('converged replacement identity semantics', () => {
    it('adopts a replacement entity when both endpoints already have identical bytes', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            initialized: true,
            entries: {
                'figure.png': {
                    entityId: 'old-remote-id',
                    fileType: 'file',
                    hash: 'old-hash',
                },
            },
            conflicts: {},
        };
        provider.reconcileLocalWriterOverlap = async () => 'none';
        provider.reconcileActiveBinaryConflict = async () => 'none';
        provider.createConflict = async () => {
            throw new Error('identical endpoints must not conflict');
        };
        let decision;
        provider.applyDecision = async (_path, _baseline, _local, _remote, next) => {
            decision = next;
            return 'none';
        };
        const local = snapshot('figure.png', 'file');
        local.hash = 'new-shared-hash';
        const remote = {...local, entityId: 'new-remote-id'};

        assert.strictEqual(await provider.reconcileOne({
            local: new Map([['figure.png', local]]),
            remote: new Map([['figure.png', remote]]),
        }), 'none');
        assert.deepStrictEqual(decision, {action: 'baseline'});
    });

    it('uses the selected remote type while resolving a file-to-document replacement', async () => {
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            FileType: {File: 1, Directory: 2, SymbolicLink: 64},
            workspace: {
                textDocuments: [],
                fs: {
                    readDirectory: async () => [['reattach.bin', 1]],
                    readFile: async () => new Uint8Array(),
                },
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            initialized: true,
            entries: {
                'reattach.bin': {
                    entityId: 'old-file',
                    fileType: 'file',
                    hash: 'old-file-hash',
                },
            },
            conflicts: {},
        };
        provider.resolvingPaths = new Set(['reattach.bin']);
        provider.textReplicas = new Map();
        provider.isIgnored = () => false;
        provider.assertSafePath = () => {};
        provider.localUri = filePath => ({fsPath: path.join('/replica', filePath)});
        provider.localIdentity = async () => '1:2';
        const remote = new Map([['reattach.bin', {
            path: 'reattach.bin',
            entityId: 'replacement-doc',
            fileType: 'doc',
            hash: contentHash(new Uint8Array()),
            content: new Uint8Array(),
            text: '',
        }]]);

        const resolving = await provider.scanLocal(remote);
        assert.strictEqual(resolving.get('reattach.bin').fileType, 'doc');

        provider.resolvingPaths.clear();
        const conservative = await provider.scanLocal(remote);
        assert.strictEqual(conservative.get('reattach.bin').fileType, 'file');
    });
});

describe('Local Replica isolated revision provenance', () => {
    function isolatedRevisionProvider(artifactDirectory) {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const written = [];
        const manifests = [];
        provider.resolvingPaths = new Set();
        provider.textConflictTasks = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.state = {
            initialized: true,
            entries: {},
            conflicts: {
                'main.tex': {
                    path: 'main.tex',
                    artifactDirectory,
                    localFile: `${artifactDirectory}/local/main.tex`,
                    localHash: 'original-local-hash',
                },
            },
        };
        provider.stateStore = {save: async () => {}};
        provider.writeArtifact = async (path, bytes) => {
            written.push([path, new TextDecoder().decode(bytes)]);
        };
        provider.writeConflictManifest = async (record, extra) => {
            manifests.push({localHash: record.localHash, ...extra});
        };
        provider.timestamp = () => '2026-01-01T00-00-00-000Z';
        provider.output = {appendLine() {}};
        return {provider, written, manifests};
    }

    it('records a real edit made while the path is isolated', async () => {
        const state = isolatedRevisionProvider('.overleaf/conflicts/unresolved/a');
        await state.provider.persistIsolatedTextRevision('main.tex', 'USER TYPED', 'editor');

        assert.strictEqual(state.written.length, 1);
        assert.strictEqual(state.written[0][1], 'USER TYPED');
        assert.strictEqual(state.manifests.length, 1);
        assert.notStrictEqual(
            state.provider.state.conflicts['main.tex'].localHash,
            'original-local-hash',
        );
    });

    it('ignores the bytes a resolution writes into the path it is resolving', async () => {
        // Resolving writes the chosen content into the conflicted path, and
        // the editor reports it back. Archiving that as the local side would
        // replace the user's text with our own output.
        const state = isolatedRevisionProvider('.overleaf/conflicts/unresolved/a');
        state.provider.resolvingPaths.add('main.tex');

        await state.provider.persistIsolatedTextRevision('main.tex', 'REMOTE CHOICE', 'editor');

        assert.deepStrictEqual(state.written, []);
        assert.deepStrictEqual(state.manifests, []);
        assert.strictEqual(
            state.provider.state.conflicts['main.tex'].localHash,
            'original-local-hash',
        );
    });

    it('never writes into a conflict archive that was already resolved', async () => {
        // An observation still in flight when the artifacts are renamed into
        // the resolved tree must not rewrite the manifest there.
        const state = isolatedRevisionProvider('.overleaf/conflicts/resolved/a');

        await state.provider.persistIsolatedTextRevision('main.tex', 'LATE ECHO', 'disk');

        assert.deepStrictEqual(state.written, []);
        assert.deepStrictEqual(state.manifests, []);
        assert.strictEqual(
            state.provider.state.conflicts['main.tex'].localHash,
            'original-local-hash',
        );
    });
});

describe('Local Replica explicit conflict transaction semantics', () => {
    it('applies a same-type local document choice through immutable-id OT before returning', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const content = new TextEncoder().encode('LOCAL');
        const local = {
            path: 'main.tex', fileType: 'doc', content, text: 'LOCAL',
            hash: contentHash(content),
        };
        const remote = {
            path: 'main.tex', fileType: 'doc', entityId: 'doc-1',
            hash: contentHash(new TextEncoder().encode('REMOTE')), text: 'REMOTE',
        };
        let committed;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.acceptPendingDocumentOperation = async (
            filePath, ancestor, target, expectedEntityId,
        ) => {
            committed = {filePath, ancestor, target, expectedEntityId};
            return target;
        };
        provider.updatePendingTarget = async () => {};
        provider.convergeTextToLocal = async (_path, authoritative) => authoritative;

        await provider.applyResolvedLocalSnapshot(local, remote);

        assert.deepStrictEqual(committed, {
            filePath: 'main.tex', ancestor: 'REMOTE', target: 'LOCAL', expectedEntityId: 'doc-1',
        });
    });

    it('safely retains a remote document before creating the selected local file type', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const events = [];
        const content = Buffer.from([0xff, 0x00, 0x01]);
        const local = {
            path: 'main.tex', fileType: 'file', content,
            hash: contentHash(content),
        };
        const remote = {
            path: 'main.tex', fileType: 'doc', entityId: 'doc-old',
            hash: 'remote-hash', text: 'REMOTE',
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.removeRemoteSnapshot = async current => {
            assert.strictEqual(current, remote);
            events.push('retain:doc-old');
        };
        provider.vfs = {
            createTypedFile: async (_uri, bytes, fileType) => {
                events.push(`create:${fileType}`);
                assert(Buffer.from(bytes).equals(content));
                return {entityId: 'file-new', content: bytes};
            },
        };
        provider.updatePendingTarget = async bytes => {
            events.push(`target:${contentHash(bytes)}`);
        };

        await provider.applyResolvedLocalSnapshot(local, remote);

        assert.deepStrictEqual(events, [
            'retain:doc-old',
            'create:file',
            `target:${local.hash}`,
        ]);
    });

    it('safely retains a remote file before creating the selected local document type', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const events = [];
        const content = new TextEncoder().encode('LOCAL\n');
        const local = {
            path: 'main.tex', fileType: 'doc', content, text: 'LOCAL\n',
            hash: contentHash(content),
        };
        const remote = {
            path: 'main.tex', fileType: 'file', entityId: 'file-old',
            hash: 'remote-hash',
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.removeRemoteSnapshot = async current => {
            assert.strictEqual(current, remote);
            events.push('retain:file-old');
        };
        provider.vfs = {
            createTypedFile: async (_uri, bytes, fileType) => {
                events.push(`create:${fileType}`);
                return {entityId: 'doc-new', content: bytes};
            },
        };
        provider.updatePendingTarget = async () => { events.push('target'); };

        await provider.applyResolvedLocalSnapshot(local, remote);

        assert.deepStrictEqual(events, ['retain:file-old', 'create:doc', 'target']);
    });

    it('archives a pending document WAL before committing its discard token', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const events = [];
        const record = {
            path: 'main.tex',
            artifactDirectory: '.overleaf/conflicts/unresolved/test',
            reason: 'test',
            createdAt: new Date(0).toISOString(),
            remoteEntityId: 'doc-1',
            remoteType: 'doc',
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {'main.tex': record},
        };
        provider.vfs = {
            prepareDocumentRecoveryById: async entityId => ({
                token: 'token-1', documentId: entityId,
                visibleContent: 'LOCAL', acknowledgedContent: 'REMOTE',
                revision: 3, pendingOperationIds: ['op-1'], epoch: 2,
                hash: 'snapshot-hash',
            }),
            commitDiscardDocumentRecovery: async token => {
                assert(events.includes('manifest') && events.includes('state'));
                assert(events.filter(event => event.startsWith('artifact:')).length === 2);
                events.push(`commit:${token}`);
            },
        };
        provider.writeArtifact = async filePath => { events.push(`artifact:${filePath}`); };
        provider.writeConflictManifest = async () => { events.push('manifest'); };
        provider.stateStore = {save: async () => { events.push('state'); }};
        const remote = {
            path: 'main.tex', fileType: 'doc', entityId: 'doc-1',
            hash: 'remote', text: 'REMOTE',
        };

        await provider.adjudicateDocumentRecoveries(record, {
            local: new Map(), remote: new Map([['main.tex', remote]]),
        });

        assert.strictEqual(events.at(-1), 'commit:token-1');
        assert.strictEqual(record.documentRecoveries[0].pendingOperationIds[0], 'op-1');
    });

    it('adjudicates every archived document identity across three replacements', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const record = {
            path: 'main.tex',
            artifactDirectory: '.overleaf/conflicts/unresolved/three-generations',
            reason: 'three immutable document generations',
            createdAt: new Date(0).toISOString(),
            baseType: 'doc',
            baselineEntityId: 'doc-oldest',
            remoteType: 'doc',
            remoteEntityId: 'doc-current',
            documentRecoveries: [{
                entityId: 'doc-middle',
                visibleHash: 'old-visible',
                acknowledgedHash: 'old-acknowledged',
                revision: 1,
                pendingOperationIds: ['old-op'],
                epoch: 1,
                snapshotHash: 'old-snapshot',
            }],
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    fileType: 'doc', entityId: 'doc-oldest', hash: 'base', text: 'BASE',
                },
            },
            conflicts: {'main.tex': record},
        };
        const prepared = [];
        const committed = [];
        provider.vfs = {
            prepareDocumentRecoveryById: async entityId => {
                prepared.push(entityId);
                return {
                    token: `token-${entityId}`,
                    documentId: entityId,
                    visibleContent: `VISIBLE-${entityId}`,
                    acknowledgedContent: `ACK-${entityId}`,
                    revision: 2,
                    pendingOperationIds: [`op-${entityId}`],
                    epoch: 2,
                    hash: `snapshot-${entityId}`,
                };
            },
            commitDiscardDocumentRecovery: async token => { committed.push(token); },
        };
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.stateStore = {save: async () => {}};
        const current = {
            path: 'main.tex', fileType: 'doc', entityId: 'doc-current',
            hash: 'remote', text: 'REMOTE',
        };

        await provider.adjudicateDocumentRecoveries(record, {
            local: new Map(), remote: new Map([['main.tex', current]]),
        });

        assert.deepStrictEqual(new Set(prepared), new Set([
            'doc-oldest', 'doc-current', 'doc-middle',
        ]));
        assert.deepStrictEqual(new Set(committed), new Set([
            'token-doc-oldest', 'token-doc-current', 'token-doc-middle',
        ]));
        assert.deepStrictEqual(
            new Set(record.documentRecoveries.map(recovery => recovery.entityId)),
            new Set(['doc-oldest', 'doc-current', 'doc-middle']),
        );
    });

    it('fences an orphan old-id WAL before startup can observe its replacement path', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: '550e8400-e29b-41d4-a716-446655440000',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'old-doc', fileType: 'doc',
                    hash: contentHash(new TextEncoder().encode('BASE')),
                    text: 'BASE',
                },
            },
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [{entity: {_id: 'new-doc', _type: 'doc'}, path: '/main.tex'}],
            readAuthoritativeFileWithoutRecovery: async () => ({
                entityId: 'new-doc', fileType: 'doc', revision: 1,
                content: new TextEncoder().encode('REPLACEMENT'),
            }),
        };
        provider.isIgnored = () => false;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.timestamp = () => 'time';
        provider.readCommonBase = async () => new TextEncoder().encode('BASE');
        const artifacts = new Map();
        provider.writeArtifact = async (filePath, content) => {
            artifacts.set(filePath, Buffer.from(content));
        };
        provider.writeConflictManifest = async record => {
            artifacts.set(`${record.artifactDirectory}/manifest.json`, Buffer.from('manifest'));
        };
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'old-doc',
            visibleContent: 'BASE LOCAL',
            acknowledgedContent: 'BASE',
            revision: 1,
            pendingOperationIds: ['op-old'],
            epoch: 0,
            hash: 'snapshot-hash',
        }]);

        const record = provider.state.conflicts['main.tex'];
        assert(record, 'orphan WAL was not fenced as a path conflict');
        assert.strictEqual(record.baselineEntityId, 'old-doc');
        assert.strictEqual(record.remoteEntityId, 'new-doc');
        assert.strictEqual(record.documentRecoveries[0].entityId, 'old-doc');
        assert.strictEqual(artifacts.get(record.localFile).toString(), 'BASE LOCAL');
        assert.strictEqual(artifacts.get(record.remoteFile).toString(), 'REPLACEMENT');
    });

    it('uses the WAL path binding when the current replica never stored a baseline path', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [{entity: {_id: 'old-doc', _type: 'doc'}, path: '/ignored/main.tex'}],
            readAuthoritativeFileWithoutRecovery: async () => ({
                entityId: 'old-doc', fileType: 'doc', revision: 4,
                content: new TextEncoder().encode('REMOTE'),
            }),
        };
        provider.isIgnored = () => true;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.timestamp = () => 'time';
        provider.readCommonBase = async () => undefined;
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'old-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 3,
            pendingOperationIds: ['op-old'],
            lastKnownPath: 'ignored/main.tex',
            epoch: 0,
            hash: 'snapshot-hash',
        }]);

        assert(provider.state.conflicts['ignored/main.tex']);
        assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
    });

    it('uses a live custom-ignored path instead of detaching its pending WAL', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [{entity: {_id: 'live-doc', _type: 'doc'}, path: '/ignored/main.tex'}],
            readAuthoritativeFileWithoutRecovery: async () => ({
                entityId: 'live-doc', fileType: 'doc', revision: 4,
                content: new TextEncoder().encode('REMOTE'),
            }),
            prepareDocumentRecoveryById: async () => {
                throw new Error('live path must prevent detached recovery');
            },
        };
        provider.isIgnored = () => true;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.timestamp = () => 'time';
        provider.readCommonBase = async () => undefined;
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'live-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 3,
            pendingOperationIds: ['op-live'],
            epoch: 0,
            hash: 'snapshot-hash',
        }]);

        assert(provider.state.conflicts['ignored/main.tex']);
        assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
    });

    it('uses an authoritative live hard-ignored path instead of detaching its pending WAL', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [{
                entity: {_id: 'live-reserved-doc', _type: 'doc'},
                path: '/.overleaf/internal.tex',
            }],
            readAuthoritativeFileWithoutRecovery: async () => ({
                entityId: 'live-reserved-doc', fileType: 'doc', revision: 4,
                content: new TextEncoder().encode('REMOTE'),
            }),
            prepareDocumentRecoveryById: async () => {
                throw new Error('authoritative live identity must prevent detached recovery');
            },
        };
        provider.isIgnored = () => true;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.timestamp = () => 'time';
        provider.readCommonBase = async () => undefined;
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'live-reserved-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 3,
            pendingOperationIds: ['op-live'],
            epoch: 0,
            hash: 'snapshot-hash',
        }]);

        assert(provider.state.conflicts['.overleaf/internal.tex']);
        assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
    });

    it('uses a v3 WAL path after remote delete-recreate even when the baseline was cleared', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [{entity: {_id: 'replacement-doc', _type: 'doc'}, path: '/main.tex'}],
            readAuthoritativeFileWithoutRecovery: async () => ({
                entityId: 'replacement-doc', fileType: 'doc', revision: 1,
                content: new TextEncoder().encode('REPLACEMENT'),
            }),
            prepareDocumentRecoveryById: async () => {
                throw new Error('WAL path binding must prevent detached recovery');
            },
        };
        provider.isIgnored = () => false;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.timestamp = () => 'time';
        provider.readCommonBase = async () => undefined;
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'old-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 3,
            pendingOperationIds: ['op-old'],
            lastKnownPath: 'main.tex',
            epoch: 0,
            hash: 'snapshot-hash',
        }]);

        const conflict = provider.state.conflicts['main.tex'];
        assert(conflict);
        assert.strictEqual(conflict.remoteEntityId, 'replacement-doc');
        assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
    });

    it('recovers an orphan path from an existing durable conflict record', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {
                'sections/main.tex': {
                    path: 'sections/main.tex',
                    artifactDirectory: '.overleaf/conflicts/unresolved/existing',
                    reason: 'existing recovery',
                    createdAt: new Date(0).toISOString(),
                    documentRecoveries: [{
                        entityId: 'old-doc',
                        visibleHash: contentHash(Buffer.from('OLDER')),
                        acknowledgedHash: contentHash(Buffer.from('BASE')),
                        revision: 1,
                        pendingOperationIds: ['older-op'],
                        epoch: 0,
                        snapshotHash: 'older-hash',
                    }],
                },
            },
        };
        provider.vfs = {
            walk: () => [],
            readAuthoritativeFileWithoutRecovery: async () => {
                throw new Error('absent');
            },
        };
        provider.isIgnored = () => false;
        provider.isFileNotFound = () => true;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.readCommonBase = async () => undefined;
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'old-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 2,
            pendingOperationIds: ['new-op'],
            epoch: 1,
            hash: 'new-hash',
        }]);

        assert.deepStrictEqual(
            provider.state.conflicts['sections/main.tex'].documentRecoveries
                .map(recovery => recovery.entityId),
            ['old-doc'],
        );
        assert.deepStrictEqual(
            provider.state.conflicts['sections/main.tex'].documentRecoveries[0]
                .pendingOperationIds,
            ['new-op'],
        );
    });

    it('uses retained remote conflict identity instead of detaching its pending WAL', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {
                'sections/retained.tex': {
                    path: 'sections/retained.tex',
                    artifactDirectory: '.overleaf/conflicts/unresolved/retained',
                    reason: 'retained remote candidate',
                    createdAt: new Date(0).toISOString(),
                    retainedRemoteEntityId: 'old-doc',
                },
            },
        };
        provider.vfs = {
            walk: () => [],
            readAuthoritativeFileWithoutRecovery: async () => {
                const error = new Error('absent');
                error.code = 'FileNotFound';
                throw error;
            },
            prepareDocumentRecoveryById: async () => {
                throw new Error('retained remote conflict path must prevent detached recovery');
            },
        };
        provider.isIgnored = () => false;
        provider.isFileNotFound = error => error.code === 'FileNotFound';
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.readCommonBase = async () => undefined;
        provider.writeArtifact = async () => {};
        provider.writeConflictManifest = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'old-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 2,
            pendingOperationIds: ['op-old'],
            epoch: 0,
            hash: 'snapshot-hash',
        }]);

        assert(provider.state.conflicts['sections/retained.tex'].documentRecoveries);
        assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
    });

    for (const durableOwner of [
        'pending action',
        'pending AI intent',
        'completed AI commit',
    ]) {
        it(`uses a ${durableOwner} path instead of detaching its pending WAL`, async () => {
            const LocalReplicaSCMProvider = loadProvider();
            const provider = Object.create(LocalReplicaSCMProvider.prototype);
            provider.state = {
                schemaVersion: 1,
                replicaId: 'replica',
                initialized: true,
                updatedAt: new Date(0).toISOString(),
                entries: {},
                conflicts: {},
                ...(durableOwner === 'pending action'
                    ? {
                        pendingAction: {
                            id: 'action-1',
                            kind: 'push',
                            path: 'sections/pending.tex',
                            startedAt: new Date(0).toISOString(),
                            entityId: 'old-doc',
                        },
                    }
                    : durableOwner === 'pending AI intent' ? {
                        aiCommitIntents: {
                            'lease-1': {
                                leaseId: 'lease-1',
                                fencingToken: 1,
                                path: 'sections/pending.tex',
                                entityId: 'old-doc',
                                baseHash: '1'.repeat(64),
                                targetHash: '2'.repeat(64),
                                operationId: 'operation-1',
                                createdAt: new Date(0).toISOString(),
                            },
                        },
                    } : {
                        aiCompletedCommits: {
                            'lease-1': {
                                leaseId: 'lease-1',
                                fencingToken: 1,
                                path: 'sections/pending.tex',
                                entityId: 'old-doc',
                                targetHash: '1'.repeat(64),
                                receiptContentHash: '2'.repeat(64),
                                receipt: {
                                    opId: 'operation-1',
                                    path: 'sections/pending.tex',
                                    journaled: true,
                                    remoteAcknowledged: false,
                                },
                                completedAt: new Date(0).toISOString(),
                            },
                        },
                    }),
            };
            provider.vfs = {
                walk: () => [],
                readAuthoritativeFileWithoutRecovery: async () => {
                    const error = new Error('absent');
                    error.code = 'FileNotFound';
                    throw error;
                },
                prepareDocumentRecoveryById: async () => {
                    throw new Error(`${durableOwner} path must prevent detached recovery`);
                },
            };
            provider.isIgnored = () => false;
            provider.isFileNotFound = error => error.code === 'FileNotFound';
            provider.remoteUri = pathname => ({path: `/${pathname}`});
            provider.timestamp = () => 'time';
            provider.readCommonBase = async () => undefined;
            provider.writeArtifact = async () => {};
            provider.writeConflictManifest = async () => {};
            provider.stateStore = {save: async () => {}};
            provider.output = {appendLine() {}};

            await provider.registerOrphanDocumentRecoveries([{
                documentId: 'old-doc',
                visibleContent: 'LOCAL',
                acknowledgedContent: 'BASE',
                revision: 3,
                pendingOperationIds: ['op-old'],
                epoch: 0,
                hash: 'snapshot-hash',
            }]);

            assert(provider.state.conflicts['sections/pending.tex']);
            assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
        });
    }

    it('archives and retires a truly unmapped legacy WAL without pausing the project', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        const committed = [];
        provider.vfs = {
            walk: () => [],
            resolveAuthoritativeDocumentPath: async () => undefined,
            prepareDocumentRecoveryById: async documentId => ({
                token: 'discard-token',
                documentId,
                visibleContent: 'LOCAL CANDIDATE',
                acknowledgedContent: 'LAST ACK',
                revision: 7,
                pendingOperationIds: ['legacy-op'],
                epoch: 0,
                hash: 'a'.repeat(64),
            }),
            commitDiscardDocumentRecovery: async token => committed.push(token),
        };
        provider.timestamp = () => 'time';
        provider.writeArtifact = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'legacy-doc',
            visibleContent: 'LOCAL CANDIDATE',
            acknowledgedContent: 'LAST ACK',
            revision: 7,
            pendingOperationIds: ['legacy-op'],
            epoch: 0,
            hash: 'a'.repeat(64),
        }]);

        const detached = provider.state.detachedDocumentRecoveries['legacy-doc'];
        assert(detached);
        assert.strictEqual(detached.walRetired, true);
        assert.deepStrictEqual(committed, ['discard-token']);
        assert.strictEqual(Object.keys(provider.state.conflicts).length, 0);
    });

    for (const reappearance of ['before prepare', 'after prepare']) {
        it(`does not detach when the immutable id reappears ${reappearance}`, async () => {
            const LocalReplicaSCMProvider = loadProvider();
            const provider = Object.create(LocalReplicaSCMProvider.prototype);
            provider.state = {
                schemaVersion: 1,
                replicaId: 'replica',
                initialized: true,
                updatedAt: new Date(0).toISOString(),
                entries: {},
                conflicts: {},
            };
            let pathChecks = 0;
            let prepared = 0;
            let artifacts = 0;
            provider.vfs = {
                walk: () => [],
                resolveAuthoritativeDocumentPath: async () => {
                    pathChecks++;
                    return reappearance === 'before prepare' || pathChecks === 2
                        ? 'sections/reappeared.tex'
                        : undefined;
                },
                prepareDocumentRecoveryById: async documentId => {
                    prepared++;
                    return {
                        token: 'discard-token',
                        documentId,
                        visibleContent: 'LOCAL CANDIDATE',
                        acknowledgedContent: 'LAST ACK',
                        revision: 7,
                        pendingOperationIds: ['legacy-op'],
                        epoch: 0,
                        hash: 'a'.repeat(64),
                    };
                },
                commitDiscardDocumentRecovery: async () => {
                    assert.fail('a reachable immutable id must keep its WAL');
                },
            };
            provider.timestamp = () => 'time';
            provider.writeArtifact = async () => { artifacts++; };
            provider.stateStore = {save: async () => {}};
            provider.output = {appendLine() {}};

            await assert.rejects(
                provider.registerOrphanDocumentRecoveries([{
                    documentId: 'legacy-doc',
                    visibleContent: 'LOCAL CANDIDATE',
                    acknowledgedContent: 'LAST ACK',
                    revision: 7,
                    pendingOperationIds: ['legacy-op'],
                    epoch: 0,
                    hash: 'a'.repeat(64),
                }]),
                /became reachable at sections\/reappeared\.tex/,
            );

            assert.strictEqual(prepared, reappearance === 'before prepare' ? 0 : 1);
            assert.strictEqual(artifacts, 0);
            assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
        });
    }

    it('retries a reappeared immutable id without pausing the project', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.currentActionPath = 'stale.tex';
        provider.output = {appendLine() {}};
        let retry;
        provider.scheduleReconcile = (reason, delay) => { retry = {reason, delay}; };
        provider.failProject = error => assert.fail(error.message);
        // Construct through the actual detached branch so instanceof routing is
        // tested without exporting an internal implementation error type.
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [],
            resolveAuthoritativeDocumentPath: async () => 'sections/reappeared.tex',
        };
        let routed;
        try {
            await provider.registerOrphanDocumentRecoveries([{
                documentId: 'old-doc',
                visibleContent: 'LOCAL',
                acknowledgedContent: 'BASE',
                revision: 1,
                pendingOperationIds: ['op-old'],
                epoch: 0,
                hash: 'snapshot-hash',
            }]);
        } catch (candidate) {
            routed = candidate;
        }
        await provider.handleReconcileError(routed);

        assert.strictEqual(provider.currentActionPath, undefined);
        assert.strictEqual(retry.delay, 0);
        assert.match(retry.reason, /became reachable at sections\/reappeared\.tex/);
    });

    it('does not detach a settled WAL snapshot with no pending operation', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [],
            prepareDocumentRecoveryById: async () => {
                throw new Error('settled WAL must not enter detached recovery');
            },
        };

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'settled-doc',
            visibleContent: 'SETTLED',
            acknowledgedContent: 'SETTLED',
            revision: 7,
            pendingOperationIds: [],
            epoch: 0,
            hash: 'b'.repeat(64),
        }]);

        assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
        assert.deepStrictEqual(provider.state.conflicts, {});
    });

    it('leaves a pending WAL on its live document identity for normal replay', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [{entity: {_id: 'live-doc', _type: 'doc'}, path: '/main.tex'}],
            prepareDocumentRecoveryById: async () => {
                throw new Error('live identity must not enter detached recovery');
            },
        };
        provider.isIgnored = () => false;

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'live-doc',
            visibleContent: 'LOCAL',
            acknowledgedContent: 'BASE',
            revision: 7,
            pendingOperationIds: ['pending-op'],
            epoch: 0,
            hash: 'c'.repeat(64),
        }]);

        assert.strictEqual(provider.state.detachedDocumentRecoveries, undefined);
        assert.deepStrictEqual(provider.state.conflicts, {});
    });

    it('recovers a detached artifact rename before startup observation', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const unresolved = '.overleaf/conflicts/unresolved/detached-wal-test';
        const resolved = '.overleaf/conflicts/resolved/detached-wal-test';
        const record = {
            documentId: 'legacy-doc',
            artifactDirectory: unresolved,
            reason: 'path unavailable',
            createdAt: new Date(0).toISOString(),
            visibleFile: `${unresolved}/local-candidate.txt`,
            acknowledgedFile: `${unresolved}/last-acknowledged-base.txt`,
            visibleHash: '1'.repeat(64),
            acknowledgedHash: '2'.repeat(64),
            revision: 1,
            pendingOperationIds: ['legacy-op'],
            epoch: 0,
            snapshotHash: '3'.repeat(64),
            walRetired: true,
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
            detachedDocumentRecoveries: {'legacy-doc': record},
        };
        provider.localArtifactExists = async filePath => filePath === resolved;
        let manifestWrites = 0;
        provider.writeDetachedRecoveryManifest = async () => { manifestWrites++; };
        let saves = 0;
        provider.stateStore = {save: async () => { saves++; }};

        await provider.recoverDetachedRecoveryArtifactLocations();

        assert.strictEqual(record.artifactDirectory, resolved);
        assert.strictEqual(record.visibleFile, `${resolved}/local-candidate.txt`);
        assert.strictEqual(record.acknowledgedFile, `${resolved}/last-acknowledged-base.txt`);
        assert.strictEqual(manifestWrites, 1);
        assert.strictEqual(saves, 1);
    });

    it('archives a terminal live-document WAL as a complete path conflict', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: '550e8400-e29b-41d4-a716-446655440000',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'live-doc', fileType: 'doc',
                    hash: contentHash(new TextEncoder().encode('BASE')),
                    text: 'BASE',
                },
            },
            conflicts: {},
        };
        provider.vfs = {
            walk: () => [{entity: {_id: 'live-doc', _type: 'doc'}, path: '/main.tex'}],
            readAuthoritativeFileWithoutRecovery: async () => ({
                entityId: 'live-doc', fileType: 'doc', revision: 12,
                content: new TextEncoder().encode('REMOTE'),
            }),
        };
        provider.isIgnored = () => false;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.timestamp = () => 'time';
        provider.readCommonBase = async () => new TextEncoder().encode('BASE');
        const artifacts = new Map();
        let manifestMetadata;
        provider.writeArtifact = async (filePath, content) => {
            artifacts.set(filePath, Buffer.from(content));
        };
        provider.writeConflictManifest = async (_record, metadata) => {
            manifestMetadata = metadata;
        };
        provider.stateStore = {save: async () => {}};
        provider.output = {appendLine() {}};

        await provider.registerOrphanDocumentRecoveries([{
            documentId: 'live-doc',
            path: '/main.tex',
            reason: 'OT history proof failed after one fresh generation: mismatch',
            failedAt: '2026-08-10T00:00:00.000Z',
            visibleContent: 'BASE LOCAL',
            acknowledgedContent: 'BASE',
            revision: 9,
            pendingOperationIds: ['op-live'],
            epoch: 3,
            hash: 'snapshot-hash',
        }]);

        const record = provider.state.conflicts['main.tex'];
        assert(record, 'terminal live WAL was skipped as an ordinary live document');
        assert.strictEqual(record.reason, 'OT history proof failed after one fresh generation: mismatch');
        assert.strictEqual(record.baseHash, provider.state.entries['main.tex'].hash);
        assert.strictEqual(record.remoteRevision, 12);
        assert.strictEqual(artifacts.get(record.baseFile).toString(), 'BASE');
        assert.strictEqual(artifacts.get(record.remoteFile).toString(), 'REMOTE');
        assert.strictEqual(artifacts.get(record.localFile).toString(), 'BASE LOCAL');
        assert.deepStrictEqual(record.documentRecoveries[0].pendingOperationIds, ['op-live']);
        assert.strictEqual(manifestMetadata.isolatedLiveDocumentRecovery, true);
        assert.strictEqual(manifestMetadata.recoveryWalRevision, 9);
        assert.deepStrictEqual(manifestMetadata.recoveryPendingOperationIds, ['op-live']);
    });

    it('rejects a missing projection transaction without durable final-candidate proof', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.localProjectionCas = {inspectRecovery: async () => ({status: 'missing'})};
        const record = {
            path: 'main.tex',
            artifactDirectory: '.overleaf/conflicts/unresolved/test',
            reason: 'test',
            createdAt: new Date(0).toISOString(),
            localHash: 'early-candidate',
            projectionRecoveryTransactionId: 'transaction-1',
        };

        await assert.rejects(
            () => provider.refreshProjectionRecoveryCandidate(record),
            /disappeared before its latest writer bytes were durably proven/,
        );
        assert.strictEqual(record.projectionRecoveryTransactionId, 'transaction-1');
    });

    function contentSnapshot(filePath, fileType, value, entityId) {
        const content = fileType === 'doc'
            ? new TextEncoder().encode(value)
            : Buffer.from(value);
        return {
            path: filePath,
            fileType,
            entityId,
            content,
            text: fileType === 'doc' ? value : undefined,
            hash: contentHash(content),
            localIdentity: entityId === undefined ? `inode:${filePath}` : undefined,
        };
    }

    function folderSnapshot(filePath, entityId) {
        return {
            path: filePath,
            fileType: 'folder',
            entityId,
            hash: 'folder',
            treeHash: `tree:${filePath}`,
            remoteStructureHash: entityId === undefined ? undefined : `structure:${filePath}`,
            localIdentity: entityId === undefined ? `inode:${filePath}` : undefined,
        };
    }

    function createFolderResolutionFixture(LocalReplicaSCMProvider, localEntries, remoteEntries) {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const local = new Map(localEntries.map(entry => [entry.path, entry]));
        const remote = new Map(remoteEntries.map(entry => [entry.path, entry]));
        const journals = [];
        const mutations = [];
        const durablePreStates = [];
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.stateStore = {
            saveBaseline: async content => contentHash(content),
            save: async state => {
                if (state.pendingAction) {
                    durablePreStates.push(`${state.pendingAction.kind}:${state.pendingAction.path}`);
                }
            },
        };
        provider.scanBoth = async () => ({local: new Map(local), remote: new Map(remote)});
        provider.refreshConvergedState = async () => {};
        provider.remoteUri = filePath => ({path: `/${filePath}`});
        provider.convergeTextToLocal = async (_filePath, authoritativeText) => authoritativeText;
        provider.removeRemoteSnapshot = async snapshot => {
            mutations.push(`delete:${snapshot.path}`);
            remote.delete(snapshot.path);
        };
        const pathFromUri = uri => uri.path.slice(1);
        provider.vfs = {
            mkdir: async uri => {
                const filePath = pathFromUri(uri);
                mutations.push(`mkdir:${filePath}`);
                remote.set(filePath, folderSnapshot(filePath, `remote:${filePath}`));
            },
            createTypedFile: async (uri, content, fileType) => {
                const filePath = pathFromUri(uri);
                mutations.push(`create:${filePath}`);
                const value = fileType === 'doc'
                    ? new TextDecoder().decode(content)
                    : Buffer.from(content);
                const created = contentSnapshot(filePath, fileType, value, `remote:${filePath}`);
                remote.set(filePath, created);
                return {entityId: created.entityId, content: created.content};
            },
            acceptDocumentSnapshotAgainstAncestor: async (
                uri, _ancestor, target, _barrier, expectedEntityId, opId,
            ) => {
                const filePath = pathFromUri(uri);
                assert.strictEqual(remote.get(filePath).entityId, expectedEntityId);
                mutations.push(`write-doc:${filePath}`);
                const updated = contentSnapshot(filePath, 'doc', target, expectedEntityId);
                remote.set(filePath, updated);
                return {opId, content: target, acknowledgement: Promise.resolve(target)};
            },
            writeResourceAgainstBaseline: async (uri, entityId, expectedHash, content) => {
                const filePath = pathFromUri(uri);
                assert.strictEqual(remote.get(filePath).entityId, entityId);
                assert.strictEqual(remote.get(filePath).hash, expectedHash);
                mutations.push(`write-file:${filePath}`);
                remote.set(
                    filePath,
                    contentSnapshot(filePath, 'file', Buffer.from(content), entityId),
                );
                return entityId;
            },
        };
        const journalAction = provider.journalAction.bind(provider);
        provider.journalAction = async (...args) => {
            journals.push(`${args[0]}:${args[1]}`);
            return journalAction(...args);
        };
        return {provider, local, remote, journals, mutations, durablePreStates};
    }

    it('converges a local folder choice resource-by-resource, including remote extras', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const localEntries = [
            folderSnapshot('sections'),
            contentSnapshot('sections/main.tex', 'doc', 'LOCAL DOC'),
            folderSnapshot('sections/images'),
            contentSnapshot('sections/images/plot.png', 'file', Buffer.from([1, 2, 3])),
        ];
        const remoteEntries = [
            folderSnapshot('sections', 'folder-sections'),
            contentSnapshot('sections/main.tex', 'doc', 'REMOTE DOC', 'doc-main'),
            folderSnapshot('sections/images', 'folder-images'),
            contentSnapshot('sections/images/plot.png', 'file', Buffer.from([9]), 'file-plot'),
            contentSnapshot('sections/obsolete.tex', 'doc', 'REMOTE EXTRA', 'doc-extra'),
        ];
        const fixture = createFolderResolutionFixture(
            LocalReplicaSCMProvider,
            localEntries,
            remoteEntries,
        );

        await fixture.provider.convergeArchivedLocalResolution({path: 'sections'});

        assert.deepStrictEqual(fixture.journals, [
            'delete-remote:sections/obsolete.tex',
            'resolve-conflict:sections/main.tex',
            'resolve-conflict:sections/images/plot.png',
        ]);
        assert.deepStrictEqual(fixture.mutations, [
            'delete:sections/obsolete.tex',
            'write-doc:sections/main.tex',
            'write-file:sections/images/plot.png',
        ]);
        assert.deepStrictEqual(
            fixture.durablePreStates.filter((entry, index, all) => all.indexOf(entry) === index),
            fixture.journals,
        );
        assert.strictEqual(fixture.remote.has('sections/obsolete.tex'), false);
        for (const [filePath, local] of fixture.local) {
            const remote = fixture.remote.get(filePath);
            assert(remote, `missing remote resource ${filePath}`);
            assert.strictEqual(remote.fileType, local.fileType);
            assert.strictEqual(remote.hash, local.hash);
        }
    });

    it('publishes every nested resource in a restored Keep Both folder sibling', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const sibling = 'sections.local-copy';
        const localEntries = [
            folderSnapshot(sibling),
            contentSnapshot(`${sibling}/appendix.tex`, 'doc', 'RESTORED DOC'),
            folderSnapshot(`${sibling}/figures`),
            contentSnapshot(`${sibling}/figures/result.png`, 'file', Buffer.from([4, 5, 6])),
        ];
        const canonical = folderSnapshot('sections', 'folder-canonical');
        const fixture = createFolderResolutionFixture(
            LocalReplicaSCMProvider,
            localEntries,
            [canonical],
        );

        await fixture.provider.convergeRestoredSibling(sibling);

        assert.deepStrictEqual(fixture.journals, [
            `resolve-conflict:${sibling}`,
            `resolve-conflict:${sibling}/appendix.tex`,
            `resolve-conflict:${sibling}/figures`,
            `resolve-conflict:${sibling}/figures/result.png`,
        ]);
        assert.strictEqual(fixture.remote.get('sections'), canonical);
        for (const [filePath, local] of fixture.local) {
            const remote = fixture.remote.get(filePath);
            assert(remote, `missing restored remote resource ${filePath}`);
            assert.strictEqual(remote.fileType, local.fileType);
            assert.strictEqual(remote.hash, local.hash);
        }
    });

    it('refuses to clean a resolved directory still referenced by an active conflict', async () => {
        let deleteCalls = 0;
        let confirmationCalls = 0;
        const errors = [];
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            window: {
                showErrorMessage: message => { errors.push(message); },
                showWarningMessage: async () => {
                    confirmationCalls++;
                    return 'Delete resolved artifacts';
                },
            },
            workspace: {
                fs: {delete: async () => { deleteCalls++; }},
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            initialized: true,
            entries: {},
            conflicts: {
                'main.tex': {
                    path: 'main.tex',
                    artifactDirectory: '.overleaf/conflicts/resolved/crash-window',
                },
            },
        };

        await provider.cleanupResolvedArtifacts();

        assert.strictEqual(deleteCalls, 0);
        assert.strictEqual(confirmationCalls, 0);
        assert.strictEqual(errors.length, 1);
        assert.match(errors[0], /active conflict recovery evidence/);
    });

    it('rechecks active recovery evidence after the cleanup confirmation modal', async () => {
        let deleteCalls = 0;
        let confirm;
        const confirmation = new Promise(resolve => { confirm = resolve; });
        const errors = [];
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            window: {
                showErrorMessage: message => { errors.push(message); },
                showWarningMessage: async () => confirmation,
            },
            workspace: {
                fs: {delete: async () => { deleteCalls++; }},
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            initialized: true,
            entries: {},
            conflicts: {},
        };
        provider.reconcileQueue = new SerialTaskQueue();
        provider.localArtifactExists = async () => false;
        provider.localUri = pathname => ({path: pathname});

        const cleanup = provider.cleanupResolvedArtifacts();
        await new Promise(resolve => setImmediate(resolve));
        provider.state.conflicts['main.tex'] = {
            path: 'main.tex',
            artifactDirectory: '.overleaf/conflicts/resolved/newly-active',
        };
        confirm('Delete resolved artifacts');
        await cleanup;

        assert.strictEqual(deleteCalls, 0);
        assert.strictEqual(errors.length, 1);
        assert.match(errors[0], /gained active conflict recovery evidence/);
    });
});

describe('Local Replica directory deletion CAS', () => {
    function directorySnapshot(pathname, treeHash, localIdentity) {
        return {
            path: pathname,
            fileType: 'folder',
            hash: 'folder',
            treeHash,
            localIdentity,
        };
    }

    it('refuses to recursively delete a folder when a child appeared after the scan', async () => {
        const deletes = [];
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            workspace: {
                fs: {
                    readDirectory: async () => [['section.tex', 1], ['new.tex', 1]],
                    readFile: async () => new TextEncoder().encode('section'),
                    delete: async (uri, options) => { deletes.push({path: uri.path, options}); },
                },
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const content = new TextEncoder().encode('section');
        const expectedRoot = directorySnapshot('chapters', 'tree-before', 'directory-before');
        const expected = new Map([
            ['chapters', expectedRoot],
            ['chapters/section.tex', {
                path: 'chapters/section.tex',
                fileType: 'file',
                hash: contentHash(content),
                localIdentity: 'section-before',
            }],
        ]);
        const current = new Map([
            ['chapters', directorySnapshot('chapters', 'tree-after', 'directory-before')],
            ['chapters/section.tex', expected.get('chapters/section.tex')],
            ['chapters/new.tex', {
                path: 'chapters/new.tex',
                fileType: 'file',
                hash: contentHash(new TextEncoder().encode('new')),
                localIdentity: 'new-after',
            }],
        ]);
        provider.lastScan = {remote: new Map()};
        provider.scanLocal = async () => current;
        provider.localUri = pathname => ({path: pathname, fsPath: `/${pathname}`});
        provider.localIdentity = async uri => uri.path === 'chapters'
            ? 'directory-before'
            : 'section-before';
        await assert.rejects(
            () => provider.deleteLocalSnapshot('chapters', expectedRoot, expected),
            /Local directory changed before deletion: chapters/,
        );
        assert.deepStrictEqual(deletes, []);
    });

    it('refuses a replaced folder even when its recursive content is unchanged', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const content = new TextEncoder().encode('section');
        const expectedRoot = directorySnapshot('chapters', 'tree-before', 'directory-before');
        const expected = new Map([
            ['chapters', expectedRoot],
            ['chapters/section.tex', {
                path: 'chapters/section.tex',
                fileType: 'file',
                hash: contentHash(content),
                localIdentity: 'section-before',
            }],
        ]);
        const current = new Map([
            ['chapters', directorySnapshot('chapters', 'tree-before', 'directory-after')],
            ['chapters/section.tex', expected.get('chapters/section.tex')],
        ]);
        provider.lastScan = {remote: new Map()};
        provider.scanLocal = async () => current;

        await assert.rejects(
            () => provider.deleteLocalSnapshot('chapters', expectedRoot, expected),
            /Local directory changed before deletion: chapters/,
        );
    });

    it('deletes a proven directory tree member-by-member without recursive deletion', async () => {
        const content = new TextEncoder().encode('section');
        const deletes = [];
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            workspace: {
                fs: {
                    readDirectory: async uri => {
                        assert.strictEqual(uri.path, 'chapters');
                        return [['section.tex', 1]];
                    },
                    readFile: async uri => {
                        assert.strictEqual(uri.path, 'chapters/section.tex');
                        return content;
                    },
                    delete: async (uri, options) => { deletes.push({path: uri.path, options}); },
                },
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const root = directorySnapshot('chapters', 'tree-before', 'directory-before');
        const expected = new Map([
            ['chapters', root],
            ['chapters/section.tex', {
                path: 'chapters/section.tex',
                fileType: 'file',
                hash: contentHash(content),
                localIdentity: 'section-before',
            }],
        ]);
        provider.lastScan = {remote: new Map()};
        provider.scanLocal = async () => expected;
        provider.localUri = pathname => ({path: pathname, fsPath: `/${pathname}`});
        provider.localIdentity = async uri => uri.path === 'chapters'
            ? 'directory-before'
            : 'section-before';
        provider.localProjectionCas = {
            remove: async (pathname, expectedHash) => {
                deletes.push({path: pathname, expectedHash});
                return {applied: true};
            },
        };

        await provider.deleteLocalSnapshot('chapters', root, expected);

        assert.deepStrictEqual(deletes, [
            {path: 'chapters/section.tex', expectedHash: contentHash(content)},
            {path: 'chapters', options: {recursive: false, useTrash: false}},
        ]);
    });

    it('treats a late non-recursive directory delete failure as a stale snapshot', async () => {
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            workspace: {
                fs: {
                    readDirectory: async () => [],
                    delete: async () => { throw new Error('directory is no longer empty'); },
                },
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const root = directorySnapshot('chapters', 'empty-tree', 'directory-before');
        const expected = new Map([['chapters', root]]);
        provider.lastScan = {remote: new Map()};
        provider.scanLocal = async () => expected;
        provider.localUri = pathname => ({path: pathname, fsPath: `/${pathname}`});
        provider.localIdentity = async () => 'directory-before';

        await assert.rejects(
            () => provider.deleteLocalSnapshot('chapters', root, expected),
            /Local directory changed during deletion: chapters/,
        );
    });

    it('uses the dirty editor endpoint for a proven conflict deletion', async () => {
        const deletes = [];
        const editorText = 'archived dirty editor';
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            workspace: {
                textDocuments: [{
                    uri: {fsPath: '/main.tex'},
                    getText: () => editorText,
                }],
                fs: {},
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.localUri = () => ({fsPath: '/main.tex'});
        provider.localIdentity = async () => 'same-inode';
        provider.localProjectionCas = {
            remove: async (pathname, expectedHash) => {
                deletes.push({pathname, expectedHash});
                return {applied: true};
            },
        };
        const expected = {
            path: 'main.tex',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode(editorText)),
            localIdentity: 'same-inode',
        };

        await provider.deleteLocalSnapshot('main.tex', expected);

        assert.deepStrictEqual(deletes, [{
            pathname: 'main.tex',
            expectedHash: expected.hash,
        }]);
    });
});

describe('Local Replica local-delete confirmation', () => {
    it('requires a stable local absence before deleting the unchanged remote document', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: 'base',
                    text: 'base',
                },
            },
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.localDeletionCandidates = new Map();
        provider.reconcileLocalWriterOverlap = async () => 'none';
        provider.reconcileActiveBinaryConflict = async () => 'none';
        const scheduled = [];
        provider.scheduleReconcile = (reason, delay) => scheduled.push({reason, delay});
        let applied;
        provider.applyDecision = async (_path, _baseline, _local, _remote, decision) => {
            applied = decision;
            return 'acted';
        };
        const remote = {
            path: 'main.tex',
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'base',
            text: 'base',
        };
        const scan = {local: new Map(), remote: new Map([['main.tex', remote]])};

        assert.strictEqual(await provider.reconcileOne(scan), 'waiting');
        assert.strictEqual(applied, undefined);
        assert.strictEqual(scheduled.length, 1);

        provider.localDeletionCandidates.get('main.tex').since = Date.now() - 1000;
        assert.strictEqual(await provider.reconcileOne(scan), 'acted');
        assert.deepStrictEqual(applied, {action: 'delete-remote'});
    });

    it('drops a deletion candidate when the projected local file reappears', () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.localDeletionCandidates = new Map([['main.tex', {
            remoteEntityId: 'doc-1', remoteHash: 'base', since: Date.now(),
        }]]);

        provider.clearLocalDeletionCandidates({
            local: new Map([['main.tex', snapshot('main.tex', 'doc')]]),
            remote: new Map(),
        });

        assert.strictEqual(provider.localDeletionCandidates.size, 0);
    });

    it('requires a stable local absence before a remote change becomes a conflict', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: 'base',
                    text: 'base',
                },
            },
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.localDeletionCandidates = new Map();
        provider.reconcileLocalWriterOverlap = async () => 'none';
        provider.reconcileActiveBinaryConflict = async () => 'none';
        const scheduled = [];
        provider.scheduleReconcile = (reason, delay) => scheduled.push({reason, delay});
        let applied;
        provider.applyDecision = async (_path, _baseline, _local, _remote, decision) => {
            applied = decision;
            return 'acted';
        };
        // The remote document moved on while the pathname was not observable.
        const remote = {
            path: 'main.tex',
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'remote-next',
            text: 'remote next',
        };
        const scan = {local: new Map(), remote: new Map([['main.tex', remote]])};

        assert.strictEqual(await provider.reconcileOne(scan), 'waiting');
        assert.strictEqual(applied, undefined);
        assert.strictEqual(scheduled.length, 1);

        // The pathname came back: the absence was a projection window, and no
        // conflict record was ever written for it.
        provider.clearLocalDeletionCandidates({
            local: new Map([['main.tex', snapshot('main.tex', 'doc')]]),
            remote: scan.remote,
        });
        assert.strictEqual(provider.localAbsences.size, 0);
    });

    it('requires a stable remote absence before a replaced entity deletes or conflicts', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'reattach.bin': {
                    entityId: 'file-1',
                    fileType: 'file',
                    hash: 'base',
                },
            },
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.localDeletionCandidates = new Map();
        provider.reconcileLocalWriterOverlap = async () => 'none';
        provider.reconcileActiveBinaryConflict = async () => 'none';
        const scheduled = [];
        provider.scheduleReconcile = (reason, delay) => scheduled.push({reason, delay});
        let applied;
        provider.applyDecision = async (_path, _baseline, _local, _remote, decision) => {
            applied = decision;
            return 'acted';
        };
        // The entity was deleted and is about to be recreated at the same path,
        // and the local file was modified in the meantime.
        const local = {
            path: 'reattach.bin',
            fileType: 'file',
            hash: 'local-next',
            localIdentity: 'inode',
        };
        const scan = {local: new Map([['reattach.bin', local]]), remote: new Map()};

        assert.strictEqual(await provider.reconcileOne(scan), 'waiting');
        assert.strictEqual(applied, undefined);
        assert.strictEqual(scheduled.length, 1);

        // The replacement landed: the vacancy was the gap between two updates.
        provider.clearLocalDeletionCandidates({
            local: scan.local,
            remote: new Map([['reattach.bin', {
                path: 'reattach.bin',
                entityId: 'doc-2',
                fileType: 'doc',
                hash: 'replacement',
                text: 'replacement',
            }]]),
        });
        assert.strictEqual(provider.remoteAbsences.size, 0);
    });

    it('deletes locally once a remote absence holds still across the window', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'reattach.bin': {
                    entityId: 'file-1',
                    fileType: 'file',
                    hash: 'base',
                },
            },
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.localDeletionCandidates = new Map();
        provider.reconcileLocalWriterOverlap = async () => 'none';
        provider.reconcileActiveBinaryConflict = async () => 'none';
        provider.scheduleReconcile = () => {};
        let applied;
        provider.applyDecision = async (_path, _baseline, _local, _remote, decision) => {
            applied = decision;
            return 'acted';
        };
        const scan = {
            local: new Map([['reattach.bin', {
                path: 'reattach.bin',
                fileType: 'file',
                hash: 'base',
                localIdentity: 'inode',
            }]]),
            remote: new Map(),
        };

        assert.strictEqual(await provider.reconcileOne(scan), 'waiting');
        provider.remoteAbsences.set('reattach.bin', Date.now() - 1000);

        assert.strictEqual(await provider.reconcileOne(scan), 'acted');
        assert.deepStrictEqual(applied, {action: 'delete-local'});
        assert.strictEqual(provider.remoteAbsences.size, 0);
    });

    it('conflicts once a local absence holds still across the confirmation window', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: 'base',
                    text: 'base',
                },
            },
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.localDeletionCandidates = new Map();
        provider.reconcileLocalWriterOverlap = async () => 'none';
        provider.reconcileActiveBinaryConflict = async () => 'none';
        provider.scheduleReconcile = () => {};
        let applied;
        provider.applyDecision = async (_path, _baseline, _local, _remote, decision) => {
            applied = decision;
            return 'acted';
        };
        const scan = {
            local: new Map(),
            remote: new Map([['main.tex', {
                path: 'main.tex',
                entityId: 'doc-1',
                fileType: 'doc',
                hash: 'remote-next',
                text: 'remote next',
            }]]),
        };

        assert.strictEqual(await provider.reconcileOne(scan), 'waiting');
        provider.localAbsences.set('main.tex', Date.now() - 1000);

        assert.strictEqual(await provider.reconcileOne(scan), 'acted');
        assert.deepStrictEqual(applied, {
            action: 'conflict',
            reason: 'Local deletion overlaps a remote modification',
        });
        assert.strictEqual(provider.localAbsences.size, 0);
    });

    it('can retain an emptied parent after its baseline children were retained first', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                sections: {
                    entityId: 'folder-1', fileType: 'folder', hash: 'folder',
                    treeHash: 'tree-with-child',
                },
            },
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.localDeletionCandidates = new Map();
        provider.reconcileLocalWriterOverlap = async () => 'none';
        provider.reconcileActiveBinaryConflict = async () => 'none';
        provider.scheduleReconcile = () => {};
        let applied;
        provider.applyDecision = async (_path, _baseline, _local, _remote, decision) => {
            applied = decision;
            return 'acted';
        };
        const empty = contentHash(Buffer.from('[]'));
        const remote = {
            path: 'sections', entityId: 'folder-1', fileType: 'folder', hash: 'folder',
            treeHash: 'empty-content-tree', remoteStructureHash: empty,
        };
        const scan = {local: new Map(), remote: new Map([['sections', remote]])};

        assert.strictEqual(await provider.reconcileOne(scan), 'waiting');
        provider.localDeletionCandidates.get('sections').since = Date.now() - 1000;
        assert.strictEqual(await provider.reconcileOne(scan), 'acted');
        assert.deepStrictEqual(applied, {action: 'delete-remote'});
    });
});

describe('Local Replica text projection attachment', () => {
    it('discards a scan that crossed a plugin disk projection', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        let retired = false;
        provider.textReplicas = new Map([['main.tex', {
            remoteEntityId: 'doc-1',
            isIsolated: false,
        }]]);
        provider.textSchedulers = new Map();
        provider.textRetirements = new Map();
        provider.localProjectionCas = {
            observationEpoch: 8,
            hasActiveTransition: false,
        };
        provider.dropTextReplica = async () => { retired = true; };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {entityId: 'doc-1', fileType: 'doc', hash: 'base'},
            },
            conflicts: {},
        };
        const remote = snapshot('main.tex', 'doc');
        remote.entityId = 'doc-1';
        remote.text = 'base';

        assert.strictEqual(await provider.ensureTextReplicas({
            local: new Map(),
            remote: new Map([['main.tex', remote]]),
            projectionEpoch: 7,
        }), true);
        assert.strictEqual(retired, false);
        assert.strictEqual(provider.textReplicas.has('main.tex'), true);
    });

    it('freezes and retires an OT owner when a resource conflict fences its path', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const frozen = {
            path: 'main.tex',
            reason: 'remote entity changed',
            canonicalContent: 'REMOTE',
            editorContent: 'LOCAL',
            diskContent: 'BASE',
        };
        let freezeReason;
        let disposed = false;
        const machine = {
            isIsolated: false,
            freezeForConflict: async reason => {
                freezeReason = reason;
                machine.isIsolated = true;
                return frozen;
            },
            dispose: () => { disposed = true; },
            disposeAndWait: async () => {},
        };
        provider.textReplicas = new Map([['main.tex', machine]]);
        provider.textSchedulers = new Map();
        provider.textRetirements = new Map();
        provider.textConflictTasks = new Map();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.createTextConflict = async record => {
            assert.strictEqual(record, frozen);
            provider.state.conflicts[record.path] = {
                path: record.path,
                reason: record.reason,
                artifactDirectory: '.overleaf/conflicts/unresolved/frozen',
                createdAt: new Date(0).toISOString(),
            };
        };

        const document = snapshot('main.tex', 'doc');
        document.text = 'REMOTE';
        const scan = {
            local: new Map([['main.tex', document]]),
            remote: new Map([['main.tex', document]]),
        };
        await provider.createConflict('main.tex', frozen.reason, scan);
        assert.strictEqual(freezeReason, frozen.reason);
        assert(provider.state.conflicts['main.tex']);

        assert.strictEqual(await provider.ensureTextReplicas(scan), true);
        assert.strictEqual(provider.textReplicas.has('main.tex'), false);
        assert.strictEqual(disposed, true);
    });

    it('flushes an OT owner without conflict isolation when the same document id moved', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const events = [];
        const oldPath = 'sections/old.tex';
        const newPath = 'sections/new.tex';
        const entityId = 'doc-same-id';
        const machine = {
            isIsolated: false,
            remoteEntityId: entityId,
            hasBufferedInput: true,
            hasUnacknowledgedOperations: true,
            prepareForHandoff: async () => { events.push('flush-handoff'); },
            freezeForConflict: async () => {
                assert.fail('same-id move entered conflict isolation');
            },
            dispose: () => { events.push('dispose-machine'); },
            disposeAndWait: async () => { events.push('drain-machine'); },
        };
        const scheduler = {
            run: async (source, task) => {
                events.push(`schedule-${source}`);
                return task();
            },
            dispose: () => { events.push('dispose-scheduler'); },
            disposeAndWait: async () => { events.push('drain-scheduler'); },
        };
        provider.textReplicas = new Map([[oldPath, machine]]);
        provider.textSchedulers = new Map([[oldPath, scheduler]]);
        provider.textRetirements = new Map();
        provider.textConflictTasks = new Map();
        provider.resolvingPaths = new Set();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                [oldPath]: {entityId, fileType: 'doc', hash: 'base', text: 'base'},
            },
            conflicts: {},
        };
        provider.createTextConflict = async () => {
            assert.fail('same-id move created a conflict record');
        };
        const localOld = {
            path: oldPath,
            fileType: 'doc',
            hash: 'base',
            text: 'base',
        };
        const remoteMoved = {
            path: newPath,
            fileType: 'doc',
            entityId,
            hash: 'base',
            text: 'base',
        };

        assert.strictEqual(await provider.ensureTextReplicas({
            local: new Map([[oldPath, localOld]]),
            remote: new Map([[newPath, remoteMoved]]),
        }), true);

        assert.deepStrictEqual(events, [
            'schedule-retire',
            'flush-handoff',
            'dispose-machine',
            'dispose-scheduler',
            'drain-machine',
            'drain-scheduler',
        ]);
        assert.strictEqual(provider.textReplicas.has(oldPath), false);
        assert.strictEqual(provider.state.conflicts[oldPath], undefined);
    });

    it('does not attach or project a moved document at its destination before move planning', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const oldPath = 'sections/old.tex';
        const newPath = 'sections/new.tex';
        const entityId = 'doc-same-id';
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.textRetirements = new Map();
        provider.resolvingPaths = new Set();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                [oldPath]: {entityId, fileType: 'doc', hash: 'base', text: 'base'},
            },
            conflicts: {},
        };
        let projectionReads = 0;
        let replicaCreations = 0;
        provider.vfs = {
            readDocumentProjection: async () => {
                projectionReads++;
                return {entityId, revision: 1, content: 'base'};
            },
        };
        provider.createTextReplica = async () => { replicaCreations++; };
        provider.remoteUri = filePath => ({path: `/${filePath}`});
        const localOld = {
            path: oldPath,
            fileType: 'doc',
            hash: 'base',
            text: 'base',
        };
        const remoteMoved = {
            path: newPath,
            fileType: 'doc',
            entityId,
            hash: 'base',
            text: 'base',
        };

        assert.strictEqual(await provider.ensureTextReplicas({
            local: new Map([[oldPath, localOld]]),
            remote: new Map([[newPath, remoteMoved]]),
        }), false);

        assert.strictEqual(projectionReads, 0);
        assert.strictEqual(replicaCreations, 0);
        assert.strictEqual(provider.textReplicas.has(newPath), false);
        assert.strictEqual(provider.textSchedulers.has(newPath), false);
    });

    it('never attaches a successor from the scan that retired the previous owner', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        let releaseRetirement;
        const retirementGate = new Promise(resolve => { releaseRetirement = resolve; });
        const oldMachine = {
            isIsolated: false,
            remoteEntityId: 'old-doc',
            freezeForConflict: async reason => {
                await retirementGate;
                oldMachine.isIsolated = true;
                return {
                    path: 'main.tex',
                    reason,
                    canonicalContent: 'old',
                    editorContent: 'old + dirty',
                    diskContent: 'old',
                };
            },
            dispose: () => {},
            disposeAndWait: async () => {},
        };
        const oldScheduler = {
            run: async (_source, task) => task(),
            dispose: () => {},
            disposeAndWait: async () => {},
        };
        provider.textReplicas = new Map([['main.tex', oldMachine]]);
        provider.textSchedulers = new Map([['main.tex', oldScheduler]]);
        provider.textRetirements = new Map();
        provider.resolvingPaths = new Set();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {entityId: 'old-doc', fileType: 'doc', hash: 'old'},
            },
            conflicts: {},
        };
        const document = snapshot('main.tex', 'doc');
        document.text = 'replacement';
        document.entityId = 'new-doc';
        const staleScan = {
            local: new Map([['main.tex', document]]),
            remote: new Map([['main.tex', document]]),
        };
        provider.vfs = {
            readDocumentProjection: async () => ({
                entityId: 'new-doc', revision: 1, content: 'replacement',
            }),
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.createTextConflict = async record => {
            provider.state.conflicts[record.path] = {
                path: record.path,
                reason: record.reason,
                artifactDirectory: '.overleaf/conflicts/unresolved/replaced',
                createdAt: new Date(0).toISOString(),
            };
        };
        let attached = 0;
        provider.createTextReplica = async () => { attached++; };

        const firstEnsure = provider.ensureTextReplicas(staleScan);
        await new Promise(resolve => setImmediate(resolve));
        const secondEnsure = provider.ensureTextReplicas(staleScan);
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(attached, 0);

        releaseRetirement();
        assert.deepStrictEqual(await Promise.all([firstEnsure, secondEnsure]), [true, true]);
        assert.strictEqual(attached, 0);

        provider.state.entries = {};
        provider.state.conflicts = {};
        assert.strictEqual(await provider.ensureTextReplicas(staleScan), false);
        assert.strictEqual(attached, 1);
    });

    it('journals buffered editor bytes before dropping a text owner', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const commits = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'base',
            editorContent: 'base',
            diskContent: 'base',
        }, {
            commit: async (ancestor, target, barrier) => {
                commits.push({ancestor, target, barrier});
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
            readEditor: async () => 'base + dirty',
            projectEditor: async () => true,
            projectDisk: async () => true,
            isolate: async error => { throw new Error(error.reason); },
        });
        assert.strictEqual(
            machine.tryBufferEditorChange('base + dirty', 'base', true),
            true,
        );
        provider.textReplicas = new Map([['main.tex', machine]]);
        provider.textSchedulers = new Map([['main.tex', new FairTaskScheduler()]]);
        provider.textRetirements = new Map();

        await provider.dropTextReplica('main.tex');

        assert.deepStrictEqual(commits, [{
            ancestor: 'base',
            target: 'base + dirty',
            barrier: true,
        }]);
        assert.strictEqual(provider.textRetirements.size, 0);
    });

    it('archives buffered bytes without sending them when a document id is replaced', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const commits = [];
        let isolated;
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            remoteEntityId: 'old-doc',
            canonicalContent: 'base',
            editorContent: 'base',
            diskContent: 'base',
        }, {
            commit: async (ancestor, target, barrier) => {
                commits.push({ancestor, target, barrier});
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
            readEditor: async () => 'base + dirty',
            projectEditor: async () => true,
            projectDisk: async () => true,
            isolate: async record => { isolated = record; },
        });
        assert.strictEqual(machine.tryBufferEditorChange('base + dirty', 'base', true), true);
        provider.textReplicas = new Map([['main.tex', machine]]);
        provider.textSchedulers = new Map([['main.tex', new FairTaskScheduler()]]);
        provider.textRetirements = new Map();
        provider.textConflictTasks = new Map();
        provider.createTextConflict = async record => { isolated = record; };

        await provider.dropTextReplica(
            'main.tex',
            'resource-change',
            'remote id changed',
        );

        assert.deepStrictEqual(commits, []);
        assert.strictEqual(isolated.reason, 'remote id changed');
        assert.strictEqual(isolated.editorContent, 'base + dirty');
        assert.strictEqual(isolated.canonicalContent, 'base');
    });

    it('does not archive a conflict for a moved path whose local side is the baseline', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        let acknowledge;
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            remoteEntityId: 'doc',
            canonicalContent: 'base',
            editorContent: 'base',
            diskContent: 'base',
        }, {
            commit: async (_ancestor, target) => ({
                content: target,
                opId: 'op-1',
                // The server never answers: the operation stays unacknowledged,
                // exactly as it does when the transport stalls mid-move.
                acknowledgement: new Promise(resolve => { acknowledge = resolve; }),
            }),
            readEditor: async () => 'base + local',
            projectEditor: async () => true,
            projectDisk: async () => true,
            isolate: async () => {},
        });
        assert.strictEqual(
            machine.tryBufferEditorChange('base + local', 'base', true),
            true,
        );
        // Submit it: canonical adopts the operation's target while the server
        // acknowledgement stays outstanding.
        await machine.flushInput(true);
        assert.strictEqual(machine.hasBufferedInput, false);
        assert.strictEqual(machine.hasUnacknowledgedOperations, true);

        provider.textReplicas = new Map([['main.tex', machine]]);
        provider.textSchedulers = new Map([['main.tex', new FairTaskScheduler()]]);
        provider.textRetirements = new Map();
        provider.textConflictTasks = new Map();
        // The durable baseline still holds the bytes the local file has.
        provider.state = {entries: {'main.tex': {
            entityId: 'doc',
            fileType: 'doc',
            hash: 'base-hash',
            text: 'base',
        }}};
        let conflicted;
        provider.createTextConflict = async record => { conflicted = record; };

        await provider.dropTextReplica(
            'main.tex',
            'resource-change',
            'The local file for main.tex disappeared while its text actor was active',
        );

        // The local file equals the durable base, so there is no fork to
        // record. The unacknowledged operation lives in the document WAL and
        // replays when the scan reattaches the document at its new path.
        assert.strictEqual(conflicted, undefined);
        acknowledge?.('base + local');
    });

    it('does not finish a failed drop before its isolation artifact is durable', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textConflictTasks = new Map();
        let releaseArtifact;
        let artifactFinished = false;
        const artifact = new Promise(resolve => { releaseArtifact = resolve; })
            .then(() => { artifactFinished = true; })
            .finally(() => provider.textConflictTasks.delete('main.tex'));
        const machine = {
            isIsolated: false,
            prepareForHandoff: async () => {
                provider.textConflictTasks.set('main.tex', artifact);
                throw new Error('journal transition failed');
            },
            dispose: () => {},
            disposeAndWait: async () => {},
        };
        const scheduler = {
            run: async (_source, task) => task(),
            dispose: () => {},
            disposeAndWait: async () => {},
        };
        provider.textReplicas = new Map([['main.tex', machine]]);
        provider.textSchedulers = new Map([['main.tex', scheduler]]);
        provider.textRetirements = new Map();

        const retirement = provider.dropTextReplica('main.tex');
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(artifactFinished, false);
        assert.strictEqual(provider.textRetirements.has('main.tex'), true);

        releaseArtifact();
        await assert.rejects(retirement, /journal transition failed/);
        assert.strictEqual(artifactFinished, true);
        assert.strictEqual(provider.textRetirements.size, 0);
    });

    it('scans an isolated document without replaying its failed journal', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {
                'main.tex': {
                    path: 'main.tex',
                    reason: 'failed OT journal',
                    artifactDirectory: '.overleaf/conflicts/unresolved/test',
                    createdAt: new Date(0).toISOString(),
                },
            },
        };
        let normalReads = 0;
        let recoveryFreeReads = 0;
        provider.vfs = {
            walk: () => [{
                entity: {_id: 'doc-id', _type: 'doc', readonly: false},
                path: 'main.tex',
            }],
            readAuthoritativeFile: async () => {
                normalReads++;
                throw new Error('failed journal was replayed');
            },
            readAuthoritativeFileWithoutRecovery: async () => {
                recoveryFreeReads++;
                return {
                    entityId: 'doc-id',
                    fileType: 'doc',
                    revision: 4,
                    content: new TextEncoder().encode('remote'),
                };
            },
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.isIgnored = () => false;
        provider.assertSafePath = () => {};

        const scan = await provider.scanRemote();

        assert.strictEqual(normalReads, 0);
        assert.strictEqual(recoveryFreeReads, 1);
        assert.strictEqual(scan.get('main.tex').text, 'remote');
    });

    it('clears a local-only resolution journal immediately after a stale local preflight', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const oldBytes = new TextEncoder().encode('old');
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        const saves = [];
        provider.stateStore = {
            save: async state => { saves.push(structuredClone(state)); },
        };
        provider.localUri = pathname => ({path: pathname});
        provider.readCurrentLocalBytes = async () => new TextEncoder().encode('new');
        const local = {
            path: 'main.tex',
            fileType: 'doc',
            hash: contentHash(oldBytes),
            text: 'old',
        };

        await assert.rejects(
            () => provider.journalAction(
                'resolve-conflict',
                'main.tex',
                local,
                local,
                local.hash,
                () => provider.deleteLocalSnapshot('main.tex', local),
            ),
            /Local content changed before deletion/,
        );

        assert.strictEqual(provider.state.pendingAction, undefined);
        assert.strictEqual(saves.at(-1).pendingAction, undefined);
    });

    it('retires an isolated text machine before restoring a conflict choice', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const events = [];
        const record = {
            path: 'main.tex',
            reason: 'test isolation',
            artifactDirectory: '.overleaf/conflicts/unresolved/test',
            createdAt: new Date(0).toISOString(),
        };
        provider.disposed = false;
        provider.lockHeld = true;
        provider.pausedError = undefined;
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {'main.tex': record},
            projectionReceipts: [
                {id: 'editor-main', path: 'editor:main.tex', kind: 'change', expectedHash: 'old'},
                {id: 'disk-main', path: 'disk:main.tex', kind: 'change', expectedHash: 'old'},
                {id: 'other', path: 'editor:other.tex', kind: 'change', expectedHash: 'keep'},
            ],
        };
        let isolation;
        const oldMachine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'base',
            editorContent: 'base',
            diskContent: 'base',
        }, {
            commit: async (_ancestor, target) => ({
                content: target,
                acknowledgement: Promise.resolve(target),
            }),
            readEditor: async () => 'base',
            projectEditor: async () => {
                throw new Error('genuine projection failure');
            },
            projectDisk: async () => {},
            isolate: async isolated => { isolation = isolated; },
        });
        await assert.rejects(
            () => oldMachine.onCanonicalChanged('remote'),
            /genuine projection failure/,
        );
        assert.strictEqual(oldMachine.isIsolated, true);
        assert.strictEqual(isolation.editorContent, 'base');
        const disposeOldMachine = oldMachine.disposeAndWait.bind(oldMachine);
        oldMachine.disposeAndWait = async () => {
            events.push('retire-machine');
            await disposeOldMachine();
        };
        const oldScheduler = {
            run: async (_source, task) => task(),
            dispose: () => {},
            disposeAndWait: async () => { events.push('retire-scheduler'); },
        };
        provider.textReplicas = new Map([['main.tex', oldMachine]]);
        provider.textSchedulers = new Map([['main.tex', oldScheduler]]);
        provider.textRetirements = new Map();
        provider.textConflictTasks = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.resolvingPaths = new Set();
        provider.recoverConflictArtifactLocation = async () => {};
        provider.vfs = {
            prepareDocumentRecoveryById: async entityId => ({
                token: 'recovery-token',
                documentId: entityId,
                visibleContent: 'remote',
                acknowledgedContent: 'remote',
                revision: 1,
                pendingOperationIds: [],
                epoch: 0,
                hash: 'recovery-hash',
            }),
            commitDiscardDocumentRecovery: async () => { events.push('discard-journal'); },
            inspectPendingDocumentRecoveries: async () => [],
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        const main = snapshot('main.tex', 'doc');
        main.text = 'remote';
        main.content = new TextEncoder().encode(main.text);
        main.hash = contentHash(main.content);
        main.localIdentity = '1:2';
        const scan = {
            local: new Map([['main.tex', main]]),
            remote: new Map([['main.tex', main]]),
        };
        provider.scanBoth = async () => {
            events.push('scan');
            return scan;
        };
        provider.journalAction = async (_kind, _path, _local, _remote, _hash, action) => {
            events.push('journal');
            await action();
        };
        provider.restoreRemotePath = async () => { events.push('restore'); };
        provider.isSubtreeConverged = () => true;
        provider.stateStore = {
            save: async () => { events.push('save'); },
            saveBaseline: async content => {
                assert(provider.state.conflicts['main.tex'],
                    'Conflict became externally invisible during baseline refresh');
                events.push('baseline');
                return contentHash(content);
            },
        };
        provider.archiveResolutionCandidates = async () => { events.push('candidate-archive'); };
        provider.writeConflictManifest = async () => { events.push('recovery-manifest'); };
        provider.moveConflictToResolved = async () => {
            events.push('archive');
            return '.overleaf/conflicts/resolved/test';
        };
        provider.scheduleReconcile = () => { events.push('reattach'); };

        await provider.resolveConflictRecord('main.tex', 'remote');

        assert.strictEqual(events[0], 'retire-machine');
        assert(events.indexOf('retire-machine') < events.indexOf('restore'));
        assert(events.indexOf('retire-scheduler') < events.indexOf('restore'));
        assert(events.indexOf('scan') < events.indexOf('candidate-archive'));
        assert(events.indexOf('candidate-archive') < events.indexOf('discard-journal'));
        assert.strictEqual(provider.textReplicas.has('main.tex'), false);
        assert.strictEqual(provider.textSchedulers.has('main.tex'), false);
        assert.strictEqual(provider.state.conflicts['main.tex'], undefined);
        assert.deepStrictEqual(
            provider.state.projectionReceipts.map(receipt => receipt.id),
            ['other'],
        );
        assert.strictEqual(events.at(-1), 'reattach');

        const freshMachine = {isIsolated: false};
        provider.vfs = {
            readDocumentProjection: async () => ({
                entityId: main.entityId,
                revision: 2,
                content: 'remote',
            }),
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.createTextReplica = async () => {
            provider.textReplicas.set('main.tex', freshMachine);
        };
        provider.resolvingPaths.add('main.tex');
        await provider.ensureTextReplicas(scan);
        assert.strictEqual(provider.textReplicas.has('main.tex'), false);
        provider.resolvingPaths.delete('main.tex');
        await provider.ensureTextReplicas(scan);
        assert.strictEqual(provider.textReplicas.get('main.tex'), freshMachine);
        assert.notStrictEqual(provider.textReplicas.get('main.tex'), oldMachine);
    });

    it('does not attach after a conflict fence begins during an async projection read', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.textRetirements = new Map();
        provider.textConflictTasks = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.resolvingPaths = new Set();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        let releaseProjection;
        provider.vfs = {
            readDocumentProjection: () => new Promise(resolve => {
                releaseProjection = resolve;
            }),
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        let attached = false;
        provider.createTextReplica = async () => { attached = true; };
        const document = snapshot('main.tex', 'doc');
        document.text = 'remote';
        const scan = {
            local: new Map([['main.tex', document]]),
            remote: new Map([['main.tex', document]]),
        };

        const ensuring = provider.ensureTextReplicas(scan);
        await new Promise(resolve => setImmediate(resolve));
        const scheduler = provider.textSchedulers.get('main.tex');
        assert(scheduler, 'Attach must own a scheduler before reading the projection');
        provider.resolvingPaths.add('main.tex');
        provider.textSchedulers.delete('main.tex');
        const retirement = scheduler.disposeAndWait();
        releaseProjection({entityId: document.entityId, revision: 1, content: 'remote'});

        await Promise.all([ensuring, retirement]);
        assert.strictEqual(attached, false);
        assert.strictEqual(provider.textReplicas.has('main.tex'), false);
    });

    it('serializes initial attachment ahead of a watcher task for the same document', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.resolvingPaths = new Set();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.vfs = {
            readDocumentProjection: async () => ({
                entityId: 'main.tex-id', revision: 1, content: 'remote',
            }),
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        const order = [];
        let releaseAttach;
        const attachmentStarted = new Promise(resolve => {
            provider.createTextReplica = async () => {
                order.push('attach:start');
                resolve();
                await new Promise(unblock => { releaseAttach = unblock; });
                order.push('attach:end');
            };
        });
        const document = snapshot('main.tex', 'doc');
        document.text = 'remote';
        const scan = {
            local: new Map([['main.tex', document]]),
            remote: new Map([['main.tex', document]]),
        };

        const ensuring = provider.ensureTextReplicas(scan);
        await attachmentStarted;
        const watcher = provider.textSchedulers.get('main.tex').run('filesystem', async () => {
            order.push('watcher');
        });
        releaseAttach();
        await Promise.all([ensuring, watcher]);

        assert.deepStrictEqual(order, ['attach:start', 'attach:end', 'watcher']);
    });

    it('restores an archived local choice with ownership of the canonical targets', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const record = {
            path: 'main.tex',
            reason: 'test isolation',
            artifactDirectory: '.overleaf/conflicts/unresolved/test-local',
            localFile: '.overleaf/conflicts/unresolved/test-local/local/main.tex',
            createdAt: new Date(0).toISOString(),
        };
        provider.disposed = false;
        provider.lockHeld = true;
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {'main.tex': record},
        };
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.textRetirements = new Map();
        provider.textConflictTasks = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.resolvingPaths = new Set();
        provider.recoverConflictArtifactLocation = async () => {};
        provider.vfs = {
            prepareDocumentRecoveryById: async () => {
                throw new Error('No durable document recovery state exists for test');
            },
            commitDiscardDocumentRecovery: async () => {},
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        const main = snapshot('main.tex', 'doc');
        main.text = 'remote';
        const scan = {
            local: new Map([['main.tex', main]]),
            remote: new Map([['main.tex', main]]),
        };
        let scanCount = 0;
        provider.scanBoth = async () => {
            scanCount++;
            return scan;
        };
        provider.journalAction = async (_kind, _path, _local, _remote, _hash, action) => action();
        provider.restoreRemotePath = async () => {};
        provider.isSubtreeConverged = () => true;
        provider.refreshConvergedState = async () => {};
        provider.stateStore = {save: async () => {}};
        const latestLocalCandidate = `${record.artifactDirectory}/resolution-latest/main.tex`;
        provider.archiveResolutionCandidates = async current => {
            current.localFile = latestLocalCandidate;
            current.localHash = main.hash;
        };
        let releaseRevision;
        const revision = new Promise(resolve => { releaseRevision = resolve; })
            .finally(() => provider.isolatedRevisionTasks.delete('main.tex'));
        provider.isolatedRevisionTasks.set('main.tex', revision);
        let expectedTargets;
        let restoredSource;
        provider.copyLocalTree = async (source, _target, expected) => {
            restoredSource = source;
            expectedTargets = expected;
        };
        provider.convergeArchivedLocalResolution = async () => {};
        provider.moveConflictToResolved = async () => '.overleaf/conflicts/resolved/test-local';
        provider.scheduleReconcile = () => {};

        const resolution = provider.resolveConflictRecord('main.tex', 'local');
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(scanCount, 0, 'resolution passed an in-flight isolated revision');
        releaseRevision();
        await resolution;

        assert.strictEqual(expectedTargets, scan.local);
        assert.strictEqual(restoredSource, latestLocalCandidate);
        assert.strictEqual(provider.state.conflicts['main.tex'], undefined);
    });

    it('recovers a conflict record after its artifact was renamed before state commit', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const unresolved = '.overleaf/conflicts/unresolved/crash';
        const resolved = '.overleaf/conflicts/resolved/crash';
        const record = {
            path: 'main.tex',
            reason: 'test crash',
            artifactDirectory: unresolved,
            baseFile: `${unresolved}/base/main.tex`,
            localFile: `${unresolved}/local-editor/main.tex`,
            remoteFile: `${unresolved}/remote/main.tex`,
            retainedRemoteFile: `${unresolved}/retained/main.tex`,
            documentRecoveries: [{
                entityId: 'doc-1',
                visibleFile: `${unresolved}/wal/visible/main.tex`,
                acknowledgedFile: `${unresolved}/wal/ack/main.tex`,
                visibleHash: 'visible',
                acknowledgedHash: 'ack',
                revision: 1,
                pendingOperationIds: ['op-1'],
                epoch: 1,
                snapshotHash: 'snapshot',
            }],
            createdAt: new Date(0).toISOString(),
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {'main.tex': record},
        };
        provider.localArtifactExists = async filePath => filePath === resolved;
        let saved;
        let manifest;
        provider.stateStore = {
            save: async state => { saved = structuredClone(state); },
        };
        provider.writeConflictManifest = async current => {
            manifest = structuredClone(current);
        };

        await provider.recoverConflictArtifactLocation(record);

        assert.strictEqual(record.artifactDirectory, resolved);
        assert.strictEqual(record.baseFile, `${resolved}/base/main.tex`);
        assert.strictEqual(record.localFile, `${resolved}/local-editor/main.tex`);
        assert.strictEqual(record.remoteFile, `${resolved}/remote/main.tex`);
        assert.strictEqual(record.retainedRemoteFile, `${resolved}/retained/main.tex`);
        assert.strictEqual(record.documentRecoveries[0].visibleFile,
            `${resolved}/wal/visible/main.tex`);
        assert.strictEqual(record.documentRecoveries[0].acknowledgedFile,
            `${resolved}/wal/ack/main.tex`);
        assert.strictEqual(manifest.artifactDirectory, resolved);
        assert.strictEqual(saved.conflicts['main.tex'].artifactDirectory, resolved);
    });

    it('automatically retires a non-divergent editor-observation conflict', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const hash = 'same-hash';
        const record = {
            path: 'main.tex',
            reason: 'Cannot prove the event-time ancestor of a VS Code edit',
            artifactDirectory: '.overleaf/conflicts/unresolved/no-op',
            createdAt: new Date(0).toISOString(),
            baseHash: hash,
            localHash: hash,
            remoteHash: hash,
            baselineEntityId: 'doc-1',
            remoteEntityId: 'doc-1',
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {'main.tex': record},
            projectionReceipts: [
                {id: 'main', path: 'editor:main.tex', kind: 'change', expectedHash: hash},
                {id: 'other', path: 'editor:other.tex', kind: 'change', expectedHash: 'other'},
            ],
        };
        const events = [];
        provider.textReplicas = new Map([['main.tex', {
            isIsolated: true,
            dispose: () => {},
            disposeAndWait: async () => { events.push('retire-machine'); },
        }]]);
        provider.textSchedulers = new Map([['main.tex', {
            run: async (_source, task) => task(),
            dispose: () => {},
            disposeAndWait: async () => { events.push('retire-scheduler'); },
        }]]);
        provider.textRetirements = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.recoverConflictArtifactLocation = async () => { events.push('recover-artifact'); };
        provider.moveConflictToResolved = async () => {
            events.push('archive');
            return '.overleaf/conflicts/resolved/no-op';
        };
        provider.stateStore = {save: async () => { events.push('save'); }};
        provider.output = {appendLine: message => events.push(message)};

        assert.strictEqual(
            await provider.recoverNonDivergentEditorObservationConflict(),
            true,
        );
        assert.strictEqual(provider.state.conflicts['main.tex'], undefined);
        assert.strictEqual(provider.textReplicas.has('main.tex'), false);
        assert.strictEqual(provider.textSchedulers.has('main.tex'), false);
        assert.deepStrictEqual(
            provider.state.projectionReceipts.map(receipt => receipt.id),
            ['other'],
        );
        assert.deepStrictEqual(events.slice(0, 5), [
            'recover-artifact',
            'retire-machine',
            'retire-scheduler',
            'archive',
            'save',
        ]);
        assert.match(events.at(-1), /auto-recovered/);
    });

    it('keeps a conflict when an isolated revision lands during auto-recovery', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const hash = 'same-hash';
        const record = {
            path: 'main.tex',
            reason: 'Cannot prove the event-time ancestor of a VS Code edit',
            artifactDirectory: '.overleaf/conflicts/unresolved/revision-race',
            createdAt: new Date(0).toISOString(),
            baseHash: hash,
            localHash: hash,
            remoteHash: hash,
            baselineEntityId: 'doc-1',
            remoteEntityId: 'doc-1',
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {'main.tex': record},
        };
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.textRetirements = new Map();
        provider.isolatedRevisionTasks = new Map();
        provider.recoverConflictArtifactLocation = async () => {};
        let releaseRevision;
        const revision = new Promise(resolve => { releaseRevision = resolve; })
            .then(() => { record.localHash = 'new-local-revision'; })
            .finally(() => provider.isolatedRevisionTasks.delete('main.tex'));
        provider.isolatedRevisionTasks.set('main.tex', revision);
        provider.moveConflictToResolved = async () => {
            assert.fail('a divergent revision was auto-resolved');
        };

        const recovery = provider.recoverNonDivergentEditorObservationConflict();
        await new Promise(resolve => setImmediate(resolve));
        releaseRevision();

        assert.strictEqual(await recovery, false);
        assert.strictEqual(provider.state.conflicts['main.tex'], record);
    });

    it('defers attachment until proved editor and disk variants are locally merged', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.vfs = {
            readDocumentProjection: async () => {
                throw new Error('variant merge must run before document attachment');
            },
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        const scan = {
            local: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                hash: 'editor',
                text: 'editor',
                localVariants: {
                    diskContent: new TextEncoder().encode('disk'),
                    editorContent: new TextEncoder().encode('editor'),
                    diskHash: 'disk',
                    editorHash: 'editor',
                    mergedContent: new TextEncoder().encode('merged'),
                },
            }]]),
            remote: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: 'base',
                text: 'base',
            }]]),
        };

        await provider.ensureTextReplicas(scan);

        assert.strictEqual(provider.textReplicas.size, 0);
    });

    it('retries an attach race instead of isolating a newly observed editor/disk fork', async () => {
        const vscode = {
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            window: {
                createOutputChannel: () => ({appendLine() {}, clear() {}, dispose() {}}),
            },
            workspace: {
                textDocuments: [{
                    uri: {fsPath: '/replica/main.tex'},
                    isDirty: true,
                    getText: () => 'EDITOR\nBASE\n',
                }],
            },
        };
        const LocalReplicaSCMProvider = loadProvider(vscode);
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(new TextEncoder().encode('BASE\n')),
                    text: 'BASE\n',
                },
            },
            conflicts: {},
        };
        provider.localUri = pathname => ({fsPath: `/replica/${pathname}`});
        provider.readLocalText = async () => 'BASE\nDISK\n';

        await assert.rejects(
            () => provider.createTextReplica('main.tex', 'BASE\n', {
                path: 'main.tex',
                fileType: 'doc',
                hash: contentHash(new TextEncoder().encode('BASE\n')),
                text: 'BASE\n',
            }),
            /changed while attaching main\.tex/,
        );
        assert.strictEqual(provider.textReplicas.size, 0);
        assert.strictEqual(Object.keys(provider.state.conflicts).length, 0);
    });

    it('retries when a clean disk writer advances after the attachment scan', async () => {
        const vscode = {
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            window: {
                createOutputChannel: () => ({appendLine() {}, clear() {}, dispose() {}}),
            },
            workspace: {textDocuments: []},
        };
        const LocalReplicaSCMProvider = loadProvider(vscode);
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(new TextEncoder().encode('BASE\n')),
                    text: 'BASE\n',
                },
            },
            conflicts: {},
        };
        provider.localUri = pathname => ({fsPath: `/replica/${pathname}`});
        provider.readLocalText = async () => 'NEW DISK\nBASE\n';

        await assert.rejects(
            () => provider.createTextReplica('main.tex', 'REMOTE\nBASE\n', {
                path: 'main.tex',
                entityId: 'doc-1',
                fileType: 'doc',
                hash: contentHash(new TextEncoder().encode('BASE\n')),
                text: 'BASE\n',
            }),
            /changed while attaching main\.tex/,
        );
        assert.strictEqual(provider.textReplicas.size, 0);
        assert.strictEqual(Object.keys(provider.state.conflicts).length, 0);
    });

    it('OT-rebases an external disk edit over remote changes from a common base', async () => {
        let disk = 'LOCAL\nBASE\n';
        const vscode = {
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            window: {
                createOutputChannel: () => ({appendLine() {}, clear() {}, dispose() {}}),
                visibleTextEditors: [],
            },
            workspace: {
                textDocuments: [],
                fs: {
                    writeFile: async (_uri, bytes) => {
                        disk = new TextDecoder().decode(bytes);
                    },
                },
            },
        };
        const LocalReplicaSCMProvider = loadProvider(vscode);
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.resolvingPaths = new Set();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(new TextEncoder().encode('BASE\n')),
                    text: 'BASE\n',
                },
            },
            conflicts: {},
        };
        provider.localUri = pathname => ({fsPath: `/replica/${pathname}`});
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.readLocalText = async () => disk;
        provider.ensureLocalParent = async () => {};
        provider.advanceTextBaselineIfAuthoritative = async () => {};
        provider.stateStore = {save: async () => {}};
        provider.pendingEditorObservations = new Map();
        provider.editorObservationDrains = new Map();
        provider.localProjectionCas = {
            replace: async (_path, _expectedHash, content) => {
                disk = new TextDecoder().decode(content);
                return {applied: true, actual: content};
            },
        };
        const commits = [];
        provider.vfs = {
            acceptDocumentSnapshotAgainstAncestor: async (_uri, ancestor, target, barrier) => {
                commits.push({ancestor, target, barrier});
                return {
                    content: 'REMOTE\nLOCAL\nBASE\n',
                    acknowledgement: Promise.resolve('REMOTE\nLOCAL\nBASE\n'),
                };
            },
        };
        const conflicts = [];
        provider.createTextConflict = async record => { conflicts.push(record); };

        await provider.createTextReplica('main.tex', 'REMOTE\nBASE\n', {
            path: 'main.tex',
            entityId: 'doc-1',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode('LOCAL\nBASE\n')),
            text: 'LOCAL\nBASE\n',
        });

        assert.deepStrictEqual(commits, [{
            ancestor: 'BASE\n',
            target: 'LOCAL\nBASE\n',
            barrier: false,
        }]);
        assert.strictEqual(provider.textReplicas.get('main.tex').canonical,
            'REMOTE\nLOCAL\nBASE\n');
        assert.strictEqual(disk, 'REMOTE\nLOCAL\nBASE\n');
        assert.deepStrictEqual(conflicts, []);
    });

    it('adopts a journal-recovered editor and disk projection without resubmitting it', async () => {
        const base = 'BASE\n';
        const recovered = 'BASE\nJOURNALED\n';
        const vscode = {
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            window: {
                createOutputChannel: () => ({appendLine() {}, clear() {}, dispose() {}}),
                visibleTextEditors: [],
            },
            workspace: {
                textDocuments: [{
                    uri: {fsPath: '/replica/main.tex'},
                    isDirty: true,
                    getText: () => recovered,
                }],
            },
        };
        const LocalReplicaSCMProvider = loadProvider(vscode);
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.textRetirements = new Map();
        provider.resolvingPaths = new Set();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(new TextEncoder().encode(base)),
                    text: base,
                },
            },
            conflicts: {},
        };
        provider.localUri = pathname => ({fsPath: `/replica/${pathname}`});
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.readLocalText = async () => recovered;
        provider.stateStore = {save: async () => {}};
        provider.localProjectionCas = {
            replace: async () => {
                assert.fail('an already-recovered disk projection was rewritten');
            },
        };
        provider.vfs = {
            acceptDocumentSnapshotAgainstAncestor: async () => {
                assert.fail('journal-recovered editor bytes were submitted twice');
            },
        };
        let converged;
        provider.advanceTextBaselineIfAuthoritative = async (_path, content) => {
            converged = content;
        };
        provider.createTextConflict = async () => {
            assert.fail('a converged recovery projection was isolated');
        };
        const local = {
            path: 'main.tex',
            entityId: 'doc-1',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode(recovered)),
            text: recovered,
        };

        await provider.createTextReplica(
            'main.tex',
            recovered,
            local,
            undefined,
            'doc-1',
        );

        assert.strictEqual(provider.textReplicas.get('main.tex').isSettled, true);
        assert.strictEqual(converged, recovered);
    });

    it('advances an acknowledged OT projection before a following local delete', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const base = 'BASE\n';
        const merged = 'REMOTE\nLOCAL\nBASE\n';
        const encoder = new TextEncoder();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(encoder.encode(base)),
                    text: base,
                    localIdentity: '1:2',
                },
            },
            conflicts: {},
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.localUri = pathname => ({fsPath: `/replica/${pathname}`});
        provider.vfs = {
            readAuthoritativeFile: async () => ({
                entityId: 'doc-1',
                fileType: 'doc',
                content: encoder.encode(merged),
                revision: 3,
            }),
        };
        provider.readLocalText = async () => merged;
        provider.localIdentity = async () => '1:2';
        provider.stateStore = {
            saveBaseline: async () => assert.fail(
                'document text is already retained inline and must not create a baseline blob',
            ),
            save: async () => {},
        };

        await provider.advanceTextBaselineIfAuthoritative('main.tex', merged);

        const baseline = provider.state.entries['main.tex'];
        assert.strictEqual(baseline.text, merged);
        assert.deepStrictEqual(
            decideReconciliation(
                baseline,
                undefined,
                {
                    path: 'main.tex',
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(encoder.encode(merged)),
                    text: merged,
                },
            ),
            {action: 'delete-remote'},
        );
    });

    it('does not roll a newer baseline back with an older converged scan', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const stale = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode('')),
            text: '',
            localIdentity: '1:1',
        };
        const current = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode('COMMON BASE\n')),
            text: 'COMMON BASE\n',
            localIdentity: '1:1',
        };
        const unrelatedBefore = {
            entityId: 'doc-2',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode('OTHER OLD\n')),
            text: 'OTHER OLD\n',
            localIdentity: '2:2',
        };
        const unrelatedAfter = {
            path: 'other.tex',
            entityId: 'doc-2',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode('OTHER NEW\n')),
            text: 'OTHER NEW\n',
            localIdentity: '2:2',
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': current,
                'other.tex': unrelatedBefore,
            },
            conflicts: {},
        };
        provider.vfs = {inspectPendingDocumentRecoveries: async () => []};
        provider.stateStore = {
            saveBaseline: async () => assert.fail('document baselines stay inline'),
        };
        const staleSnapshot = {
            path: 'main.tex',
            fileType: 'doc',
            entityId: 'doc-1',
            hash: stale.hash,
            text: '',
            localIdentity: '1:1',
        };

        await provider.refreshConvergedState(provider.state, {
            local: new Map([
                ['main.tex', staleSnapshot],
                ['other.tex', unrelatedAfter],
            ]),
            remote: new Map([
                ['main.tex', staleSnapshot],
                ['other.tex', unrelatedAfter],
            ]),
            baselineAtStart: new Map([
                ['main.tex', stale],
                ['other.tex', unrelatedBefore],
            ]),
        });

        assert.deepStrictEqual(provider.state.entries['main.tex'], current);
        assert.strictEqual(provider.state.entries['other.tex'].text, 'OTHER NEW\n');
    });

    it('creates a replica from the journal-visible projection, not the older remote scan', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.textReplicas = new Map();
        provider.textSchedulers = new Map();
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.vfs = {
            readDocumentProjection: async () => ({
                entityId: 'doc-1',
                revision: 8,
                content: 'visible journaled text',
            }),
        };
        let attachedCanonical;
        provider.createTextReplica = async (_filePath, canonicalContent) => {
            attachedCanonical = canonicalContent;
        };
        const scan = {
            local: new Map([['main.tex', {
                path: 'main.tex', fileType: 'doc', hash: 'local', text: 'local disk',
            }]]),
            remote: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: 'acknowledged',
                text: 'acknowledged remote text',
            }]]),
        };

        await provider.ensureTextReplicas(scan);

        assert.strictEqual(attachedCanonical, 'visible journaled text');
        assert.strictEqual(provider.currentActionPath, undefined);
    });

    it('refreshes an existing replica from the journal-visible projection', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        let refreshedCanonical;
        provider.textReplicas = new Map([['main.tex', {
            isIsolated: false,
            canonical: 'old canonical',
            onCanonicalChanged: async content => { refreshedCanonical = content; },
        }]]);
        provider.textSchedulers = new Map();
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.vfs = {
            hasLiveDocumentProjection: () => false,
            readDocumentProjection: async () => ({
                entityId: 'doc-1',
                revision: 8,
                content: 'visible journaled text',
            }),
        };
        const scan = {
            local: new Map([['main.tex', {
                path: 'main.tex', fileType: 'doc', hash: 'local', text: 'local disk',
            }]]),
            remote: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: 'acknowledged',
                text: 'acknowledged remote text',
            }]]),
        };

        await provider.ensureTextReplicas(scan);

        assert.strictEqual(refreshedCanonical, 'visible journaled text');
        assert.strictEqual(provider.currentActionPath, undefined);
    });

    it('does not let a scan become a second canonical source for a live OT session', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        let canonicalChanges = 0;
        provider.textReplicas = new Map([['main.tex', {
            remoteEntityId: 'doc-1',
            isIsolated: false,
            canonical: 'latest ordered projection',
            onCanonicalChanged: async () => { canonicalChanges += 1; },
        }]]);
        provider.textSchedulers = new Map();
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.vfs = {
            hasLiveDocumentProjection: () => true,
            readDocumentProjection: async () => {
                assert.fail('a live OT session was reread by the reconciliation scan');
            },
        };
        const scan = {
            local: new Map([['main.tex', {
                path: 'main.tex', fileType: 'doc', hash: 'local', text: 'local disk',
            }]]),
            remote: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: 'acknowledged',
                text: 'older acknowledged remote text',
            }]]),
        };

        await provider.ensureTextReplicas(scan);

        assert.strictEqual(canonicalChanges, 0);
        assert.strictEqual(provider.currentActionPath, undefined);
    });

    it('does not inject a scan snapshot after opening a new direct projection source', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        let live = false;
        let canonicalChanges = 0;
        provider.textReplicas = new Map([['main.tex', {
            remoteEntityId: 'doc-1',
            isIsolated: false,
            canonical: 'old canonical',
            onCanonicalChanged: async () => { canonicalChanges += 1; },
        }]]);
        provider.textSchedulers = new Map();
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.vfs = {
            hasLiveDocumentProjection: () => live,
            readDocumentProjection: async () => {
                live = true;
                return {
                    entityId: 'doc-1',
                    revision: 8,
                    content: 'published by session onChange',
                };
            },
        };
        const scan = {
            local: new Map([['main.tex', {
                path: 'main.tex', fileType: 'doc', hash: 'local', text: 'local disk',
            }]]),
            remote: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: 'acknowledged',
                text: 'acknowledged remote text',
            }]]),
        };

        await provider.ensureTextReplicas(scan);

        assert.strictEqual(canonicalChanges, 0);
        assert.strictEqual(provider.currentActionPath, undefined);
    });

    it('reads an existing replica projection inside its document queue', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        let refreshedCanonical;
        provider.textReplicas = new Map([['main.tex', {
            isIsolated: false,
            canonical: 'base',
            onCanonicalChanged: async content => { refreshedCanonical = content; },
        }]]);
        provider.textSchedulers = new Map();
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        let visibleProjection = 'captured-before-queue';
        provider.vfs = {
            hasLiveDocumentProjection: () => false,
            readDocumentProjection: async () => ({
                entityId: 'doc-1',
                revision: visibleProjection === 'captured-before-queue' ? 8 : 9,
                content: visibleProjection,
            }),
        };
        provider.textTask = async (_path, _source, task) => {
            visibleProjection = 'journal-visible-inside-queue';
            return task();
        };
        const scan = {
            local: new Map([['main.tex', {
                path: 'main.tex', fileType: 'doc', hash: 'local', text: 'local disk',
            }]]),
            remote: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: 'acknowledged',
                text: 'acknowledged remote text',
            }]]),
        };

        await provider.ensureTextReplicas(scan);

        assert.strictEqual(refreshedCanonical, 'journal-visible-inside-queue');
        assert.strictEqual(provider.currentActionPath, undefined);
    });
});

describe('Local Replica move CAS', () => {
    function pendingMoveCompletionProvider(LocalReplicaSCMProvider, state, scan) {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.disposed = false;
        provider.state = state;
        provider.scanBoth = async () => scan;
        provider.clearLocalDeletionCandidates = () => {};
        provider.renderReport = () => {};
        provider.recoverNonDivergentEditorObservationConflict = async () => false;
        provider.isolateChangedRetainedPendingCandidate = async () => false;
        provider.vfs = {
            safeSyncCapability: {writable: false, reason: 'stop after recovery'},
            inspectPendingDocumentRecoveries: async () => [],
        };
        provider.stateStore = {
            save: async () => {},
            saveBaseline: async () => assert.fail('divergent endpoints replaced their common base'),
        };
        return provider;
    }

    it('relocates the old file baseline after either move direction despite concurrent edits', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        for (const kind of ['move-local', 'move-remote']) {
            const oldPath = `${kind}-old.tex`;
            const newPath = `${kind}-new.tex`;
            const baseline = {
                entityId: `doc-${kind}`,
                fileType: 'doc',
                hash: 'common-base-hash',
                text: 'common base',
                localIdentity: `inode-${kind}`,
            };
            const state = {
                schemaVersion: 1,
                replicaId: 'replica',
                initialized: true,
                updatedAt: new Date(0).toISOString(),
                entries: {[oldPath]: baseline},
                conflicts: {},
                pendingAction: {
                    id: `pending-${kind}`,
                    kind,
                    path: oldPath,
                    targetPath: newPath,
                    startedAt: new Date(0).toISOString(),
                    expectedLocalHash: 'common-base-hash',
                    expectedLocalIdentity: `inode-${kind}`,
                    expectedRemoteHash: 'common-base-hash',
                    targetHash: 'common-base-hash',
                    entityId: `doc-${kind}`,
                },
            };
            const scan = {
                local: new Map([[newPath, {
                    path: newPath,
                    fileType: 'doc',
                    hash: 'concurrent-local-edit',
                    text: 'common base + local edit',
                    localIdentity: `inode-${kind}`,
                }]]),
                remote: new Map([[newPath, {
                    path: newPath,
                    fileType: 'doc',
                    entityId: `doc-${kind}`,
                    hash: 'concurrent-remote-edit',
                    text: 'common base + remote edit',
                }]]),
            };
            const provider = pendingMoveCompletionProvider(
                LocalReplicaSCMProvider,
                state,
                scan,
            );

            await provider.reconcileUntilStable(`recover ${kind}`);

            assert.strictEqual(state.entries[oldPath], undefined, kind);
            assert.deepStrictEqual(state.entries[newPath], baseline, kind);
            assert.strictEqual(state.entries[newPath].hash, 'common-base-hash', kind);
            assert.strictEqual(state.entries[newPath].text, 'common base', kind);
            assert.strictEqual(state.pendingAction, undefined, kind);
        }
    });

    it('relocates every folder descendant baseline even when child hashes diverged', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const oldPath = 'chapters';
        const newPath = 'moved-chapters';
        const originalEntries = {
            chapters: {
                entityId: 'folder-root', fileType: 'folder', hash: 'folder',
                treeHash: 'common-root-tree', localIdentity: 'inode-root',
            },
            'chapters/main.tex': {
                entityId: 'doc-main', fileType: 'doc', hash: 'common-doc',
                text: 'common doc', localIdentity: 'inode-doc',
            },
            'chapters/images': {
                entityId: 'folder-images', fileType: 'folder', hash: 'folder',
                treeHash: 'common-images-tree', localIdentity: 'inode-images',
            },
            'chapters/images/plot.png': {
                entityId: 'file-plot', fileType: 'file', hash: 'common-binary',
                localIdentity: 'inode-binary',
            },
        };
        const expectedRelocated = structuredClone(originalEntries);
        const state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: originalEntries,
            conflicts: {},
            pendingAction: {
                id: 'pending-folder-move',
                kind: 'move-local',
                path: oldPath,
                targetPath: newPath,
                startedAt: new Date(0).toISOString(),
                expectedLocalHash: 'folder',
                expectedLocalTreeHash: 'common-root-tree',
                expectedLocalIdentity: 'inode-root',
                expectedRemoteHash: 'folder',
                expectedRemoteStructureHash: 'common-structure',
                targetHash: 'folder',
                entityId: 'folder-root',
            },
        };
        const scan = {
            local: new Map([
                [newPath, {
                    path: newPath, fileType: 'folder', hash: 'folder',
                    treeHash: 'local-root-tree', localIdentity: 'inode-root',
                }],
                [`${newPath}/main.tex`, {
                    path: `${newPath}/main.tex`, fileType: 'doc', hash: 'local-doc-edit',
                    text: 'common doc + local', localIdentity: 'inode-doc',
                }],
                [`${newPath}/images`, {
                    path: `${newPath}/images`, fileType: 'folder', hash: 'folder',
                    treeHash: 'local-images-tree', localIdentity: 'inode-images',
                }],
                [`${newPath}/images/plot.png`, {
                    path: `${newPath}/images/plot.png`, fileType: 'file',
                    hash: 'local-binary-edit', localIdentity: 'inode-binary',
                }],
            ]),
            remote: new Map([
                [newPath, {
                    path: newPath, fileType: 'folder', entityId: 'folder-root', hash: 'folder',
                    treeHash: 'remote-root-tree', remoteStructureHash: 'changed-structure',
                }],
                [`${newPath}/main.tex`, {
                    path: `${newPath}/main.tex`, fileType: 'doc', entityId: 'doc-main',
                    hash: 'remote-doc-edit', text: 'common doc + remote',
                }],
                [`${newPath}/images`, {
                    path: `${newPath}/images`, fileType: 'folder', entityId: 'folder-images',
                    hash: 'folder', treeHash: 'remote-images-tree',
                }],
                [`${newPath}/images/plot.png`, {
                    path: `${newPath}/images/plot.png`, fileType: 'file', entityId: 'file-plot',
                    hash: 'remote-binary-edit',
                }],
            ]),
        };
        const provider = pendingMoveCompletionProvider(
            LocalReplicaSCMProvider,
            state,
            scan,
        );

        await provider.reconcileUntilStable('recover folder move');

        assert.deepStrictEqual(Object.keys(state.entries).sort(), [
            newPath,
            `${newPath}/images`,
            `${newPath}/images/plot.png`,
            `${newPath}/main.tex`,
        ].sort());
        for (const [oldEntryPath, baseline] of Object.entries(expectedRelocated)) {
            const suffix = oldEntryPath.slice(oldPath.length);
            assert.deepStrictEqual(state.entries[`${newPath}${suffix}`], baseline);
        }
        assert.strictEqual(state.pendingAction, undefined);
    });

    it('refuses a local move after its source inode, tree, or hash changes', async () => {
        const changes = [
            {label: 'inode', value: {localIdentity: 'inode-new'}},
            {label: 'tree', value: {treeHash: 'tree-new'}},
            {label: 'hash', value: {hash: 'hash-new'}},
        ];
        for (const change of changes) {
            let renameCalls = 0;
            const LocalReplicaSCMProvider = loadProvider({
                l10n: {t: value => value},
                ThemeIcon: class ThemeIcon {},
                workspace: {
                    textDocuments: [],
                    fs: {rename: async () => { renameCalls++; }},
                },
            });
            const provider = Object.create(LocalReplicaSCMProvider.prototype);
            const expected = {
                path: 'sections',
                fileType: 'folder',
                hash: 'folder',
                treeHash: 'tree-old',
                localIdentity: 'inode-old',
            };
            provider.lastScan = {remote: new Map()};
            provider.localUri = filePath => ({fsPath: `/${filePath}`, path: `/${filePath}`});
            provider.scanLocal = async () => new Map([[
                'sections',
                {...expected, ...change.value},
            ]]);

            await assert.rejects(
                () => provider.moveLocalSnapshotAgainstBaseline(
                    'sections',
                    'moved-sections',
                    expected,
                ),
                error => error.message === 'Local move source changed: sections',
                change.label,
            );
            assert.strictEqual(renameCalls, 0, change.label);
        }
    });

    it('moves an open dirty document with WorkspaceEdit while preserving content and identity', async () => {
        const oldPath = 'sections/draft.tex';
        const newPath = 'sections/final.tex';
        const text = 'unsaved local edit\n';
        const document = {
            uri: {fsPath: `/${oldPath}`, path: `/${oldPath}`},
            isDirty: true,
            getText: () => text,
        };
        let applied = false;
        let filesystemRenameCalls = 0;
        let submittedEdit;
        class WorkspaceEdit {
            renameFile(oldUri, newUri, options) {
                this.rename = {oldUri, newUri, options};
            }
        }
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            WorkspaceEdit,
            workspace: {
                textDocuments: [document],
                applyEdit: async edit => {
                    submittedEdit = edit;
                    document.uri = edit.rename.newUri;
                    applied = true;
                    return true;
                },
                fs: {rename: async () => { filesystemRenameCalls++; }},
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const expected = {
            path: oldPath,
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode(text)),
            text,
            localIdentity: 'device:inode',
        };
        let finalSnapshot;
        provider.lastScan = {remote: new Map()};
        provider.localUri = filePath => ({fsPath: `/${filePath}`, path: `/${filePath}`});
        provider.scanLocal = async () => {
            const filePath = applied ? newPath : oldPath;
            const current = {
                ...expected,
                path: filePath,
                text: document.getText(),
                hash: contentHash(new TextEncoder().encode(document.getText())),
            };
            if (applied) {
                finalSnapshot = current;
            }
            return new Map([[filePath, current]]);
        };

        await provider.moveLocalSnapshotAgainstBaseline(oldPath, newPath, expected);

        assert(submittedEdit instanceof WorkspaceEdit);
        assert.deepStrictEqual(submittedEdit.rename, {
            oldUri: {fsPath: `/${oldPath}`, path: `/${oldPath}`},
            newUri: {fsPath: `/${newPath}`, path: `/${newPath}`},
            options: {overwrite: false, ignoreIfExists: false},
        });
        assert.strictEqual(filesystemRenameCalls, 0);
        assert.strictEqual(document.uri.fsPath, `/${newPath}`);
        assert.strictEqual(document.isDirty, true);
        assert.strictEqual(document.getText(), text);
        assert.strictEqual(finalSnapshot.hash, expected.hash);
        assert.strictEqual(finalSnapshot.localIdentity, expected.localIdentity);
    });

    it('passes immutable identity, type, and the authoritative move hash to the remote CAS', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const calls = [];
        provider.remoteUri = filePath => ({path: `/${filePath}`});
        provider.vfs = {
            moveResourceAgainstBaseline: async (...args) => calls.push(args),
        };

        await provider.moveRemoteSnapshotAgainstBaseline('main.tex', 'draft.tex', {
            path: 'main.tex',
            fileType: 'doc',
            entityId: 'doc-immutable',
            hash: 'content-hash',
        });
        await provider.moveRemoteSnapshotAgainstBaseline('sections', 'draft-sections', {
            path: 'sections',
            fileType: 'folder',
            entityId: 'folder-immutable',
            hash: 'folder',
            treeHash: 'content-tree-hash',
            remoteStructureHash: 'structure-hash',
        });

        assert.deepStrictEqual(calls, [
            [
                {path: '/main.tex'},
                {path: '/draft.tex'},
                'doc-immutable',
                'doc',
                'content-hash',
            ],
            [
                {path: '/sections'},
                {path: '/draft-sections'},
                'folder-immutable',
                'folder',
                'structure-hash',
            ],
        ]);
    });

    it('does not complete a pending move against a lookalike target identity or tree', () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const localPending = {
            id: 'move-local',
            kind: 'move-local',
            path: 'sections',
            targetPath: 'moved-sections',
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: 'folder',
            expectedLocalTreeHash: 'tree-owned',
            expectedLocalIdentity: 'inode-owned',
        };
        const localTarget = {
            path: 'moved-sections',
            fileType: 'folder',
            hash: 'folder',
            treeHash: 'tree-owned',
            localIdentity: 'inode-owned',
        };
        assert.strictEqual(provider.pendingActionCompleted(localPending, {
            local: new Map([['moved-sections', {...localTarget, localIdentity: 'inode-other'}]]),
            remote: new Map(),
        }), false);
        assert.strictEqual(provider.pendingActionCompleted(localPending, {
            local: new Map([['moved-sections', {...localTarget, treeHash: 'tree-other'}]]),
            remote: new Map(),
        }), false);

        const remotePending = {
            id: 'move-remote',
            kind: 'move-remote',
            path: 'sections',
            targetPath: 'moved-sections',
            startedAt: new Date(0).toISOString(),
            expectedRemoteHash: 'folder',
            expectedRemoteStructureHash: 'structure-owned',
            entityId: 'folder-owned',
        };
        const remoteTarget = {
            path: 'moved-sections',
            fileType: 'folder',
            hash: 'folder',
            entityId: 'folder-owned',
            remoteStructureHash: 'structure-owned',
        };
        assert.strictEqual(provider.pendingActionCompleted(remotePending, {
            local: new Map(),
            remote: new Map([['moved-sections', {...remoteTarget, entityId: 'folder-other'}]]),
        }), false);
        assert.strictEqual(provider.pendingActionCompleted(remotePending, {
            local: new Map(),
            remote: new Map([[
                'moved-sections',
                {...remoteTarget, remoteStructureHash: 'structure-other'},
            ]]),
        }), false);
    });

    it('refuses pending move recovery when the source tree changed or target is a lookalike', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        let moveCalls = 0;
        provider.moveRemoteSnapshotAgainstBaseline = async () => { moveCalls++; };
        const pending = {
            id: 'move-remote',
            kind: 'move-remote',
            path: 'sections',
            targetPath: 'moved-sections',
            startedAt: new Date(0).toISOString(),
            expectedRemoteHash: 'folder',
            expectedRemoteTreeHash: 'tree-owned',
            expectedRemoteStructureHash: 'structure-owned',
            entityId: 'folder-owned',
        };
        const changedSource = {
            path: 'sections',
            fileType: 'folder',
            hash: 'folder',
            treeHash: 'tree-changed',
            remoteStructureHash: 'structure-changed',
            entityId: 'folder-owned',
        };
        assert.strictEqual(await provider.resumePendingAction(pending, {
            local: new Map(),
            remote: new Map([['sections', changedSource]]),
        }), false);
        assert.strictEqual(await provider.resumePendingAction(pending, {
            local: new Map(),
            remote: new Map([['moved-sections', {
                ...changedSource,
                path: 'moved-sections',
                treeHash: 'tree-owned',
                remoteStructureHash: 'structure-owned',
                entityId: 'folder-lookalike',
            }]]),
        }), false);
        assert.strictEqual(moveCalls, 0);
    });
});

describe('Local Replica pending text action recovery', () => {
    it('derives the same path/type folder hash used by remote retained moves', () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const entries = new Map([
            ['sections', {path: 'sections', fileType: 'folder', hash: 'folder'}],
            ['sections/a.tex', {path: 'sections/a.tex', fileType: 'doc', hash: 'a'}],
            ['sections/images', {path: 'sections/images', fileType: 'folder', hash: 'folder'}],
            ['sections/images/x.png', {path: 'sections/images/x.png', fileType: 'file', hash: 'x'}],
        ]);

        provider.attachRemoteStructureHashes(entries);

        assert.strictEqual(
            entries.get('sections').remoteStructureHash,
            contentHash(Buffer.from(JSON.stringify([
                {path: 'a.tex', type: 'doc'},
                {path: 'images', type: 'folder'},
                {path: 'images/x.png', type: 'file'},
            ]))),
        );
        assert.strictEqual(
            entries.get('sections/images').remoteStructureHash,
            contentHash(Buffer.from(JSON.stringify([
                {path: 'x.png', type: 'file'},
            ]))),
        );
    });

    it('retains remote documents, resources, and folders through typed delete transactions', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        const calls = [];
        provider.vfs = {
            removeDocumentAgainstBaseline: async (...args) => calls.push(['doc', ...args]),
            removeResourceAgainstBaseline: async (...args) => calls.push(['file', ...args]),
            removeFolderAgainstBaseline: async (...args) => calls.push(['folder', ...args]),
        };

        await provider.removeRemoteSnapshot({
            path: 'main.tex', fileType: 'doc', entityId: 'doc-1', hash: 'doc-hash',
        });
        await provider.removeRemoteSnapshot({
            path: 'figure.png', fileType: 'file', entityId: 'file-1', hash: 'file-hash',
        });
        await provider.removeRemoteSnapshot({
            path: 'sections', fileType: 'folder', entityId: 'folder-1',
            hash: 'folder', treeHash: 'tree-hash', remoteStructureHash: 'structure-hash',
        });

        assert.deepStrictEqual(calls, [
            ['doc', {path: '/main.tex'}, 'doc-1', 'doc-hash'],
            ['file', {path: '/figure.png'}, 'file-1', 'file-hash'],
            ['folder', {path: '/sections'}, 'folder-1', 'structure-hash'],
        ]);
    });

    it('replays a proven pending local deletion from its durable pre-state', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const local = {
            path: 'hershel-e2e-interrupted',
            fileType: 'folder',
            hash: 'folder',
            treeHash: 'owned-tree',
            localIdentity: 'owned-directory',
        };
        const scan = {
            local: new Map([[local.path, local]]),
            remote: new Map(),
        };
        const calls = [];
        provider.deleteLocalSnapshot = async (filePath, snapshot, targets) => {
            calls.push({filePath, snapshot, targets});
        };

        const resumed = await provider.resumePendingAction({
            id: 'pending-delete',
            kind: 'delete-local',
            path: local.path,
            startedAt: new Date(0).toISOString(),
            expectedLocalHash: local.hash,
        }, scan);

        assert.strictEqual(resumed, true);
        assert.deepStrictEqual(calls, [{
            filePath: local.path,
            snapshot: local,
            targets: scan.local,
        }]);
    });

    it('commits an OT-transformed push before a subsequent local edit', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const encoder = new TextEncoder();
        const baselineText = 'shared base';
        const localText = 'shared base\nlocal first edit';
        const transformedText = 'remote edit\nshared base\nlocal first edit';
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(encoder.encode(baselineText)),
                    text: baselineText,
                    localIdentity: '1:2',
                },
            },
            conflicts: {},
        };
        const persisted = [];
        provider.stateStore = {
            save: async state => persisted.push(structuredClone(state)),
            saveBaseline: async content => contentHash(content),
        };
        provider.vfs = {
            acceptDocumentSnapshotAgainstAncestor: async (
                _uri, _ancestor, _target, _barrier, _entityId, opId,
            ) => ({opId, content: transformedText, acknowledgement: Promise.resolve(transformedText)}),
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.convergeTextToLocal = async () => transformedText;

        await provider.pushLocalSnapshot(
            'main.tex',
            provider.state.entries['main.tex'],
            {
                path: 'main.tex',
                fileType: 'doc',
                hash: contentHash(encoder.encode(localText)),
                content: encoder.encode(localText),
                text: localText,
                localIdentity: '1:2',
            },
            {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: contentHash(encoder.encode(baselineText)),
                text: baselineText,
            },
        );

        const committed = persisted.at(-1);
        assert.strictEqual(committed.pendingAction, undefined);
        assert.deepStrictEqual(committed.entries['main.tex'], {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: contentHash(encoder.encode(transformedText)),
            text: transformedText,
            localIdentity: '1:2',
        });
        assert.deepStrictEqual(decideReconciliation(
            committed.entries['main.tex'],
            {
                path: 'main.tex',
                fileType: 'doc',
                hash: contentHash(encoder.encode(`${transformedText}\nlocal second edit`)),
                text: `${transformedText}\nlocal second edit`,
            },
            {
                path: 'main.tex',
                fileType: 'doc',
                entityId: 'doc-1',
                hash: contentHash(encoder.encode(transformedText)),
                text: transformedText,
            },
        ), {action: 'push'});
    });

    it('atomically commits the landed push before processing a newer local edit', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const baseline = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'base',
            text: 'base text',
            localIdentity: '1:2',
        };
        provider.disposed = false;
        provider.textReplicas = new Map();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {'main.tex': baseline},
            conflicts: {},
            pendingAction: {
                id: 'direct-push',
                kind: 'push',
                path: 'main.tex',
                startedAt: new Date(0).toISOString(),
                expectedLocalHash: 'pushed',
                expectedRemoteHash: 'base',
                targetHash: 'pushed',
                entityId: 'doc-1',
            },
        };
        const newerLocal = {
            path: 'main.tex',
            fileType: 'doc',
            hash: 'newer-local',
            text: 'pushed text plus another local edit',
            localIdentity: '1:2',
        };
        const landedRemote = {
            path: 'main.tex',
            fileType: 'doc',
            entityId: 'doc-1',
            hash: 'pushed',
            text: 'pushed text',
        };
        const scan = {
            local: new Map([['main.tex', newerLocal]]),
            remote: new Map([['main.tex', landedRemote]]),
        };
        provider.scanBoth = async () => scan;
        provider.renderReport = () => {};
        provider.vfs = {
            safeSyncCapability: {writable: true},
            inspectRetainedRemoteCandidate: async () => undefined,
        };
        provider.ensureTextReplicas = async () => {};
        const persisted = [];
        provider.stateStore = {
            save: async state => persisted.push(structuredClone(state)),
            saveBaseline: async () => {},
        };
        const decisions = [];
        provider.reconcileOne = async current => {
            decisions.push(decideReconciliation(
                provider.state.entries['main.tex'],
                current.local.get('main.tex'),
                current.remote.get('main.tex'),
            ));
            return 'waiting';
        };

        await provider.reconcileUntilStable('second local edit after push');

        assert.strictEqual(persisted.length, 1);
        assert.strictEqual(persisted[0].pendingAction, undefined);
        assert.deepStrictEqual(persisted[0].entries['main.tex'], {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: 'pushed',
            text: 'pushed text',
            treeHash: undefined,
            localIdentity: '1:2',
        });
        assert.deepStrictEqual(decisions, [{action: 'push'}]);
    });

    it('commits a landed pull before a subsequent local edit', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const encoder = new TextEncoder();
        const baselineText = 'shared base';
        const remoteText = 'shared base\nremote edit';
        const baseline = {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: contentHash(encoder.encode(baselineText)),
            text: baselineText,
            localIdentity: '1:2',
        };
        const local = {
            path: 'main.tex',
            fileType: 'doc',
            hash: baseline.hash,
            text: baselineText,
            localIdentity: '1:2',
        };
        const remote = {
            path: 'main.tex',
            fileType: 'doc',
            entityId: 'doc-1',
            hash: contentHash(encoder.encode(remoteText)),
            text: remoteText,
        };
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {'main.tex': baseline},
            conflicts: {},
        };
        const persisted = [];
        provider.stateStore = {
            save: async state => persisted.push(structuredClone(state)),
            saveBaseline: async () => {},
        };
        provider.convergeTextToLocal = async () => remoteText;

        await provider.applyDecision(
            'main.tex',
            baseline,
            local,
            remote,
            {action: 'pull'},
            {local: new Map(), remote: new Map()},
        );

        const committed = persisted.at(-1);
        assert.strictEqual(committed.pendingAction, undefined);
        assert.deepStrictEqual(committed.entries['main.tex'], {
            entityId: 'doc-1',
            fileType: 'doc',
            hash: remote.hash,
            text: remoteText,
            localIdentity: '1:2',
        });
        assert.deepStrictEqual(decideReconciliation(
            committed.entries['main.tex'],
            {
                ...local,
                hash: contentHash(encoder.encode(`${remoteText}\nlocal edit`)),
                text: `${remoteText}\nlocal edit`,
            },
            remote,
        ), {action: 'push'});
    });
});

describe('Local Replica failure scope', () => {
    it('routes a terminal OT-history failure into durable WAL conflict registration', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.currentActionPath = 'main.tex';
        const recovery = {
            documentId: 'doc-1',
            path: '/main.tex',
            reason: 'history mismatch',
            failedAt: new Date(0).toISOString(),
        };
        provider.vfs = {
            inspectIsolatedDocumentRecovery: async documentId => {
                assert.strictEqual(documentId, 'doc-1');
                return recovery;
            },
        };
        const registered = [];
        provider.registerOrphanDocumentRecoveries = async candidates => {
            registered.push(...candidates);
        };
        const scheduled = [];
        provider.scheduleReconcile = (reason, delay) => scheduled.push([reason, delay]);

        await provider.handleReconcileError(new MockDocumentRecoveryIsolationError(
            'doc-1',
            '/main.tex',
            'history mismatch',
        ));

        assert.deepStrictEqual(registered, [recovery]);
        assert.deepStrictEqual(scheduled, [[
            'document recovery isolated: /main.tex',
            0,
        ]]);
        assert.strictEqual(provider.currentActionPath, undefined);
    });

    it('turns an unrollbackable retained remote candidate into a durable active conflict', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.currentActionPath = 'main.tex';
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
            pendingAction: {
                id: 'delete', kind: 'delete-remote', path: 'main.tex',
                startedAt: new Date(0).toISOString(),
            },
        };
        provider.lastScan = {local: new Map(), remote: new Map()};
        provider.createConflict = async (filePath, reason) => {
            provider.state.conflicts[filePath] = {
                path: filePath,
                reason,
                artifactDirectory: '.overleaf/conflicts/unresolved/retained',
                createdAt: new Date(0).toISOString(),
            };
        };
        provider.vfs = {
            readRetainedRemoteCandidate: async () => Buffer.from('LATEST-RETAINED'),
        };
        const artifacts = new Map();
        provider.writeArtifact = async (filePath, content) => {
            artifacts.set(filePath, Buffer.from(content));
        };
        provider.writeConflictManifest = async record => {
            artifacts.set('manifest', Buffer.from(JSON.stringify(record)));
        };
        let saves = 0;
        provider.stateStore = {save: async () => { saves++; }};
        provider.failProject = error => assert.fail(error.message);

        const error = new MockRetainedRemoteCandidateError('retained');
        error.uri = {path: '/main.tex'};
        error.entityId = 'old-doc';
        error.archivedHash = 'hash-at-failure';
        error.fileType = 'doc';
        await provider.handleReconcileError(error);

        const record = provider.state.conflicts['main.tex'];
        assert.strictEqual(record.retainedRemoteEntityId, 'old-doc');
        assert.strictEqual(
            record.retainedRemoteHash,
            contentHash(Buffer.from('LATEST-RETAINED')),
        );
        assert.strictEqual(
            artifacts.get(record.retainedRemoteFile).toString(),
            'LATEST-RETAINED',
        );
        assert.strictEqual(provider.state.pendingAction, undefined);
        assert(saves >= 2);
    });

    it('creates only one artifact for concurrent isolation of the same text path', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.textConflictTasks = new Map();
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1',
                    fileType: 'doc',
                    hash: contentHash(new TextEncoder().encode('BASE\n')),
                    text: 'BASE\n',
                },
            },
            conflicts: {},
        };
        provider.lastScan = {local: new Map(), remote: new Map()};
        provider.vfs = {
            readDocumentProjection: async () => {
                throw new Error('no live revision');
            },
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.stateStore = {save: async () => {}};
        provider.renderReport = () => {};
        provider.timestamp = () => 'time';
        const artifacts = [];
        provider.writeArtifact = async filePath => { artifacts.push(filePath); };
        let releaseBase;
        const baseGate = new Promise(resolve => { releaseBase = resolve; });
        provider.readCommonBase = async () => {
            await baseGate;
            return new TextEncoder().encode('BASE\n');
        };
        const isolation = {
            path: 'main.tex',
            reason: 'projection failed',
            canonicalContent: 'REMOTE\nBASE\n',
            editorContent: 'LOCAL\nBASE\n',
            diskContent: 'BASE\n',
        };

        const first = provider.createTextConflict(isolation);
        const second = provider.createTextConflict(isolation);
        releaseBase();
        await Promise.all([first, second]);

        assert.strictEqual(
            artifacts.filter(filePath => filePath.endsWith('/manifest.json')).length,
            1,
        );
        assert(provider.state.conflicts['main.tex']);
    });

    it('isolates an OT/path failure without pausing unrelated paths', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.currentActionPath = 'main.tex';
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
            pendingAction: {
                id: 'pending',
                kind: 'push',
                path: 'main.tex',
                startedAt: new Date(0).toISOString(),
            },
        };
        provider.lastScan = {local: new Map(), remote: new Map()};
        const persisted = [];
        provider.stateStore = {
            save: async state => persisted.push(structuredClone(state)),
        };
        provider.renderReport = () => {};
        let isolated;
        let pendingAtIsolation;
        provider.createConflict = async (filePath, reason) => {
            pendingAtIsolation = structuredClone(provider.state.pendingAction);
            isolated = {filePath, reason};
            provider.state.conflicts[filePath] = {
                path: filePath,
                reason,
                artifactDirectory: '.overleaf/conflicts/unresolved/test',
                createdAt: new Date(0).toISOString(),
            };
            await provider.stateStore.save(provider.state);
        };

        await provider.handleReconcileError(new Error('OT invariant failed'));

        assert.strictEqual(provider.pausedError, undefined);
        assert.strictEqual(provider.state.pendingAction, undefined);
        assert.strictEqual(provider.currentActionPath, undefined);
        assert.strictEqual(pendingAtIsolation.id, 'pending');
        assert.strictEqual(persisted[0].pendingAction.id, 'pending');
        assert(persisted[0].conflicts['main.tex']);
        assert.strictEqual(persisted.at(-1).pendingAction, undefined);
        assert.deepStrictEqual(isolated, {
            filePath: 'main.tex',
            reason: 'Document/path operation was isolated: OT invariant failed',
        });
    });

    it('does not pause the project because a file name contains "wal"', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {schemaVersion: 1, entries: {}, conflicts: {}};
        provider.lastScan = {local: new Map(), remote: new Map()};
        provider.output = {appendLine: () => {}};
        provider.failProject = error => assert.fail(
            `paused the whole project for ${error.message}`,
        );
        const scheduled = [];
        provider.scheduleReconcile = (reason, delay) => scheduled.push({reason, delay});

        // A document deleted and recreated under a new immutable id makes an
        // unscoped read miss. vscode.FileSystemError carries the offending
        // URI as its entire message, so the user's own file name is the only
        // text available to classify by.
        const missing = new Error(
            'overleaf-workshop://host/project/run/orphan-wal.tex'
            + '?user=u&project=p',
        );
        missing.name = 'EntryNotFound (FileSystemError)';
        missing.code = 'FileNotFound';
        await provider.handleReconcileError(missing);

        assert.strictEqual(provider.pausedError, undefined);
        assert.strictEqual(scheduled.length, 1);

        // The same holds for an ordinary node:fs miss on a path that merely
        // reads like durable-state prose.
        const enoent = new Error(
            "ENOENT: no such file or directory, open '/replica/firewall.tex'",
        );
        enoent.code = 'ENOENT';
        await provider.handleReconcileError(enoent);
        assert.strictEqual(provider.pausedError, undefined);
        assert.strictEqual(scheduled.length, 2);
    });

    it('retries a bare Overleaf 5xx instead of archiving a conflict for it', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {schemaVersion: 1, entries: {}, conflicts: {}};
        provider.lastScan = {local: new Map(), remote: new Map()};
        provider.output = {appendLine: () => {}};
        provider.failProject = error => assert.fail(error.message);
        provider.isolatePathFailure = () => assert.fail(
            'archived a conflict for a transient server refusal',
        );
        const scheduled = [];
        provider.scheduleReconcile = (reason, delay) => scheduled.push({reason, delay});

        // BaseAPI reports a failed request as "<status>: <body>".
        await provider.handleReconcileError(new Error(
            'Could not move remote resource watcher-roundtrip.tex: '
            + '500: <!DOCTYPE html><html lang="en">',
        ));

        assert.strictEqual(scheduled.length, 1);
        assert.strictEqual(provider.pausedError, undefined);
    });

    it('still pauses the project for a genuine durable-state failure', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {schemaVersion: 1, entries: {}, conflicts: {}};
        provider.lastScan = {local: new Map(), remote: new Map()};
        provider.output = {appendLine: () => {}};
        provider.scheduleReconcile = () => assert.fail('retried a durable-state failure');
        const paused = [];
        provider.failProject = error => paused.push(error.message);

        await provider.handleReconcileError(new Error('WAL handle was not opened'));
        await provider.handleReconcileError(
            new Error('Local replica binding is not a JSON object'),
        );
        await provider.handleReconcileError(new Error('Invalid checkpoint WAL sequence'));

        assert.deepStrictEqual(paused, [
            'WAL handle was not opened',
            'Local replica binding is not a JSON object',
            'Invalid checkpoint WAL sequence',
        ]);
    });

    it('archives common base, remote, local, and version metadata', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'figure.png': {
                    entityId: 'baseline-entity',
                    fileType: 'file',
                    hash: 'base-hash',
                },
            },
            conflicts: {},
        };
        provider.readCommonBase = async () => Buffer.from('base');
        provider.archiveSubtree = async (_directory, side) => `.overleaf/conflicts/unresolved/id/${side}/figure.png`;
        provider.timestamp = () => 'time';
        provider.stateStore = {save: async () => {}};
        const artifacts = new Map();
        provider.writeArtifact = async (filePath, content) => {
            artifacts.set(filePath, Buffer.from(content));
        };
        provider.status = {status: 'idle', message: ''};
        const scan = {
            local: new Map([['figure.png', {
                path: 'figure.png', fileType: 'file', hash: 'local-hash', content: Buffer.from('local'),
            }]]),
            remote: new Map([['figure.png', {
                path: 'figure.png', fileType: 'file', hash: 'remote-hash', entityId: 'remote-entity', content: Buffer.from('remote'),
            }]]),
        };

        await provider.createConflict('figure.png', 'concurrent binary edit', scan);

        const conflict = provider.state.conflicts['figure.png'];
        assert.strictEqual(conflict.baseHash, 'base-hash');
        assert.strictEqual(conflict.baselineEntityId, 'baseline-entity');
        assert.strictEqual(conflict.remoteEntityId, 'remote-entity');
        assert.strictEqual(artifacts.get(`${conflict.artifactDirectory}/base/figure.png`).toString(), 'base');
        const manifest = JSON.parse(artifacts.get(`${conflict.artifactDirectory}/manifest.json`).toString());
        assert.strictEqual(manifest.schemaVersion, 3);
        assert.strictEqual(manifest.baseAvailable, true);
        assert.strictEqual(manifest.reason, 'concurrent binary edit');
    });

    it('durably fences a displaced projection writer before acknowledging recovery', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const base = Buffer.from('BASE');
        const remote = Buffer.from('REMOTE');
        const local = Buffer.from('LOCAL-AFTER-DISPLACE');
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'base-doc',
                    fileType: 'doc',
                    hash: contentHash(base),
                    text: 'BASE',
                },
            },
            conflicts: {},
        };
        provider.readCommonBase = async () => base;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.vfs = {
            readAuthoritativeFileWithoutRecovery: async () => ({
                content: remote,
                entityId: 'remote-doc',
                fileType: 'doc',
                revision: 19,
            }),
        };
        const events = [];
        provider.stateStore = {
            save: async state => {
                assert(state.conflicts['main.tex'], 'conflict fence must exist before save');
                events.push('state-save');
            },
        };
        provider.localProjectionCas = {
            acknowledgeRecovery: async transactionId => {
                events.push(`ack:${transactionId}`);
            },
        };
        provider.output = {appendLine: value => events.push(value)};
        const artifacts = new Map();
        provider.writeArtifact = async (filePath, content) => {
            artifacts.set(filePath, Buffer.from(content));
        };
        const candidate = {
            transactionId: 'interrupted-1',
            path: 'main.tex',
            operation: 'replace',
            artifactDirectory: '.overleaf/conflicts/unresolved/projection-recovery-interrupted-1',
            localFile: '.overleaf/conflicts/unresolved/projection-recovery-interrupted-1/local-recovery/main.tex',
            localHash: contentHash(local),
            expectedHash: contentHash(base),
            targetHash: contentHash(remote),
        };

        await provider.registerProjectionRecoveryConflicts([candidate]);

        const record = provider.state.conflicts['main.tex'];
        assert(record, 'recovered writer was not made an active conflict');
        assert.strictEqual(record.localFile, candidate.localFile);
        assert.strictEqual(record.localHash, candidate.localHash);
        assert.strictEqual(record.remoteEntityId, 'remote-doc');
        assert.strictEqual(record.remoteRevision, 19);
        assert.strictEqual(
            artifacts.get(`${candidate.artifactDirectory}/base/main.tex`).toString(),
            'BASE',
        );
        assert.strictEqual(
            artifacts.get(`${candidate.artifactDirectory}/remote/main.tex`).toString(),
            'REMOTE',
        );
        const manifest = JSON.parse(
            artifacts.get(`${candidate.artifactDirectory}/manifest.json`).toString(),
        );
        assert.strictEqual(manifest.recoveryTransactionId, 'interrupted-1');
        assert.strictEqual(manifest.projectionExpectedHash, contentHash(base));
        assert.deepStrictEqual(events.slice(0, 2), [
            'state-save',
            'ack:interrupted-1',
        ]);
    });

    it('keeps the projection transaction replayable when the conflict fence cannot commit', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.readCommonBase = async () => undefined;
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.vfs = {
            readAuthoritativeFileWithoutRecovery: async () => ({
                content: Buffer.from('REMOTE'),
                entityId: 'remote-doc',
                fileType: 'doc',
                revision: 1,
            }),
        };
        provider.writeArtifact = async () => {};
        provider.stateStore = {save: async () => { throw new Error('state disk full'); }};
        let acknowledged = false;
        provider.localProjectionCas = {
            acknowledgeRecovery: async () => { acknowledged = true; },
        };
        provider.output = {appendLine() {}};
        const candidate = {
            transactionId: 'interrupted-2',
            path: 'main.tex',
            operation: 'replace',
            artifactDirectory: '.overleaf/conflicts/unresolved/projection-recovery-interrupted-2',
            localFile: '.overleaf/conflicts/unresolved/projection-recovery-interrupted-2/local-recovery/main.tex',
            localHash: contentHash(Buffer.from('LOCAL')),
            expectedHash: contentHash(Buffer.from('BASE')),
            targetHash: contentHash(Buffer.from('REMOTE')),
        };

        await assert.rejects(
            provider.registerProjectionRecoveryConflicts([candidate]),
            /state disk full/,
        );
        assert.strictEqual(acknowledged, false);
    });

    it('keeps the displaced writer selectable when the disk still shows the recovered projection target', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.timestamp = () => 'time';
        provider.archiveSubtree = async (_directory, side) => (
            `.overleaf/conflicts/unresolved/recovery/${side}/main.tex`
        );
        provider.writeConflictManifest = async () => {};
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.stateStore = {save: async () => {}};
        const record = {
            path: 'main.tex',
            artifactDirectory: '.overleaf/conflicts/unresolved/recovery',
            reason: 'projection recovery',
            createdAt: new Date(0).toISOString(),
            localFile: '.overleaf/conflicts/unresolved/recovery/local-recovery/main.tex',
            localHash: 'displaced-local-hash',
            projectionTargetHash: 'plugin-target-hash',
        };
        const scan = {
            local: new Map([['main.tex', {
                path: 'main.tex',
                fileType: 'doc',
                hash: 'plugin-target-hash',
            }]]),
            remote: new Map(),
        };

        await provider.archiveResolutionCandidates(record, scan);

        assert.strictEqual(
            record.localFile,
            '.overleaf/conflicts/unresolved/recovery/local-recovery/main.tex',
        );
        assert.strictEqual(record.localHash, 'displaced-local-hash');

        scan.local.get('main.tex').hash = 'new-user-edit-hash';
        await provider.archiveResolutionCandidates(record, scan);
        assert.match(record.localFile, /resolution-time-.*-local\/main\.tex/);
        assert.strictEqual(record.localHash, 'new-user-edit-hash');
    });
});

describe('Local Replica pending document operation crash recovery', () => {
    function createPendingHarness() {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const baselines = new Map();
        const saves = [];
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica-1',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {},
            conflicts: {},
        };
        provider.stateStore = {
            saveBaseline: async bytes => {
                const hash = contentHash(bytes);
                baselines.set(hash, Uint8Array.from(bytes));
                return hash;
            },
            readBaseline: async hash => baselines.get(hash),
            save: async state => {
                saves.push(JSON.parse(JSON.stringify(state.pendingAction)));
            },
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.status = {status: 'idle', message: ''};
        return {provider, baselines, saves};
    }

    async function prepareAction(provider) {
        await assert.rejects(
            () => provider.journalAction(
                'push',
                'main.tex',
                {path: 'main.tex', fileType: 'doc', hash: contentHash(Buffer.from('aLb'))},
                {
                    path: 'main.tex', fileType: 'doc', entityId: 'doc-1',
                    hash: contentHash(Buffer.from('ab')), text: 'ab',
                },
                contentHash(Buffer.from('aLb')),
                async () => { throw new Error('simulated crash'); },
                undefined,
                'doc-1',
                {ancestor: 'ab', target: 'aLb'},
            ),
            /simulated crash/,
        );
        return provider.state.pendingAction;
    }

    it('replays prepared intent with its original opId when the WAL has no operation', async () => {
        const {provider} = createPendingHarness();
        const pending = await prepareAction(provider);
        const acceptedIds = [];
        provider.vfs = {
            inspectDocumentOperation: async () => ({disposition: 'unknown', content: 'ab'}),
            acceptDocumentSnapshotAgainstAncestor: async (
                _uri, ancestor, target, barrier, entityId, opId,
            ) => {
                acceptedIds.push({ancestor, target, barrier, entityId, opId});
                return {
                    opId,
                    content: 'aLb',
                    acknowledgement: new Promise(() => {}),
                };
            },
        };

        assert.strictEqual(await provider.recoverPendingDocumentOperation(pending), 'waiting');
        assert.deepStrictEqual(acceptedIds, [{
            ancestor: 'ab', target: 'aLb', barrier: true,
            entityId: 'doc-1', opId: pending.documentOperationId,
        }]);
        assert.strictEqual(pending.documentOperationPhase, 'accepted');
        assert.strictEqual(pending.targetHash, contentHash(Buffer.from('aLb')));
    });

    it('adopts a WAL-pending operation after the accept-to-receipt crash cut without resubmitting', async () => {
        const {provider} = createPendingHarness();
        const pending = await prepareAction(provider);
        let submissions = 0;
        provider.vfs = {
            inspectDocumentOperation: async (_uri, entityId, opId) => {
                assert.strictEqual(entityId, 'doc-1');
                assert.strictEqual(opId, pending.documentOperationId);
                return {disposition: 'pending', content: 'aLRb'};
            },
            acceptDocumentSnapshotAgainstAncestor: async () => {
                submissions += 1;
            },
        };

        assert.strictEqual(await provider.recoverPendingDocumentOperation(pending), 'waiting');
        assert.strictEqual(submissions, 0);
        assert.strictEqual(pending.documentOperationPhase, 'accepted');
        assert.strictEqual(pending.targetHash, contentHash(Buffer.from('aLRb')));
    });

    it('persists accepted receipt and visible target before awaiting the remote ACK', async () => {
        const {provider, saves} = createPendingHarness();
        const pending = await prepareAction(provider);
        let acknowledge;
        const acknowledgement = new Promise(resolve => { acknowledge = resolve; });
        provider.vfs = {
            acceptDocumentSnapshotAgainstAncestor: async (
                _uri, _ancestor, _target, _barrier, _entityId, opId,
            ) => ({opId, content: 'aLRb', acknowledgement}),
        };

        const writing = provider.acceptPendingDocumentOperation(
            'main.tex',
            'ab',
            'aLb',
            'doc-1',
        );
        await new Promise(resolve => setImmediate(resolve));

        assert.strictEqual(pending.documentOperationPhase, 'accepted');
        assert.strictEqual(pending.targetHash, contentHash(Buffer.from('aLRb')));
        assert.strictEqual(saves.at(-1).documentOperationPhase, 'accepted');

        acknowledge('aLRb');
        assert.strictEqual(await writing, 'aLRb');
        assert.strictEqual(pending.documentOperationPhase, 'settled');
    });

    it('records settled disposition after the ACK-to-replica-save crash cut', async () => {
        const {provider} = createPendingHarness();
        const pending = await prepareAction(provider);
        pending.documentOperationPhase = 'accepted';
        provider.vfs = {
            inspectDocumentOperation: async () => ({disposition: 'settled', content: 'aLRb'}),
        };

        assert.strictEqual(await provider.recoverPendingDocumentOperation(pending), 'advanced');
        assert.strictEqual(pending.documentOperationPhase, 'settled');
        assert.strictEqual(pending.targetHash, contentHash(Buffer.from('aLRb')));
    });

    it('isolates accepted intent when its WAL disposition is no longer provable', async () => {
        const {provider} = createPendingHarness();
        const pending = await prepareAction(provider);
        pending.documentOperationPhase = 'accepted';
        provider.vfs = {
            inspectDocumentOperation: async () => ({disposition: 'unknown', content: 'ab'}),
        };

        await assert.rejects(
            () => provider.recoverPendingDocumentOperation(pending),
            /Cannot prove disposition of accepted document operation/,
        );
    });

    it('does not accumulate content-addressed blobs during a long text stream', async () => {
        const LocalReplicaSCMProvider = loadProvider();
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        const encoder = new TextEncoder();
        let current = 'BASE\n';
        provider.state = {
            schemaVersion: 1,
            replicaId: 'replica',
            initialized: true,
            updatedAt: new Date(0).toISOString(),
            entries: {
                'main.tex': {
                    entityId: 'doc-1', fileType: 'doc',
                    hash: contentHash(encoder.encode(current)),
                    text: current, localIdentity: '1:2',
                },
            },
            conflicts: {},
        };
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.localUri = pathname => ({fsPath: `/replica/${pathname}`});
        provider.vfs = {
            readAuthoritativeFile: async () => ({
                entityId: 'doc-1', fileType: 'doc',
                content: encoder.encode(current), revision: 1,
            }),
        };
        provider.readLocalText = async () => current;
        provider.localIdentity = async () => '1:2';
        let baselineBlobWrites = 0;
        provider.stateStore = {
            saveBaseline: async () => {
                baselineBlobWrites++;
                throw new Error('unexpected document baseline blob');
            },
            save: async () => {},
        };

        for (let index = 0; index < 200; index++) {
            current += `edit-${index}\n`;
            await provider.advanceTextBaselineIfAuthoritative('main.tex', current, 'doc-1');
        }

        assert.strictEqual(baselineBlobWrites, 0);
        assert.strictEqual(provider.state.entries['main.tex'].text, current);
    });
});

describe('Local Replica projection atomicity', () => {
    // A filesystem writer that wins the disk CAS must not leave the editor
    // holding the target text. The retry loop re-derives its expected local
    // hash from the CAS result, so a half-applied projection makes the editor
    // and disk branches disagree forever instead of converging.
    function projectionHarness(diskBytes) {
        const editor = {text: 'base', dirty: false};
        const disk = {bytes: diskBytes};
        const LocalReplicaSCMProvider = loadProvider({
            l10n: {t: value => value},
            ThemeIcon: class ThemeIcon {},
            Uri: {file: fsPath => ({fsPath, path: fsPath})},
            window: {
                visibleTextEditors: [],
                createOutputChannel: () => ({appendLine() {}, clear() {}, dispose() {}}),
            },
            workspace: {
                textDocuments: [{
                    uri: {fsPath: '/main.tex', toString: () => 'file:///main.tex'},
                    get isDirty() { return editor.dirty; },
                    getText: () => editor.text,
                }],
                applyEdit: async () => true,
                fs: {
                    createDirectory: async () => {},
                    readFile: async () => disk.bytes,
                },
            },
            WorkspaceEdit: class WorkspaceEdit {
                replace() {}
            },
            Range: class Range {},
            Position: class Position {},
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.localUri = () => ({fsPath: '/main.tex', path: '/main.tex'});
        provider.projectEditorCas = async (_path, expected, target) => {
            if (editor.text !== expected) {
                return false;
            }
            editor.text = target;
            return true;
        };
        return {provider, editor, disk};
    }

    it('rolls the editor back when a filesystem writer wins the disk CAS', async () => {
        const baseBytes = new TextEncoder().encode('base');
        const foreignBytes = new TextEncoder().encode('written by an external agent');
        const {provider, editor} = projectionHarness(baseBytes);
        provider.localProjectionCas = {
            replace: async () => ({applied: false, actual: foreignBytes}),
        };

        await assert.rejects(
            () => provider.writeLocalContent(
                'main.tex',
                new TextEncoder().encode('remote canonical'),
                contentHash(baseBytes),
                true,
            ),
            /Local content changed while projecting synchronized content/,
        );
        assert.strictEqual(
            editor.text,
            'base',
            'the editor kept the target text after the disk CAS refused it',
        );
    });

    it('accepts synchronized bytes installed by a concurrent idempotent projection', async () => {
        const baseBytes = new TextEncoder().encode('base');
        const targetBytes = new TextEncoder().encode('remote canonical');
        const {provider, editor, disk} = projectionHarness(baseBytes);
        provider.localProjectionCas = {
            replace: async () => {
                disk.bytes = targetBytes;
                return {applied: false, actual: targetBytes};
            },
        };

        await provider.writeLocalContent(
            'main.tex',
            targetBytes,
            contentHash(baseBytes),
            true,
        );

        assert.strictEqual(editor.text, 'remote canonical');
        assert.strictEqual(new TextDecoder().decode(disk.bytes), 'remote canonical');
    });

    it('reports the editor endpoint when a keystroke blocks the rollback', async () => {
        const baseBytes = new TextEncoder().encode('base');
        const foreignBytes = new TextEncoder().encode('written by an external agent');
        const {provider, editor} = projectionHarness(baseBytes);
        provider.localProjectionCas = {
            replace: async () => {
                // A keystroke lands between the editor projection and the
                // rollback, so the rollback CAS must not overwrite it.
                editor.text = 'remote canonicalX';
                return {applied: false, actual: foreignBytes};
            },
        };

        await assert.rejects(
            () => provider.writeLocalContent(
                'main.tex',
                new TextEncoder().encode('remote canonical'),
                contentHash(baseBytes),
                true,
            ),
            error => (
                error.actualContent !== undefined
                && new TextDecoder().decode(error.actualContent) === 'remote canonicalX'
            ),
        );
        assert.strictEqual(editor.text, 'remote canonicalX');
    });

    it('rebases a post-projection keystroke from the projected head exactly once', async () => {
        const encoder = new TextEncoder();
        const baseBytes = encoder.encode('base');
        const remoteText = 'base remote';
        const localAfterProjection = `${remoteText} local`;
        const {provider, editor, disk} = projectionHarness(baseBytes);
        editor.dirty = true;
        let projections = 0;
        provider.localProjectionCas = {
            replace: async (_path, _expected, target) => {
                projections += 1;
                disk.bytes = target;
                if (projections === 1) {
                    editor.text = localAfterProjection;
                }
                return {applied: true, actual: target};
            },
        };
        const remoteWrites = [];
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        provider.preparePendingDocumentOperation = async () => {};
        provider.acceptPendingDocumentOperation = async (_filePath, ancestor, target, entityId) => {
            remoteWrites.push({ancestor, target, entityId});
            return target;
        };
        provider.updatePendingTarget = async () => {};

        const converged = await provider.convergeTextToLocal(
            'main.tex',
            remoteText,
            'base',
            contentHash(baseBytes),
            contentHash(baseBytes),
            'doc-1',
        );

        assert.strictEqual(converged, localAfterProjection);
        assert.deepStrictEqual(remoteWrites, [{
            ancestor: remoteText,
            target: localAfterProjection,
            entityId: 'doc-1',
        }]);
        assert.strictEqual(new TextDecoder().decode(disk.bytes), localAfterProjection);
        assert(!converged.includes('remote remote'));
    });
});
