const assert = require('assert');
const Module = require('module');
const path = require('path');
const {decideReconciliation} = require('../out/sync/reconcile');
const {contentHash} = require('../out/sync/syncPolicy');

function loadProvider() {
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return {
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
                ConcurrentRemoteResourceError: class ConcurrentRemoteResourceError extends Error {},
            };
        }
        if (request === '../utils/eventBus') {
            return {EventBus: {fire() {}}};
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
        vfs.notify = events => notifications.push(events);
        vfs.pathToUri = (...parts) => parts.join('/');

        vfs.applyRemoteMove(document._id, destination._id);
        vfs.applyRemoteMove(document._id, destination._id);

        assert.deepStrictEqual(source.docs, []);
        assert.deepStrictEqual(destination.docs, [document]);
        assert.strictEqual(notifications.length, 1);
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
});

describe('Local Replica pending text action recovery', () => {
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
        };
        provider.vfs = {
            writeDocumentAgainstAncestor: async () => transformedText,
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
        provider.vfs = {safeSyncCapability: {writable: true}};
        const persisted = [];
        provider.stateStore = {
            save: async state => persisted.push(structuredClone(state)),
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
