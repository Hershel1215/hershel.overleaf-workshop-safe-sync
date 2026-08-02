const assert = require('assert');
const Module = require('module');
const path = require('path');
const {decideReconciliation} = require('../out/sync/reconcile');
const {contentHash} = require('../out/sync/syncPolicy');
const {TextReplicaStateMachine} = require('../out/sync/textReplicaStateMachine');

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
        provider.state = {initialized: true};
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
    it('keeps the live journal registered when session close fails', async () => {
        const {vfs} = loadVirtualFileSystem({type: 'success'});
        const {DocumentJournal} = require('../out/sync/documentJournal');
        const removeJournal = DocumentJournal.remove;
        let journalRemoved = false;
        let legacyRemoved = false;
        DocumentJournal.remove = async () => { journalRemoved = true; };
        vfs.documentSessions = new Map([['remote-id', Promise.resolve({
            close: async () => { throw new Error('close failed'); },
        })]]);
        vfs.documentJournalDirectory = '/tmp/safe-sync-test';
        vfs.legacyDocumentStateStore = {
            remove: async () => { legacyRemoved = true; },
        };
        vfs.socket = {leaveDoc: async () => {}};

        try {
            await assert.rejects(
                () => vfs.discardDocumentRecovery({path: '/fixture.tex'}),
                /close failed/,
            );
            assert.strictEqual(vfs.documentSessions.has('remote-id'), true);
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

    it('keeps the last same-document snapshot across a coalesced version jump', () => {
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.editorObservationHeads = new Map();
        let content = 'before reload';
        const document = {
            version: 4,
            getText: () => content,
        };
        provider.captureEditorObservation('fixture.tex', document);

        content = 'after external reload';
        document.version = 9;
        const aggregate = provider.captureEditorObservation('fixture.tex', document);
        assert.deepStrictEqual(aggregate, {
            content: 'after external reload',
            observedAncestor: 'before reload',
        });

        const reopened = {
            version: 10,
            getText: () => 'new document instance',
        };
        assert.deepStrictEqual(
            provider.captureEditorObservation('fixture.tex', reopened),
            {content: 'new document instance', observedAncestor: undefined},
        );
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

        await provider.deleteLocalSnapshot('chapters', root, expected);

        assert.deepStrictEqual(deletes, [
            {path: 'chapters/section.tex', options: {recursive: false, useTrash: false}},
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
                fs: {
                    delete: async (uri, options) => deletes.push({uri, options}),
                },
            },
        });
        const provider = Object.create(LocalReplicaSCMProvider.prototype);
        provider.localUri = () => ({fsPath: '/main.tex'});
        provider.localIdentity = async () => 'same-inode';
        const expected = {
            path: 'main.tex',
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode(editorText)),
            localIdentity: 'same-inode',
        };

        await provider.deleteLocalSnapshot('main.tex', expected);

        assert.deepStrictEqual(deletes, [{
            uri: {fsPath: '/main.tex'},
            options: {recursive: false, useTrash: false},
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
});

describe('Local Replica text projection attachment', () => {
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
            disposeAndWait: async () => { events.push('retire-scheduler'); },
        };
        provider.textReplicas = new Map([['main.tex', oldMachine]]);
        provider.textSchedulers = new Map([['main.tex', oldScheduler]]);
        provider.resolvingPaths = new Set();
        provider.recoverConflictArtifactLocation = async () => {};
        provider.vfs = {
            discardDocumentRecovery: async () => { events.push('discard-journal'); },
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
        provider.moveConflictToResolved = async () => {
            events.push('archive');
            return '.overleaf/conflicts/resolved/test';
        };
        provider.scheduleReconcile = () => { events.push('reattach'); };

        await provider.resolveConflictRecord('main.tex', 'remote');

        assert.strictEqual(events[0], 'retire-machine');
        assert(events.indexOf('retire-machine') < events.indexOf('restore'));
        assert(events.indexOf('retire-scheduler') < events.indexOf('restore'));
        assert(events.indexOf('discard-journal') < events.indexOf('scan'));
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
        provider.resolvingPaths = new Set();
        provider.recoverConflictArtifactLocation = async () => {};
        provider.vfs = {discardDocumentRecovery: async () => {}};
        provider.remoteUri = pathname => ({path: `/${pathname}`});
        const main = snapshot('main.tex', 'doc');
        main.text = 'remote';
        const scan = {
            local: new Map([['main.tex', main]]),
            remote: new Map([['main.tex', main]]),
        };
        provider.scanBoth = async () => scan;
        provider.journalAction = async (_kind, _path, _local, _remote, _hash, action) => action();
        provider.restoreRemotePath = async () => {};
        provider.isSubtreeConverged = () => true;
        provider.refreshConvergedState = async () => {};
        provider.stateStore = {save: async () => {}};
        let expectedTargets;
        provider.copyLocalTree = async (_source, _target, expected) => {
            expectedTargets = expected;
        };
        provider.moveConflictToResolved = async () => '.overleaf/conflicts/resolved/test-local';
        provider.scheduleReconcile = () => {};

        await provider.resolveConflictRecord('main.tex', 'local');

        assert.strictEqual(expectedTargets, scan.local);
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
        provider.stateStore = {
            save: async state => { saved = structuredClone(state); },
        };

        await provider.recoverConflictArtifactLocation(record);

        assert.strictEqual(record.artifactDirectory, resolved);
        assert.strictEqual(record.baseFile, `${resolved}/base/main.tex`);
        assert.strictEqual(record.localFile, `${resolved}/local-editor/main.tex`);
        assert.strictEqual(record.remoteFile, `${resolved}/remote/main.tex`);
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
            disposeAndWait: async () => { events.push('retire-machine'); },
        }]]);
        provider.textSchedulers = new Map([['main.tex', {
            disposeAndWait: async () => { events.push('retire-scheduler'); },
        }]]);
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
            saveBaseline: async content => contentHash(content),
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

describe('Local Replica pending text action recovery', () => {
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
            saveBaseline: async () => {},
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
        provider.vfs = {safeSyncCapability: {writable: true}};
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
});
