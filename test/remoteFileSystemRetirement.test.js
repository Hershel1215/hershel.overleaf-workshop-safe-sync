const assert = require('assert');
const Module = require('module');
const {mkdtemp, rm} = require('fs/promises');
const os = require('os');
const path = require('path');

function deferred() {
    let resolve;
    let reject;
    const promise = new Promise((resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
    });
    return {promise, resolve, reject};
}

function nextTurn() {
    return new Promise(resolve => setImmediate(resolve));
}

function loadVirtualFileSystem(authenticate, events) {
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return {
                Disposable: class Disposable {},
                l10n: {t: value => value},
                ProgressLocation: {Notification: 15},
                FileChangeType: {Changed: 2, Created: 1, Deleted: 3},
                workspace: {
                    workspaceFolders: undefined,
                    getConfiguration: () => ({get: (_key, fallback) => fallback}),
                },
                window: {
                    showErrorMessage: async () => { events.push('connection-error'); },
                    showWarningMessage: async () => undefined,
                    withProgress: async (_options, task) => task(),
                },
                commands: {
                    executeCommand: command => events.push(`command:${command}`),
                },
            };
        }
        if (request === '../utils/globalStateManager') {
            return {GlobalStateManager: {authenticate}};
        }
        if (request === '../collaboration/clientManager') {
            return {
                ClientManager: class ClientManager {
                    constructor() {
                        events.push('client-created');
                    }
                    get triggers() {
                        return [{dispose: () => events.push('client-disposed')}];
                    }
                },
            };
        }
        if (request === '../scm/scmCollectionProvider') {
            return {
                SCMCollectionProvider: class SCMCollectionProvider {
                    constructor() {
                        events.push('scm-created');
                    }
                    get triggers() {
                        return [{dispose: () => events.push('scm-disposed')}];
                    }
                },
            };
        }
        if (request === '../utils/eventBus') {
            return {EventBus: {fire() {}}};
        }
        if (request === '../sync/documentStateStore') {
            return {DocumentStateStore: class DocumentStateStore {}};
        }
        return originalLoad.apply(this, arguments);
    };
    try {
        delete require.cache[require.resolve('../out/core/remoteFileSystemProvider')];
        return require('../out/core/remoteFileSystemProvider').VirtualFileSystem;
    } finally {
        Module._load = originalLoad;
    }
}

function createVfs(VirtualFileSystem, options, events) {
    const vfs = Object.create(VirtualFileSystem.prototype);
    Object.assign(vfs, {
        retiring: false,
        retirementPromise: undefined,
        connectionReady: false,
        initializing: undefined,
        retryConnection: 0,
        retryTimer: undefined,
        reconnectingNotification: false,
        handlersRegistered: true,
        root: undefined,
        clientManagerItem: undefined,
        scmCollectionItem: undefined,
        documentSessions: new Map(),
        documentSessionFailures: new Map(),
        documentProjectionSources: new WeakSet(),
        documentProjectionListeners: new Set(),
        documentSessionsClosePromise: undefined,
        documentSessionClosures: new Set(),
        documentSessionCloseError: undefined,
        documentPendingOperations: new Map(),
        logicalAcknowledgementCount: 0,
        documentRecoveryFences: new Set(),
        documentRecoveryDiscardTasks: new Map(),
        logicalDocumentAcknowledgements: new Map(),
        documentAcceptAdmissions: new Map(),
        documentJournalEpochs: new Map(),
        preparedDocumentRecoveries: new Map(),
        preparedDocumentRecoveryById: new Map(),
        committingPreparedDocumentRecoveries: new Set(),
        remoteSafeSyncNamespacePromise: undefined,
        context: options.context ?? {},
        serverName: 'test-server',
        projectId: 'test-project',
        publicId: 'test-public-id',
        origin: {},
        socket: {
            needsReinit: false,
            joinProject: options.joinProject,
            retire: () => events.push('socket-retire'),
        },
        api: {
            getProjectSettings: options.getProjectSettings,
        },
    });
    return vfs;
}

describe('VirtualFileSystem retirement fence', () => {
    it('shares one reconnect attempt while an invalidated transport is backing off', async () => {
        const events = [];
        const joining = deferred();
        let joins = 0;
        const VFS = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VFS, {
            joinProject: () => { joins++; return joining.promise; },
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.retryConnection = 1;
        vfs.socket.needsReinit = true;
        vfs.socket.init = () => { vfs.socket.needsReinit = false; };
        vfs.socket.updateEventHandlers = () => {};
        const attempts = Array.from({length: 8}, () => vfs.init());
        await new Promise(resolve => setTimeout(resolve, 1100));
        joining.resolve({});
        await Promise.all(attempts);
        assert.strictEqual(joins, 1, 'parallel callers started independent reconnect loops');
    });

    it('stops after the retry budget when project settings keep failing', async () => {
        const events = [];
        const VFS = loadVirtualFileSystem(async () => ({}), events);
        let joins = 0;
        const vfs = createVfs(VFS, {
            joinProject: async () => { joins++; return {}; },
            getProjectSettings: async () => {
                // Bound the broken implementation so the regression cannot hang.
                if (joins > 7) vfs.retiring = true;
                throw new Error('settings transport unavailable');
            },
        }, events);
        const realSetTimeout = global.setTimeout;
        global.setTimeout = (fn, _ms, ...args) => realSetTimeout(fn, 0, ...args);
        try {
            await assert.rejects(vfs.init());
            const attempts = joins;
            await assert.rejects(vfs.init());
            assert.strictEqual(attempts, 5, 'partial join success reset the retry budget');
            assert.strictEqual(joins, attempts, 'a later observer restarted exhausted retries');
            assert.strictEqual(events.filter(e => e === 'connection-error').length, 1);
        } finally {
            global.setTimeout = realSetTimeout;
        }
    });

    it('does not append another remote watcher when rebuilding a socket', async () => {
        const events = [];
        const VFS = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VFS, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.handlersRegistered = false;
        let watchers = 0;
        vfs.socket.updateEventHandlers = () => { watchers++; };
        vfs.socket.init = () => { vfs.socket.needsReinit = false; };
        await vfs.init();
        for (let i = 0; i < 4; i++) {
            vfs.socket.needsReinit = true;
            await vfs.init();
        }
        assert.strictEqual(watchers, 1, 'SocketIOAPI already reattaches its registered handlers');
    });

    it('publishes one monotonic retirement before manager disposal can re-enter it', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        let reentered;
        vfs.clientManagerItem = {
            triggers: [{dispose: () => { reentered = vfs.retireForReplicaHandoff(); }}],
        };

        const retirement = vfs.retireForReplicaHandoff();
        await retirement;
        await reentered;

        assert.strictEqual(events.filter(event => event === 'socket-retire').length, 1);
        assert.strictEqual(vfs.retiring, true);
    });

    it('waits for joinProject to settle and never retries a retiring initialization', async () => {
        const events = [];
        const joining = deferred();
        let authenticateCalls = 0;
        const VirtualFileSystem = loadVirtualFileSystem(async () => {
            authenticateCalls++;
            return {};
        }, events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: () => joining.promise,
            getProjectSettings: async () => ({settings: {}}),
        }, events);

        const initialization = vfs.init();
        await nextTurn(); // Enter joinProject before testing in-flight retirement.
        let retirementSettled = false;
        const retirement = vfs.retireForReplicaHandoff().then(() => {
            retirementSettled = true;
        });
        const duplicateRetirement = vfs.retireForReplicaHandoff();
        await nextTurn();

        assert.strictEqual(retirementSettled, false);
        assert.strictEqual(events.filter(event => event === 'socket-retire').length, 1);
        joining.reject(new Error('socket closed'));
        await assert.rejects(initialization, /retiring for replica handoff/);
        await Promise.all([retirement, duplicateRetirement]);

        assert.strictEqual(vfs.retryConnection, 0);
        assert.strictEqual(authenticateCalls, 0);
        assert.strictEqual(vfs.root, undefined);
        await assert.rejects(vfs.init(), /retiring for replica handoff/);
    });

    it('checks retirement after joinProject before authenticating', async () => {
        const events = [];
        const joining = deferred();
        let authenticateCalls = 0;
        const VirtualFileSystem = loadVirtualFileSystem(async () => {
            authenticateCalls++;
            return {};
        }, events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: () => joining.promise,
            getProjectSettings: async () => ({settings: {}}),
        }, events);

        const initialization = vfs.init();
        const retirement = vfs.retireForReplicaHandoff();
        joining.resolve({});

        await assert.rejects(initialization, /retiring for replica handoff/);
        await retirement;
        assert.strictEqual(authenticateCalls, 0);
        assert.strictEqual(vfs.root, undefined);
    });

    it('checks retirement after authentication before requesting settings', async () => {
        const events = [];
        const authenticating = deferred();
        let settingsCalls = 0;
        const VirtualFileSystem = loadVirtualFileSystem(
            () => authenticating.promise,
            events,
        );
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => {
                settingsCalls++;
                return {settings: {}};
            },
        }, events);

        const initialization = vfs.init();
        await nextTurn();
        const retirement = vfs.retireForReplicaHandoff();
        authenticating.resolve({});

        await assert.rejects(initialization, /retiring for replica handoff/);
        await retirement;
        assert.strictEqual(settingsCalls, 0);
        assert.strictEqual(vfs.root, undefined);
    });

    it('checks retirement after settings before publishing root or managers', async () => {
        const events = [];
        const loadingSettings = deferred();
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: () => loadingSettings.promise,
        }, events);

        const initialization = vfs.init();
        await nextTurn();
        const retirement = vfs.retireForReplicaHandoff();
        loadingSettings.resolve({settings: {compiler: 'pdflatex'}});

        await assert.rejects(initialization, /retiring for replica handoff/);
        await retirement;
        assert.strictEqual(vfs.root, undefined);
        assert(!events.includes('client-created'));
        assert(!events.includes('scm-created'));
        assert(!events.some(event => event.startsWith('command:')));
    });

    it('drains a newer document-session generation before retirement completes', async () => {
        const events = [];
        const firstClose = deferred();
        const secondClose = deferred();
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.documentSessions.set('first', Promise.resolve({
            close: async () => {
                events.push('first-close');
                await firstClose.promise;
            },
        }));
        vfs.resetDocumentSessions();
        vfs.documentSessions.set('second', Promise.resolve({
            close: async () => {
                events.push('second-close');
                await secondClose.promise;
            },
        }));

        let retired = false;
        const retirement = vfs.retireForReplicaHandoff().then(() => { retired = true; });
        await nextTurn();
        assert.deepStrictEqual(events.filter(event => event.endsWith('-close')), ['first-close']);
        assert.strictEqual(retired, false);

        firstClose.resolve();
        await nextTurn();
        assert.deepStrictEqual(events.filter(event => event.endsWith('-close')), [
            'first-close',
            'second-close',
        ]);
        assert.strictEqual(retired, false);

        secondClose.resolve();
        await retirement;
        assert.strictEqual(vfs.documentSessions.size, 0);
    });

    it('tracks a removed document session close through ownership retirement', async () => {
        const events = [];
        const closing = deferred();
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        let handlers;
        vfs.socket.updateEventHandlers = value => { handlers = value; };
        const fileEntity = {_id: 'removed-doc', name: 'main.tex'};
        vfs._resolveById = () => ({
            parentFolder: {},
            fileType: 'doc',
            fileEntity,
            path: '/main.tex',
        });
        vfs.removeEntity = () => events.push('entity-removed');
        vfs.notify = () => events.push('delete-notified');
        vfs.pathToUri = pathname => pathname;
        vfs.documentSessions.set(fileEntity._id, Promise.resolve({
            close: async () => {
                events.push('removed-close');
                await closing.promise;
            },
        }));
        vfs.remoteWatch();

        handlers.onFileRemoved(fileEntity._id);
        let retired = false;
        const retirement = vfs.retireForReplicaHandoff().then(() => { retired = true; });
        await nextTurn();
        assert.strictEqual(retired, false);
        assert(events.includes('removed-close'));
        assert.strictEqual(vfs.documentSessions.has(fileEntity._id), false);

        closing.resolve();
        await retirement;
        assert.strictEqual(retired, true);
    });

    it('does not reopen a document WAL before the previous session closes', async () => {
        const events = [];
        const closing = deferred();
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const session = {id: 'next-owner', bindPath: async () => {}};
        vfs.connectionReady = true;
        vfs.documentSessions.set('doc-id', Promise.resolve(session));
        vfs.documentSessionsClosePromise = closing.promise;

        let opened = false;
        const opening = vfs.getDocumentSession({_id: 'doc-id'}, '/main.tex').then(value => {
            opened = true;
            return value;
        });
        await nextTurn();
        assert.strictEqual(opened, false);

        vfs.documentSessionsClosePromise = undefined;
        closing.resolve();
        assert.strictEqual(await opening, session);
    });

    it('reuses the Local Replica manager across a project reconnect', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);

        await vfs.init();
        vfs.root = undefined;
        vfs.connectionReady = false;
        vfs.initializing = undefined;
        await vfs.init();

        assert.strictEqual(events.filter(event => event === 'client-created').length, 1);
        assert.strictEqual(events.filter(event => event === 'scm-created').length, 1);
        assert(!events.includes('client-disposed'));
        assert(!events.includes('scm-disposed'));
    });

    it('rebuilds an invalidated transport instead of reporting readiness', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        let joins = 0;
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => { joins++; return {}; },
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.socket.init = () => {
            events.push('socket-init');
            vfs.socket.needsReinit = false;
        };
        vfs.socket.updateEventHandlers = () => events.push('handlers-registered');

        await vfs.init();
        assert.strictEqual(joins, 1);
        assert.strictEqual(vfs.connectionReady, true);

        // A membership RPC that timed out invalidates the socket without
        // disconnecting the project, so root and connectionReady both survive
        // it. Readiness that only consults those two answers "ready" forever on
        // a socket that refuses every joinDoc.
        vfs.socket.needsReinit = true;
        await vfs.init();

        assert.strictEqual(joins, 2, 'readiness answered ready on a poisoned transport');
        assert.strictEqual(events.filter(event => event === 'socket-init').length, 1);
        assert.strictEqual(vfs.socket.needsReinit, false);
    });

    it('returns an existing WAL owner offline without waiting for project reconnect', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const session = {
            id: 'offline-owner',
            suspendTransport: () => events.push('transport-suspended'),
            bindPath: async () => {},
        };
        vfs.documentSessions.set('doc-id', Promise.resolve(session));
        vfs.init = async () => {
            throw new Error('existing WAL owner must not await the network');
        };

        assert.strictEqual(
            await vfs.getDocumentSession({_id: 'doc-id'}, '/main.tex'),
            session,
        );
        assert.deepStrictEqual(events, ['transport-suspended']);
    });

    it('journals through the immutable document id while the path resolver is offline', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const accepted = {
            content: 'aLb',
            acknowledgement: Promise.resolve('aLb'),
        };
        const session = {
            content: 'aLb',
            revision: 3,
            sequence: 5,
            suspendTransport: () => events.push('transport-suspended'),
            bindPath: async () => {},
            acceptSnapshotAgainstAncestor: async (...args) => {
                events.push(['accept', ...args]);
                return accepted;
            },
        };
        vfs.documentSessions.set('doc-id', Promise.resolve(session));
        vfs._resolveUri = async () => {
            throw new Error('offline path resolution must not run');
        };

        assert.strictEqual(
            await vfs.acceptDocumentSnapshotAgainstAncestor(
                {path: '/main.tex'},
                'ab',
                'aLb',
                true,
                'doc-id',
            ),
            accepted,
        );
        assert.deepStrictEqual(
            await vfs.readDocumentProjection({path: '/main.tex'}, 'doc-id'),
            {content: 'aLb', revision: 3, entityId: 'doc-id', sequence: 5},
        );
        assert.deepStrictEqual(events, [
            'transport-suspended',
            ['accept', 'ab', 'aLb', true],
            'transport-suspended',
        ]);
    });

    it('journals through the immutable document id after the remote path moves', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const document = {_id: 'doc-id', name: 'main.tex'};
        vfs.root = {};
        vfs.connectionReady = true;
        vfs._resolveById = entityId => {
            assert.strictEqual(entityId, 'doc-id');
            return {
                fileType: 'doc',
                fileEntity: document,
                path: '/sections/main.tex',
            };
        };
        vfs._resolveUri = async () => {
            throw new Error('stale path lookup must not run');
        };
        vfs.getDocumentSession = async (entity, resolvedPath) => {
            assert.strictEqual(entity, document);
            assert.strictEqual(resolvedPath, '/sections/main.tex');
            return {
                acceptSnapshotAgainstAncestor: async (...args) => {
                    events.push(['accept', ...args]);
                    return {
                        opId: 'fixed-op',
                        content: 'aLb',
                        acknowledgement: Promise.resolve('aLb'),
                    };
                },
            };
        };
        vfs.observeLogicalDocumentOperation = (
            _entity, resolvedPath, opId, _session, acknowledgement,
        ) => {
            events.push(['observe', resolvedPath, opId]);
            return acknowledgement;
        };

        const accepted = await vfs.acceptDocumentSnapshotAgainstAncestor(
            {path: '/main.tex'},
            'ab',
            'aLb',
            true,
            'doc-id',
            'fixed-op',
        );

        assert.strictEqual(accepted.opId, 'fixed-op');
        assert.deepStrictEqual(events, [
            ['accept', 'ab', 'aLb', true, 'fixed-op'],
            ['observe', '/sections/main.tex', 'fixed-op'],
        ]);
    });

    it('catches up a suspended WAL owner before reconnect becomes writable', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const session = {
            revision: 4,
            resumeTransport: async (...args) => {
                assert.strictEqual(vfs.connectionReady, false);
                events.push(['resume', ...args]);
            },
        };
        vfs.root = {rootFolder: []};
        vfs._resolveById = documentId => documentId === 'doc-id'
            ? {fileType: 'doc', fileEntity: {_id: 'doc-id', name: 'main.tex'}}
            : undefined;
        vfs.documentSessions.set('doc-id', Promise.resolve(session));
        vfs.socket.joinDoc = async (documentId, revision) => {
            assert.deepStrictEqual([documentId, revision], ['doc-id', 4]);
            return {
                otType: 'sharejs-text-ot',
                version: 5,
                docLines: ['aRb'],
                updates: [{v: 4, op: [{p: 1, i: 'R'}], meta: {source: 'remote'}}],
            };
        };

        await vfs.resumeDocumentSessionTransports();
        assert.deepStrictEqual(events, [[
            'resume',
            'aRb',
            5,
            [{doc: 'doc-id', v: 4, op: [{p: 1, i: 'R'}], meta: {source: 'remote'}}],
        ]]);
    });

    it('publishes every document projection at the current path and supports disposal', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs.root = {rootFolder: []};
        vfs.documentJournalDirectory = '/tmp/safe-sync-moved-open-document';
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.legacyDocumentStateStore = {load: async () => undefined};
        vfs.socket.isUsingAlternativeConnectionScheme = false;
        vfs.socket.joinDoc = async () => ({
            otType: 'sharejs-text-ot', version: 1, docLines: ['base'], updates: [],
        });
        const doc = {_id: 'moved-doc', name: 'main.tex'};
        let currentPath = '/old/main.tex';
        vfs._resolveById = () => ({path: currentPath, fileType: 'doc', fileEntity: doc});
        vfs.pathToUri = path => ({path});
        const notifications = [];
        const projections = [];
        vfs.notify = changes => notifications.push(...changes);
        const subscription = vfs.onDidChangeDocumentProjection(
            change => projections.push(change),
        );

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const durable = require('../out/sync/durableDocumentSession').DurableDocumentSession;
        const originalOpenExisting = journal.openExisting;
        const originalCreate = durable.create;
        let callbacks;
        journal.openExisting = async () => ({
            revision: 1,
            bindPath: async () => {},
            close: async () => {},
        });
        durable.create = async options => {
            callbacks = options;
            return {isFailed: false};
        };
        try {
            await vfs.getDocumentSession(doc, currentPath);
            assert.strictEqual(
                vfs.hasLiveDocumentProjection(doc._id),
                false,
                'an unresolved session registration is not yet a direct projection source',
            );
            currentPath = '/new/main.tex';
            callbacks.onChange('remote edit 1', 2, 'remote edit 1', false, 7);
            assert.strictEqual(vfs.hasLiveDocumentProjection(doc._id), true);
            callbacks.onChange('remote edit 2', 3, 'remote edit 2', false, 8);

            assert.deepStrictEqual(projections, [
                {
                    content: 'remote edit 1',
                    documentId: 'moved-doc',
                    path: '/new/main.tex',
                    revision: 2,
                    sequence: 7,
                },
                {
                    content: 'remote edit 2',
                    documentId: 'moved-doc',
                    path: '/new/main.tex',
                    revision: 3,
                    sequence: 8,
                },
            ]);
            assert.deepStrictEqual(
                notifications.map(change => change.uri.path),
                ['/new/main.tex', '/new/main.tex'],
            );

            subscription.dispose();
            callbacks.onChange('after disposal', 4, 'after disposal', false, 9);
            assert.strictEqual(projections.length, 2);

            callbacks.onError(new Error('runtime failure'));
            assert.strictEqual(vfs.hasLiveDocumentProjection(doc._id), false);
        } finally {
            journal.openExisting = originalOpenExisting;
            durable.create = originalCreate;
        }
    });

    it('retries one transient document reconnect without blocking another pending WAL', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.root = {rootFolder: []};
        vfs.connectionReady = true;
        vfs.notify = changes => events.push(...changes.map(change => `notify:${change.uri.path}`));
        vfs.pathToUri = path => ({path});
        vfs._resolveById = documentId => ({
            path: `/${documentId}.tex`,
            fileType: 'doc',
            fileEntity: {_id: documentId, name: `${documentId}.tex`},
        });
        let firstAttempts = 0;
        vfs.socket.joinDoc = async (documentId, revision) => {
            assert.strictEqual(revision, 4);
            if (documentId === 'doc-a' && firstAttempts++ === 0) {
                throw new Error('one transient join failure');
            }
            return {
                otType: 'sharejs-text-ot', version: 4, docLines: [documentId], updates: [],
            };
        };
        const resumed = [];
        const session = documentId => ({
            revision: 4,
            hasPendingOperations: true,
            resumeTransport: async content => { resumed.push([documentId, content]); },
        });
        const first = Promise.resolve(session('doc-a'));
        const second = Promise.resolve(session('doc-b'));
        vfs.documentSessions.set('doc-a', first);
        vfs.documentSessions.set('doc-b', second);

        try {
            await vfs.resumeDocumentSessionTransports();
            assert.deepStrictEqual(resumed, [['doc-b', 'doc-b']]);
            assert.strictEqual(vfs.documentSessionFailures.has('doc-a'), false);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, false);

            await new Promise(resolve => setTimeout(resolve, 400));
            assert.deepStrictEqual(resumed, [
                ['doc-b', 'doc-b'],
                ['doc-a', 'doc-a'],
            ]);
            assert.strictEqual(vfs.documentSessionFailures.has('doc-a'), false);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, true);
            assert(events.includes('notify:/doc-a.tex'));
        } finally {
            vfs.cancelDocumentSessionRetry('doc-a');
            vfs.cancelDocumentSessionRetry('doc-b');
        }
    });

    it('reconnects the project instead of retrying a document on an invalidated socket', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.root = {rootFolder: []};
        vfs.connectionReady = true;
        vfs._resolveById = documentId => ({
            path: `/${documentId}.tex`,
            fileType: 'doc',
            fileEntity: {_id: documentId, name: `${documentId}.tex`},
        });
        let joinCalls = 0;
        vfs.socket.joinDoc = async () => {
            joinCalls++;
            vfs.socket.needsReinit = true;
            const timeout = new Error('membership acknowledgement timed out');
            timeout.name = 'SocketIOTimeoutError';
            throw timeout;
        };
        let suspends = 0;
        const pending = Promise.resolve({
            revision: 4,
            hasPendingOperations: true,
            isFailed: false,
            suspendTransport: () => { suspends++; },
        });
        vfs.documentSessions.set('doc-a', pending);

        await assert.rejects(
            vfs.resumeDocumentSessionTransports(),
            /membership transport was invalidated/,
        );
        assert.strictEqual(joinCalls, 1);
        assert.strictEqual(suspends, 1);
        assert.strictEqual(vfs.documentSessions.get('doc-a'), pending);
        assert.strictEqual(vfs.documentSessionRetryTimers?.has('doc-a') ?? false, false);
        assert.strictEqual(vfs.documentSessionFailures.has('doc-a'), false);

        await new Promise(resolve => setTimeout(resolve, 350));
        assert.strictEqual(joinCalls, 1, 'poisoned socket received a document retry');
    });

    it('reopens a fresh WAL owner after resumeTransport fail-stops its generation', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.root = {rootFolder: []};
        vfs.connectionReady = true;
        vfs.notify = changes => events.push(...changes.map(change => `notify:${change.uri.path}`));
        vfs.pathToUri = path => ({path});
        const doc = {_id: 'failed-generation', name: 'main.tex'};
        vfs._resolveById = documentId => documentId === doc._id
            ? {path: '/main.tex', fileType: 'doc', fileEntity: doc}
            : undefined;
        vfs.socket.joinDoc = async () => ({
            otType: 'sharejs-text-ot', version: 4, docLines: ['server'], updates: [],
        });

        let failed = false;
        let closes = 0;
        const failedGeneration = {
            revision: 4,
            hasPendingOperations: true,
            get isFailed() { return failed; },
            resumeTransport: async () => {
                failed = true;
                throw new Error('reconnect ancestry mismatch');
            },
            close: async () => { closes++; },
        };
        const pending = Promise.resolve(failedGeneration);
        vfs.documentSessions.set(doc._id, pending);
        const replacement = {isFailed: false};
        let reopens = 0;
        vfs.getDocumentSession = async (reopenedDoc, reopenedPath) => {
            assert.strictEqual(reopenedDoc, doc);
            assert.strictEqual(reopenedPath, '/main.tex');
            reopens++;
            return replacement;
        };

        try {
            await vfs.resumeDocumentSessionTransports();
            assert.strictEqual(vfs.documentSessions.has(doc._id), false);
            assert.strictEqual(closes, 1);
            assert.match(vfs.documentSessionFailures.get(doc._id).message, /ancestry mismatch/);

            await new Promise(resolve => setTimeout(resolve, 400));
            assert.strictEqual(reopens, 1);
            assert.strictEqual(vfs.documentSessionFailures.has(doc._id), false);
            assert.strictEqual(vfs.documentSessionReopenIds.has(doc._id), false);
            assert(events.includes('notify:/main.tex'));
        } finally {
            vfs.cancelDocumentSessionRetry(doc._id);
        }
    });

    it('isolates one repeated OT-history proof without blocking another document', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.root = {rootFolder: []};
        vfs.connectionReady = true;
        vfs.documentJournalDirectory = '/tmp/safe-sync-terminal-ot-proof';
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.legacyDocumentStateStore = {load: async () => undefined};
        vfs.socket.isUsingAlternativeConnectionScheme = false;
        vfs.notify = changes => events.push(...changes.map(change => `notify:${change.uri.path}`));
        vfs.pathToUri = path => ({path});
        const documents = new Map([
            ['doc-a', {_id: 'doc-a', name: 'main.tex'}],
            ['doc-b', {_id: 'doc-b', name: 'appendix.tex'}],
        ]);
        vfs._resolveById = documentId => {
            const doc = documents.get(documentId);
            return doc ? {
                path: documentId === 'doc-a' ? '/main.tex' : '/appendix.tex',
                fileType: 'doc',
                fileEntity: doc,
            } : undefined;
        };
        vfs.socket.joinDoc = async documentId => ({
            otType: 'sharejs-text-ot',
            version: 4,
            docLines: [documentId === 'doc-a' ? 'REMOTE' : 'appendix'],
            updates: [],
        });

        let firstFailed = false;
        let firstCloses = 0;
        let secondResumes = 0;
        const first = Promise.resolve({
            revision: 4,
            hasPendingOperations: true,
            get isFailed() { return firstFailed; },
            resumeTransport: async () => {
                firstFailed = true;
                throw new Error('first reconnect ancestry mismatch');
            },
            close: async () => { firstCloses++; },
        });
        const second = Promise.resolve({
            revision: 4,
            hasPendingOperations: false,
            isFailed: false,
            resumeTransport: async () => { secondResumes++; },
            close: async () => {},
        });
        vfs.documentSessions.set('doc-a', first);
        vfs.documentSessions.set('doc-b', second);

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const durable = require('../out/sync/durableDocumentSession').DurableDocumentSession;
        const originalOpenExisting = journal.openExisting;
        const originalCreate = durable.create;
        let freshProofs = 0;
        journal.openExisting = async () => ({
            revision: 4,
            bindPath: async () => {},
            close: async () => {},
        });
        durable.create = async () => {
            freshProofs++;
            throw new Error('operation history does not reproduce authoritative snapshot');
        };
        vfs.readDocumentRecoverySnapshot = async () => ({
            visibleContent: 'BASE LOCAL',
            acknowledgedContent: 'BASE',
            revision: 4,
            pendingOperationIds: ['op-a'],
        });

        try {
            await vfs.resumeDocumentSessionTransports();
            assert.strictEqual(secondResumes, 1);
            assert.strictEqual(firstCloses, 1);

            await new Promise(resolve => setTimeout(resolve, 450));
            assert.strictEqual(freshProofs, 1);
            assert.strictEqual(vfs.documentSessionRetryTimers.has('doc-a'), false);
            assert.strictEqual(vfs.documentSessionReopenIds.has('doc-a'), false);
            assert.strictEqual(vfs.documentSessionFailures.has('doc-a'), false);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, true);
            assert(events.includes('notify:/main.tex'));

            const recovery = await vfs.inspectIsolatedDocumentRecovery('doc-a');
            assert.strictEqual(recovery.path, '/main.tex');
            assert.strictEqual(recovery.visibleContent, 'BASE LOCAL');
            assert.strictEqual(recovery.acknowledgedContent, 'BASE');
            assert.deepStrictEqual(recovery.pendingOperationIds, ['op-a']);
            assert.match(recovery.reason, /after one fresh generation/);
            assert.match(recovery.reason, /operation history/);

            await new Promise(resolve => setTimeout(resolve, 350));
            assert.strictEqual(freshProofs, 1, 'terminal history proof retried silently');
        } finally {
            journal.openExisting = originalOpenExisting;
            durable.create = originalCreate;
            vfs.cancelDocumentSessionRetry('doc-a');
            vfs.cancelDocumentSessionRetry('doc-b');
        }
    });

    it('follows one journaled operation across repeated reconnects without resubmitting it', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        const doc = {_id: 'doc-id', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        vfs._resolveById = () => ({path: '/main.tex'});

        const firstAck = deferred();
        const secondAck = deferred();
        let acceptCount = 0;
        let acceptedArgs;
        const observedIds = [];
        const first = {
            acceptSnapshotAgainstAncestor: async (...args) => {
                acceptCount++;
                acceptedArgs = args;
                return {opId: 'stable-op-id', content: 'aLb', acknowledgement: firstAck.promise};
            },
        };
        const second = {
            observeOperation: opId => {
                observedIds.push(opId);
                return secondAck.promise;
            },
        };
        const third = {
            observeOperation: opId => {
                observedIds.push(opId);
                return Promise.resolve('aLb');
            },
        };
        const generations = [first, second, third];
        vfs.getDocumentSession = async () => generations.shift();

        const writing = vfs.writeDocumentAgainstAncestor(
            {path: '/main.tex'},
            'ab',
            'aLb',
            'doc-id',
            'bootstrap-fixed-id',
        );
        await nextTurn();
        firstAck.reject(new (require('../out/sync/durableDocumentSession').DocumentSessionRetiredError)());
        await nextTurn();
        secondAck.reject(new (require('../out/sync/durableDocumentSession').DocumentSessionRetiredError)());

        assert.strictEqual(await writing, 'aLb');
        assert.strictEqual(acceptCount, 1);
        assert.deepStrictEqual(acceptedArgs, ['ab', 'aLb', true, 'bootstrap-fixed-id']);
        assert.deepStrictEqual(observedIds, ['stable-op-id', 'stable-op-id']);
        assert.strictEqual(vfs.logicalAcknowledgementCount, 0);
    });

    it('does not report a disconnected or closing generation as settled', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        assert.strictEqual(vfs.isSafeSyncConnectionReady, true);

        vfs.connectionReady = false;
        assert.strictEqual(vfs.isSafeSyncConnectionReady, false);
        vfs.connectionReady = true;
        vfs.documentSessionClosures.add(new Promise(() => {}));
        assert.strictEqual(vfs.isSafeSyncConnectionReady, false);
        assert.strictEqual(vfs.hasPendingDocumentOperations, true);
    });

    it('reads an E2E recovery proof only from the registered WAL owner', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const doc = {_id: 'doc-id', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        let opened = 0;
        vfs.getDocumentSession = async () => {
            opened++;
            throw new Error('E2E proof opened another document session');
        };
        vfs.documentSessions.set(doc._id, Promise.resolve({
            revision: 7,
            content: 'BASE LOCAL',
            acknowledgedContent: 'BASE',
            pendingOperationIds: ['fixed-op'],
            inspectOperation: opId => {
                assert.strictEqual(opId, 'fixed-op');
                return {disposition: 'pending', content: 'BASE LOCAL'};
            },
        }));

        assert.deepStrictEqual(
            await vfs.inspectDocumentRecoveryForE2E({path: '/main.tex'}, 'fixed-op'),
            {
                entityId: 'doc-id',
                revision: 7,
                visibleContent: 'BASE LOCAL',
                acknowledgedContent: 'BASE',
                pendingOperationIds: ['fixed-op'],
                operationDisposition: 'pending',
            },
        );
        vfs.documentSessions.clear();
        assert.strictEqual(
            await vfs.inspectDocumentRecoveryForE2E({path: '/main.tex'}, 'fixed-op'),
            undefined,
        );
        assert.strictEqual(opened, 0);
    });

    it('fails closed instead of reopening a WAL after a session close failure', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs.documentSessionCloseError = new Error('previous WAL close failed');
        let joinCalls = 0;
        vfs.socket.joinDoc = async () => {
            joinCalls++;
            throw new Error('must not join');
        };

        await assert.rejects(
            vfs.getDocumentSession({_id: 'doc-id', name: 'main.tex'}, '/main.tex'),
            /previous WAL close failed/,
        );
        assert.strictEqual(joinCalls, 0);
        assert.strictEqual(vfs.documentSessions.size, 0);
        assert.strictEqual(vfs.isSafeSyncConnectionReady, false);
        assert.strictEqual(vfs.hasPendingDocumentOperations, true);
    });

    it('routes filesystem document writes through the admitted ancestor operation path', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const doc = {_id: 'doc-id', name: 'main.tex'};
        const uri = {path: '/main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        vfs.readDocumentProjection = async () => ({
            content: 'remote-base', revision: 4, entityId: doc._id,
        });
        const writes = [];
        vfs.writeDocumentAgainstAncestor = async (...args) => { writes.push(args); };

        await vfs.writeFile(uri, new TextEncoder().encode('local-target'), false, true);

        assert.deepStrictEqual(writes, [[uri, 'remote-base', 'local-target', 'doc-id']]);
        assert.strictEqual(vfs.isDirty, false);
    });

    it('rejects a recreated document path before opening the replacement session', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs._resolveUri = async () => ({
            fileType: 'doc',
            fileEntity: {_id: 'replacement-id', name: 'main.tex'},
        });
        vfs.getDocumentSession = async () => {
            throw new Error('replacement session must not be opened');
        };

        await assert.rejects(
            vfs.acceptDocumentSnapshotAgainstAncestor(
                {path: '/main.tex'},
                'ab',
                'aLb',
                true,
                'original-id',
            ),
            /Remote resource changed after the common baseline/,
        );
        await assert.rejects(
            vfs.readDocumentProjection({path: '/main.tex'}, 'original-id'),
            /Remote resource changed after the common baseline/,
        );
    });

    it('fences logical ACK followers before closing and removing a recovery WAL', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs.documentJournalDirectory = '/tmp/safe-sync-test-journals';
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.legacyDocumentStateStore = {
            remove: async () => { events.push('legacy-remove'); },
        };
        vfs.socket.leaveDoc = async () => { events.push('leave-doc'); };
        const doc = {
            _id: 'discard-doc',
            name: 'main.tex',
            version: 7,
            localCache: 'aLb',
            remoteCache: 'ab',
        };
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        vfs._resolveById = () => ({path: '/main.tex'});

        const acknowledgement = deferred();
        const closeGate = deferred();
        let closeCount = 0;
        let acceptCount = 0;
        const session = {
            bindPath: async () => {},
            acceptSnapshotAgainstAncestor: async () => {
                acceptCount++;
                return {
                    opId: 'discard-op',
                    content: 'aLb',
                    acknowledgement: acknowledgement.promise,
                };
            },
            close: async () => {
                closeCount++;
                events.push('session-close');
                acknowledgement.reject(
                    new (require('../out/sync/durableDocumentSession').DocumentSessionRetiredError)(),
                );
                await closeGate.promise;
                events.push('session-closed');
            },
            waitForTransportIdle: async () => { events.push('transport-idle'); },
            hasPendingOperations: false,
        };
        vfs.documentSessions.set(doc._id, Promise.resolve(session));
        const originalGet = vfs.getDocumentSession.bind(vfs);
        let getSessionCalls = 0;
        vfs.getDocumentSession = (...args) => {
            getSessionCalls++;
            return originalGet(...args);
        };

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalRemove = journal.remove;
        journal.remove = async () => { events.push('wal-remove'); };
        try {
            const accepted = await vfs.acceptDocumentSnapshotAgainstAncestor(
                {path: '/main.tex'},
                'ab',
                'aLb',
                true,
            );
            const logicalResult = accepted.acknowledgement.catch(error => error);
            const discarding = vfs.discardDocumentRecovery({path: '/main.tex'});
            await nextTurn();
            await assert.rejects(
                vfs.acceptDocumentSnapshotAgainstAncestor(
                    {path: '/main.tex'},
                    'ab',
                    'aSECONDb',
                    true,
                ),
                /recovery is being discarded/,
            );
            assert.strictEqual(acceptCount, 1);
            closeGate.resolve();
            await discarding;

            const followerError = await logicalResult;
            assert(followerError instanceof require('../out/sync/durableDocumentSession').DocumentSessionRetiredError);
            assert.strictEqual(getSessionCalls, 1);
            assert.strictEqual(closeCount, 1);
            assert(events.indexOf('session-closed') < events.indexOf('wal-remove'));
            assert.strictEqual(vfs.logicalDocumentAcknowledgements.has(doc._id), false);
            assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), false);
        } finally {
            journal.remove = originalRemove;
        }
    });

    it('closes an acquired journal when joinDoc fails before session creation', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs.documentJournalDirectory = '/tmp/safe-sync-factory-test';
        vfs.legacyDocumentStateStore = {load: async () => undefined};
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.socket.isUsingAlternativeConnectionScheme = false;
        vfs.socket.joinDoc = async () => {
            events.push('join-doc');
            throw new Error('socket disconnected');
        };

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        let closeCount = 0;
        journal.openExisting = async () => ({
            revision: 4,
            bindPath: async () => {},
            close: async () => {
                closeCount++;
                events.push('journal-close');
            },
        });
        try {
            await assert.rejects(
                vfs.getDocumentSession({_id: 'factory-doc', name: 'main.tex'}, '/main.tex'),
                /socket disconnected/,
            );
            assert.deepStrictEqual(events, ['join-doc', 'journal-close']);
            assert.strictEqual(closeCount, 1);

            await vfs.closeDocumentSessions();
            assert.strictEqual(vfs.documentSessionCloseError, undefined);
            assert.strictEqual(vfs.documentSessions.size, 0);
        } finally {
            journal.openExisting = originalOpenExisting;
        }
    });

    it('treats a fenced pending session factory as a cleaned cancellation', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.documentJournalDirectory = '/tmp/safe-sync-pending-factory-test';
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.legacyDocumentStateStore = {remove: async () => events.push('legacy-remove')};
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const doc = {_id: 'pending-doc', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        const pending = deferred();
        vfs.documentSessions.set(doc._id, pending.promise);

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalRemove = journal.remove;
        journal.remove = async () => events.push('wal-remove');
        try {
            const discarding = vfs.discardDocumentRecovery({path: '/main.tex'});
            pending.reject(
                new (require('../out/sync/durableDocumentSession').DocumentSessionRetiredError)(),
            );
            await discarding;

            assert(events.includes('wal-remove'));
            assert.strictEqual(vfs.documentSessionCloseError, undefined);
            assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), false);
        } finally {
            journal.remove = originalRemove;
        }
    });

    it('preserves the WAL when an in-flight send has no proven server disposition', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-unsettled-send';
        vfs.legacyDocumentStateStore = {remove: async () => events.push('legacy-remove')};
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const doc = {_id: 'unsettled-doc', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        const session = {
            waitForTransportIdle: async () => { events.push('transport-idle'); },
            hasPendingOperations: true,
            close: async () => { events.push('session-close'); },
        };
        vfs.documentSessions.set(doc._id, Promise.resolve(session));

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalRemove = journal.remove;
        journal.remove = async () => events.push('wal-remove');
        try {
            await assert.rejects(
                vfs.discardDocumentRecovery({path: '/main.tex'}),
                /without a proven server disposition/,
            );
            assert.deepStrictEqual(events, ['transport-idle']);
            assert.strictEqual(vfs.documentSessions.has(doc._id), true);
            assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), false);
        } finally {
            journal.remove = originalRemove;
        }
    });

    it('preserves a detached WAL whose durable operation disposition is still pending', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-detached-pending';
        vfs.legacyDocumentStateStore = {remove: async () => events.push('legacy-remove')};
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const doc = {_id: 'detached-pending-doc', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        const originalRemove = journal.remove;
        journal.openExisting = async () => ({
            hasPendingOperations: true,
            close: async () => events.push('journal-close'),
        });
        journal.remove = async () => events.push('wal-remove');
        try {
            await assert.rejects(
                vfs.discardDocumentRecovery({path: '/main.tex'}),
                /durable operations without a proven server disposition/,
            );
            assert.deepStrictEqual(events, ['journal-close']);
            assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), false);
        } finally {
            journal.openExisting = originalOpenExisting;
            journal.remove = originalRemove;
        }
    });

    it('commits a prepared pending session only after archive, transport idle, and owner close', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-prepare-session';
        vfs.legacyDocumentStateStore = {
            load: async () => undefined,
            remove: async () => events.push('legacy-remove'),
        };
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const documentId = 'prepared-session-doc';
        const snapshot = {
            content: 'visible local candidate',
            acknowledgedContent: 'acknowledged remote',
            revision: 7,
            pendingOperationIds: ['pending-op'],
        };
        const session = {
            ...snapshot,
            waitForTransportIdle: async () => events.push('transport-idle'),
            hasPendingOperations: true,
            close: async () => events.push('session-close'),
        };
        vfs.documentSessions.set(documentId, Promise.resolve(session));
        vfs.documentJournalEpochs.set(documentId, 3);

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        const originalRemove = journal.remove;
        journal.openExisting = async () => ({
            content: snapshot.content,
            acknowledgedContent: snapshot.acknowledgedContent,
            revision: snapshot.revision,
            pendingOperationIds: snapshot.pendingOperationIds,
            close: async () => events.push('journal-close'),
        });
        journal.remove = async () => events.push('wal-remove');
        try {
            const prepared = await vfs.prepareDocumentRecoveryById(documentId);
            assert.strictEqual(prepared.visibleContent, snapshot.content);
            assert.strictEqual(prepared.acknowledgedContent, snapshot.acknowledgedContent);
            assert.strictEqual(prepared.revision, snapshot.revision);
            assert.deepStrictEqual(prepared.pendingOperationIds, snapshot.pendingOperationIds);
            assert.strictEqual(prepared.epoch, 3);
            assert(!events.includes('wal-remove'));
            events.push('durable-archive');

            await vfs.commitDiscardDocumentRecovery(prepared.token);

            assert(events.indexOf('durable-archive') < events.lastIndexOf('transport-idle'));
            assert(events.indexOf('transport-idle') < events.indexOf('session-close'));
            assert(events.indexOf('session-close') < events.indexOf('wal-remove'));
            assert(events.includes('legacy-remove'));
            assert.strictEqual(vfs.documentSessions.has(documentId), false);
            assert.strictEqual(vfs.documentRecoveryFences.has(documentId), false);
        } finally {
            journal.openExisting = originalOpenExisting;
            journal.remove = originalRemove;
        }
    });

    it('prepares and commits a WAL-only old entity after its path was recreated', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-prepare-deleted-recreated';
        vfs.legacyDocumentStateStore = {
            load: async () => undefined,
            remove: async id => events.push(`legacy-remove:${id}`),
        };
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const documentId = 'deleted-old-doc';
        const replacement = {_id: 'replacement-doc', name: 'main.tex', _type: 'doc'};
        vfs.root = {rootFolder: [{
            _id: 'root', name: 'Project', docs: [replacement], fileRefs: [], folders: [],
        }]};
        const replacementSession = {close: async () => events.push('replacement-close')};
        vfs.documentSessions.set(replacement._id, Promise.resolve(replacementSession));
        const snapshot = {
            content: 'old visible candidate',
            acknowledgedContent: 'old acknowledged content',
            revision: 4,
            pendingOperationIds: ['old-pending-op'],
        };

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        const originalRemove = journal.remove;
        journal.openExisting = async () => ({
            ...snapshot,
            hasPendingOperations: true,
            close: async () => events.push('journal-close'),
        });
        journal.remove = async (_directory, id) => events.push(`wal-remove:${id}`);
        try {
            const prepared = await vfs.prepareDocumentRecoveryById(documentId);
            assert.strictEqual(prepared.visibleContent, snapshot.content);
            assert.deepStrictEqual(prepared.pendingOperationIds, snapshot.pendingOperationIds);
            events.push('durable-archive');

            await vfs.commitDiscardDocumentRecovery(prepared.token);

            assert(events.indexOf('durable-archive') < events.indexOf(`wal-remove:${documentId}`));
            assert(events.includes(`legacy-remove:${documentId}`));
            assert.strictEqual(vfs.documentSessions.get(replacement._id) !== undefined, true);
            assert(!events.includes('replacement-close'));
            assert.strictEqual(vfs.documentRecoveryFences.has(documentId), false);
        } finally {
            journal.openExisting = originalOpenExisting;
            journal.remove = originalRemove;
        }
    });

    it('keeps a pending WAL when its content changes after preparation', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-prepare-changed';
        vfs.legacyDocumentStateStore = {
            load: async () => undefined,
            remove: async () => events.push('legacy-remove'),
        };
        vfs.socket.leaveDoc = async () => {};
        const documentId = 'changed-after-prepare-doc';
        let opens = 0;

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        const originalRemove = journal.remove;
        journal.openExisting = async () => {
            const changed = opens++ > 0;
            return {
                content: changed ? 'new visible content' : 'archived visible content',
                acknowledgedContent: 'acknowledged content',
                revision: 2,
                pendingOperationIds: ['pending-op'],
                close: async () => {},
            };
        };
        journal.remove = async () => events.push('wal-remove');
        try {
            const prepared = await vfs.prepareDocumentRecoveryById(documentId);
            await assert.rejects(
                vfs.commitDiscardDocumentRecovery(prepared.token),
                /content changed after recovery preparation/,
            );

            assert(!events.includes('wal-remove'));
            assert(!events.includes('legacy-remove'));
            assert.strictEqual(vfs.documentRecoveryFences.has(documentId), false);
        } finally {
            journal.openExisting = originalOpenExisting;
            journal.remove = originalRemove;
        }
    });

    it('waits for late server acceptance before closing and removing the WAL', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-late-acceptance';
        vfs.legacyDocumentStateStore = {remove: async () => events.push('legacy-remove')};
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const doc = {_id: 'late-acceptance-doc', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        const transport = deferred();
        let pending = true;
        const session = {
            waitForTransportIdle: async () => {
                events.push('transport-wait');
                await transport.promise;
                events.push('transport-idle');
            },
            get hasPendingOperations() { return pending; },
            close: async () => { events.push('session-close'); },
        };
        vfs.documentSessions.set(doc._id, Promise.resolve(session));

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalRemove = journal.remove;
        journal.remove = async () => events.push('wal-remove');
        try {
            const discarding = vfs.discardDocumentRecovery({path: '/main.tex'});
            await nextTurn();
            assert.deepStrictEqual(events, ['transport-wait']);
            pending = false;
            transport.resolve();
            await discarding;

            assert(events.indexOf('transport-idle') < events.indexOf('session-close'));
            assert(events.indexOf('session-close') < events.indexOf('wal-remove'));
            assert.strictEqual(vfs.documentSessions.has(doc._id), false);
        } finally {
            journal.remove = originalRemove;
        }
    });

    it('admits only one VFS owner for a shared project document WAL namespace', async () => {
        const root = await mkdtemp(path.join(os.tmpdir(), 'hershel-vfs-wal-lock-'));
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const options = {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
            context: {globalStorageUri: {fsPath: root}},
        };
        const first = createVfs(VirtualFileSystem, options, events);
        const second = createVfs(VirtualFileSystem, options, events);
        try {
            await first.acquireDocumentJournalOwnership();
            await assert.rejects(
                second.acquireDocumentJournalOwnership(),
                /already owned by process/,
            );
            await first.releaseDocumentJournalOwnership();
            await second.acquireDocumentJournalOwnership();
            await second.releaseDocumentJournalOwnership();
        } finally {
            await first.releaseDocumentJournalOwnership();
            await second.releaseDocumentJournalOwnership();
            await rm(root, {recursive: true, force: true});
        }
    });

    it('requires the project WAL lease before discarding recovery state', async () => {
        const root = await mkdtemp(path.join(os.tmpdir(), 'hershel-vfs-wal-remove-lock-'));
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const options = {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
            context: {globalStorageUri: {fsPath: root}},
        };
        const owner = createVfs(VirtualFileSystem, options, events);
        const contender = createVfs(VirtualFileSystem, options, events);
        const doc = {_id: 'shared-doc', name: 'main.tex'};
        contender._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        contender.documentJournalDirectory = path.join(root, 'journals');
        contender.legacyDocumentStateStore = {remove: async () => {}};
        contender.socket.leaveDoc = async () => {};

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalRemove = journal.remove;
        let removeCount = 0;
        journal.remove = async () => { removeCount++; };
        try {
            await owner.acquireDocumentJournalOwnership();
            await assert.rejects(
                contender.discardDocumentRecovery({path: '/main.tex'}),
                /already owned by process/,
            );
            assert.strictEqual(removeCount, 0);
            assert.strictEqual(contender.documentRecoveryFences.has(doc._id), true);

            await owner.releaseDocumentJournalOwnership();
            await contender.discardDocumentRecovery({path: '/main.tex'});
            assert.strictEqual(removeCount, 1);
            assert.strictEqual(contender.documentRecoveryFences.has(doc._id), false);
        } finally {
            journal.remove = originalRemove;
            await owner.releaseDocumentJournalOwnership();
            await contender.releaseDocumentJournalOwnership();
            await rm(root, {recursive: true, force: true});
        }
    });

    it('preserves a receipt that journals while conflict discard is fencing the document', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-admission-race';
        vfs.legacyDocumentStateStore = {remove: async () => events.push('legacy-remove')};
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const doc = {_id: 'admission-doc', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        vfs._resolveById = () => ({path: '/main.tex'});

        const commitGate = deferred();
        const commitStarted = deferred();
        let closeCount = 0;
        const session = {
            bindPath: async () => {},
            acceptSnapshotAgainstAncestor: async () => {
                events.push('accept-started');
                commitStarted.resolve();
                await commitGate.promise;
                events.push('journal-receipt');
                return {
                    opId: 'late-durable-op',
                    content: 'aLb',
                    acknowledgement: Promise.resolve('aLb'),
                };
            },
            close: async () => { closeCount++; },
        };
        vfs.documentSessions.set(doc._id, Promise.resolve(session));

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalRemove = journal.remove;
        let removeCount = 0;
        journal.remove = async () => { removeCount++; };
        try {
            const accepting = vfs.acceptDocumentSnapshotAgainstAncestor(
                {path: '/main.tex'},
                'ab',
                'aLb',
                true,
            );
            await commitStarted.promise;
            const discarding = vfs.discardDocumentRecovery({path: '/main.tex'});
            await nextTurn();
            assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), true);
            commitGate.resolve();

            const accepted = await accepting;
            assert.strictEqual(accepted.opId, 'late-durable-op');
            assert.strictEqual(await accepted.acknowledgement, 'aLb');
            await assert.rejects(discarding, /Document changed while conflict resolution was fencing/);
            assert.strictEqual(removeCount, 0);
            assert.strictEqual(closeCount, 0);
            assert.strictEqual(vfs.documentSessions.has(doc._id), true);
            assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), false);
            assert.strictEqual(vfs.documentJournalEpochs.get(doc._id), 1);
        } finally {
            journal.remove = originalRemove;
        }
    });

    it('waits for a detached session owner to close before removing its WAL', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-detached-close';
        vfs.legacyDocumentStateStore = {remove: async () => events.push('legacy-remove')};
        vfs.socket.leaveDoc = async () => events.push('leave-doc');
        const doc = {_id: 'detached-doc', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});

        const closeGate = deferred();
        const detached = vfs.trackDocumentSessionClose(Promise.resolve({
            close: async () => {
                events.push('detached-close-started');
                await closeGate.promise;
                events.push('detached-closed');
            },
        }));
        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalRemove = journal.remove;
        journal.remove = async () => events.push('wal-remove');
        try {
            const discarding = vfs.discardDocumentRecovery({path: '/main.tex'});
            await nextTurn();
            assert(events.includes('detached-close-started'));
            assert(!events.includes('wal-remove'));

            closeGate.resolve();
            await Promise.all([detached, discarding]);
            assert(events.indexOf('detached-closed') < events.indexOf('wal-remove'));
        } finally {
            journal.remove = originalRemove;
        }
    });
    it('enumerates pending legacy OT state that has not migrated to the v2 journal', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/no-v2-journals';
        vfs.legacyDocumentStateStore = {
            listExisting: async () => [{
                schemaVersion: 1,
                documentId: 'legacy-doc',
                updatedAt: new Date(0).toISOString(),
                state: {
                    schemaVersion: 1,
                    revision: 7,
                    serverContent: 'BASE',
                    localContent: 'BASE LOCAL',
                    outstanding: {ids: ['legacy-op'], operation: []},
                },
            }],
        };
        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalListExisting = journal.listExisting;
        journal.listExisting = async () => [];
        try {
            const recoveries = await vfs.inspectPendingDocumentRecoveries();
            assert.strictEqual(recoveries.length, 1);
            assert.strictEqual(recoveries[0].documentId, 'legacy-doc');
            assert.strictEqual(recoveries[0].visibleContent, 'BASE LOCAL');
            assert.strictEqual(recoveries[0].acknowledgedContent, 'BASE');
            assert.deepStrictEqual(recoveries[0].pendingOperationIds, ['legacy-op']);
            assert.match(recoveries[0].hash, /^[a-f0-9]{64}$/);
        } finally {
            journal.listExisting = originalListExisting;
        }
    });

    it('enumerates the durable path bound to a pending v3 document WAL', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const directory = await mkdtemp(path.join(os.tmpdir(), 'safe-sync-path-recovery-'));
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = directory;
        vfs.legacyDocumentStateStore = {listExisting: async () => []};
        const {DocumentJournal} = require('../out/sync/documentJournal');
        const {TextOperation} = require('../out/sync/textOperation');
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'path-doc',
            initialRevision: 0,
            initialContent: 'base',
            initialPath: 'sections/main.tex',
        });
        await journal.commitLocalOperation(
            TextOperation.fromDiff('base', 'base local'),
            'path-op',
            true,
        );
        await journal.close();
        try {
            const recoveries = await vfs.inspectPendingDocumentRecoveries();
            assert.strictEqual(recoveries.length, 1);
            assert.strictEqual(recoveries[0].documentId, 'path-doc');
            assert.strictEqual(recoveries[0].lastKnownPath, 'sections/main.tex');
            assert.deepStrictEqual(recoveries[0].pendingOperationIds, ['path-op']);
        } finally {
            await rm(directory, {recursive: true, force: true});
        }
    });

    it('does not enumerate a path-bound document WAL without pending operations', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const directory = await mkdtemp(path.join(os.tmpdir(), 'safe-sync-settled-recovery-'));
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = directory;
        vfs.legacyDocumentStateStore = {listExisting: async () => []};
        const {DocumentJournal} = require('../out/sync/documentJournal');
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'settled-doc',
            initialRevision: 0,
            initialContent: 'base',
            initialPath: 'sections/main.tex',
        });
        await journal.close();
        try {
            assert.deepStrictEqual(await vfs.inspectPendingDocumentRecoveries(), []);
        } finally {
            await rm(directory, {recursive: true, force: true});
        }
    });

    it('retires a deleted document generation on reconnect without poisoning project readiness', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.root = {rootFolder: [{_id: 'root', docs: [], fileRefs: [], folders: []}]};
        vfs.connectionReady = true;
        vfs.documentSessionFailures.set('deleted-doc', new Error('old reconnect failure'));
        vfs.documentPendingOperations.set('deleted-doc', false);
        let closed = 0;
        const session = {
            hasPendingOperations: false,
            close: async () => { closed++; },
        };
        vfs.documentSessions.set('deleted-doc', Promise.resolve(session));
        vfs.trackDocumentSessionClose = async pending => (await pending).close();
        vfs.socket.joinDoc = async () => {
            throw new Error('joinDoc must not run for a deleted immutable id');
        };

        await vfs.resumeDocumentSessionTransports();

        assert.strictEqual(closed, 1);
        assert.strictEqual(vfs.documentSessions.has('deleted-doc'), false);
        assert.strictEqual(vfs.documentSessionFailures.has('deleted-doc'), false);
        assert.strictEqual(vfs.documentPendingOperations.has('deleted-doc'), false);
        assert.strictEqual(vfs.isSafeSyncConnectionReady, true);
    });
});

describe('VirtualFileSystem remote resource safety', () => {
    function folder(id, name) {
        return {_id: id, name, _type: 'folder', docs: [], fileRefs: [], folders: []};
    }

    function resourceVfs(events) {
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({token: 'identity'}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const root = folder('root', 'Project');
        vfs.root = {rootFolder: [root]};
        vfs.readFreshProjectSnapshot = async () => ({rootFolder: [root]});
        vfs.connectionReady = true;
        vfs.notify = changes => events.push(...changes.map(change => `notify:${change.type}`));
        return {vfs, root};
    }

    function remoteUri(filePath) {
        return {
            path: `/Project/${filePath}`,
            query: 'user=test-user&project=test-project',
            authority: 'test-server',
        };
    }

    function projectEntities(root) {
        const entities = [];
        const visit = (current, prefix) => {
            for (const document of current.docs) {
                entities.push({path: `${prefix}${document.name}`, type: 'doc'});
            }
            for (const file of current.fileRefs) {
                entities.push({path: `${prefix}${file.name}`, type: 'file'});
            }
            for (const child of current.folders) {
                const childPath = `${prefix}${child.name}`;
                entities.push({path: childPath, type: 'folder'});
                visit(child, `${childPath}/`);
            }
        };
        visit(root, '');
        return entities;
    }

    it('keeps absent startup verification read-only, then atomically promotes owned staging', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        let uploaded;
        let addFolderCalls = 0;
        let uploadCalls = 0;
        const mutations = [];
        vfs.api = {
            addFolder: async (_identity, _project, name) => {
                addFolderCalls++;
                mutations.push(`add:${name}`);
                return {type: 'success', entity: folder(`folder-${addFolderCalls}`, name)};
            },
            uploadFile: async (_identity, _project, _parent, name, content) => {
                uploadCalls++;
                uploaded = content;
                mutations.push(`upload:${name}`);
                return {type: 'success', entity: {_id: 'owner-file', name, _type: 'file'}};
            },
            getFile: async () => ({type: 'success', content: uploaded}),
            renameEntity: async (_identity, _project, type, entityId, name) => {
                const staging = root.folders.find(item => item._id === entityId);
                assert.strictEqual(type, 'folder');
                assert.strictEqual(staging.fileRefs[0].name, '.owner.json');
                mutations.push(`rename:${name}`);
                return {type: 'success'};
            },
        };

        await vfs.ensureSafeSyncRemoteNamespace();
        assert.strictEqual(addFolderCalls, 0);
        assert.strictEqual(uploadCalls, 0);
        assert.deepStrictEqual(root.folders, []);

        await vfs.getOwnedRemoteSafeSyncNamespace();
        const marker = JSON.parse(new TextDecoder().decode(uploaded));
        const {ELEGANT_NAME, ROOT_NAME} = require('../out/extensionIdentity');
        assert.strictEqual(marker.schemaVersion, 1);
        assert.strictEqual(marker.projectId, vfs.projectId);
        assert.strictEqual(marker.extensionId, ROOT_NAME);
        assert.strictEqual(marker.extensionName, ELEGANT_NAME);
        assert.match(marker.namespaceId, /^[0-9a-f-]{36}$/i);
        assert.strictEqual(root.folders[0].name, '.hershel-safe-sync');
        assert.match(mutations[0], /^add:\.hershel-safe-sync\.staging-[0-9a-f-]{36}$/i);
        assert.deepStrictEqual(mutations.slice(1), [
            'upload:.owner.json',
            'rename:.hershel-safe-sync',
        ]);

        await vfs.ensureSafeSyncRemoteNamespace();

        assert.strictEqual(addFolderCalls, 1);
        assert.strictEqual(uploadCalls, 1);
    });

    it('recovers only staging whose marker proves ownership and matches its name', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const namespaceId = '550e8400-e29b-41d4-a716-446655440000';
        const recoverable = folder(
            'recoverable',
            `.hershel-safe-sync.staging-${namespaceId}`,
        );
        recoverable.fileRefs.push({_id: 'marker', name: '.owner.json', _type: 'file'});
        root.folders.push(recoverable);
        const {ELEGANT_NAME, ROOT_NAME} = require('../out/extensionIdentity');
        const marker = new TextEncoder().encode(JSON.stringify({
            schemaVersion: 1,
            projectId: vfs.projectId,
            extensionId: ROOT_NAME,
            extensionName: ELEGANT_NAME,
            namespaceId,
        }));
        const mutations = [];
        vfs.api = {
            getFile: async (_identity, _project, entityId) => {
                assert.strictEqual(entityId, 'marker');
                return {type: 'success', content: marker};
            },
            addFolder: async () => {
                throw new Error('valid staging must be recovered');
            },
            uploadFile: async () => {
                throw new Error('valid staging marker must be reused');
            },
            renameEntity: async (_identity, _project, type, entityId, name) => {
                mutations.push([type, entityId, name]);
                return {type: 'success'};
            },
        };

        await vfs.ensureSafeSyncRemoteNamespace();
        assert.deepStrictEqual(mutations, []);
        await vfs.getOwnedRemoteSafeSyncNamespace();

        assert.deepStrictEqual(mutations, [[
            'folder', 'recoverable', '.hershel-safe-sync',
        ]]);
        assert.strictEqual(recoverable.name, '.hershel-safe-sync');
    });

    it('fails closed instead of hiding an unowned staging-shaped root folder', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        root.folders.push(folder(
            'unowned',
            '.hershel-safe-sync.staging-550e8400-e29b-41d4-a716-446655440000',
        ));
        vfs.api = {
            getFile: async () => {
                throw new Error('an unmarked staging folder has no readable owner');
            },
        };

        await assert.rejects(
            vfs.ensureSafeSyncRemoteNamespace(),
            /unowned or ambiguous remote safe-sync staging folder/,
        );
    });

    it('never publishes the final namespace when owner marker upload fails', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        let renameCalls = 0;
        vfs.api = {
            addFolder: async (_identity, _project, name) => ({
                type: 'success', entity: folder('staging', name),
            }),
            uploadFile: async () => {
                throw new Error('simulated marker upload failure');
            },
            renameEntity: async () => {
                renameCalls++;
                return {type: 'success'};
            },
        };

        await assert.rejects(
            vfs.getOwnedRemoteSafeSyncNamespace(),
            /simulated marker upload failure/,
        );

        assert.strictEqual(renameCalls, 0);
        assert.match(root.folders[0].name, /^\.hershel-safe-sync\.staging-/);
        assert.strictEqual(root.folders.some(item => item.name === '.hershel-safe-sync'), false);
    });

    it('fails closed when an existing remote safe-sync folder has no owner marker', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        root.folders.push(folder('user-folder', '.hershel-safe-sync'));
        let mutations = 0;
        vfs.api = {
            addFolder: async () => { mutations++; throw new Error('must not mutate'); },
            uploadFile: async () => { mutations++; throw new Error('must not mutate'); },
        };

        await assert.rejects(
            vfs.ensureSafeSyncRemoteNamespace(),
            /Refusing to reuse an unowned remote \.hershel-safe-sync directory/,
        );
        assert.strictEqual(mutations, 0);
    });

    it('adopts only an explicit initialized legacy history/trash namespace', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const internal = folder('legacy-internal', '.hershel-safe-sync');
        internal.folders.push(folder('legacy-history', 'history'));
        root.folders.push(internal);
        let uploaded;
        vfs.api = {
            uploadFile: async (_identity, _project, _parent, name, content) => {
                uploaded = content;
                return {type: 'success', entity: {_id: 'owner-file', name, _type: 'file'}};
            },
            getFile: async () => ({type: 'success', content: uploaded}),
        };
        const replicaId = '550e8400-e29b-41d4-a716-446655440000';

        await vfs.ensureSafeSyncRemoteNamespace(replicaId);
        await vfs.ensureSafeSyncRemoteNamespace();

        const marker = JSON.parse(new TextDecoder().decode(uploaded));
        assert.strictEqual(marker.namespaceId, replicaId);
        assert.strictEqual(internal.fileRefs[0].name, '.owner.json');
    });

    it('fails closed when the remote namespace marker belongs to another project', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const internal = folder('internal', '.hershel-safe-sync');
        internal.fileRefs.push({_id: 'owner-file', name: '.owner.json', _type: 'file'});
        root.folders.push(internal);
        const marker = new TextEncoder().encode(JSON.stringify({
            schemaVersion: 1,
            projectId: 'another-project',
            extensionId: 'hershel.overleaf-workshop-safe-sync',
            extensionName: 'Hershel Overleaf Workshop Safe Sync',
            namespaceId: '550e8400-e29b-41d4-a716-446655440000',
        }));
        vfs.api = {getFile: async () => ({type: 'success', content: marker})};

        await assert.rejects(
            vfs.ensureSafeSyncRemoteNamespace(),
            /ownership marker does not match this project and extension/,
        );
    });

    it('moves and renames only the proven immutable document', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const sections = folder('sections', 'sections');
        const document = {_id: 'doc-1', name: 'main.tex', _type: 'doc'};
        root.docs.push(document);
        root.folders.push(sections);
        const content = new TextEncoder().encode('baseline text');
        const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
        const mutations = [];
        vfs.documentSessions.set('doc-1', Promise.resolve({bindPath: async () => {}}));
        vfs.readAuthoritativeFile = async uri => {
            const resolved = await vfs._resolveUri(uri);
            return {
                entityId: resolved.fileEntity._id,
                fileType: resolved.fileType,
                content,
            };
        };
        vfs.api = {
            projectEntitiesJson: async () => ({
                type: 'success', entities: projectEntities(root),
            }),
            renameEntity: async (_identity, _project, type, entityId, name) => {
                mutations.push(['rename', type, entityId, name]);
                return {type: 'success'};
            },
            moveEntity: async (_identity, _project, type, entityId, parentId) => {
                mutations.push(['move', type, entityId, parentId]);
                return {type: 'success'};
            },
        };

        await vfs.moveResourceAgainstBaseline(
            remoteUri('main.tex'),
            remoteUri('sections/renamed.tex'),
            'doc-1',
            'doc',
            expectedHash,
        );

        assert.deepStrictEqual(mutations, [
            ['rename', 'doc', 'doc-1', 'renamed.tex'],
            ['move', 'doc', 'doc-1', 'sections'],
        ]);
        assert.strictEqual(root.docs.length, 0);
        assert.strictEqual(sections.docs[0], document);
        assert.strictEqual(document.name, 'renamed.tex');
        assert.deepStrictEqual(events.slice(-2), ['notify:3', 'notify:1']);
    });

    it('waits for a fresh realtime project snapshot to observe an acknowledged move', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const sections = folder('sections', 'sections');
        const document = {_id: 'doc-eventual', name: 'main.tex', _type: 'doc'};
        root.docs.push(document);
        root.folders.push(sections);
        const content = new TextEncoder().encode('eventual baseline');
        const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
        const staleProject = {
            rootFolder: [JSON.parse(JSON.stringify(root))],
        };
        let moved = false;
        let stalePostMutationReads = 0;
        vfs.documentSessions.set(document._id, Promise.resolve({bindPath: async () => {}}));
        vfs.readAuthoritativeFile = async uri => {
            const resolved = await vfs._resolveUri(uri);
            return {
                entityId: resolved.fileEntity._id,
                fileType: resolved.fileType,
                content,
            };
        };
        vfs.readFreshProjectSnapshot = async () => {
            if (moved && stalePostMutationReads < 2) {
                    stalePostMutationReads++;
                return staleProject;
            }
            return {rootFolder: [root]};
        };
        vfs.api = {
            projectEntitiesJson: async () => ({
                type: 'success', entities: projectEntities(staleProject.rootFolder[0]),
            }),
            moveEntity: async () => {
                moved = true;
                return {type: 'success'};
            },
        };

        await vfs.moveResourceAgainstBaseline(
            remoteUri('main.tex'),
            remoteUri('sections/main.tex'),
            document._id,
            'doc',
            expectedHash,
        );

        assert.strictEqual(stalePostMutationReads, 2);
        assert.strictEqual(root.docs.length, 0);
        assert.strictEqual(sections.docs[0], document);
    });

    it('recursively deletes through per-entity recovery fences instead of deleteEntity', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const fixture = folder('fixture', 'fixture');
        const nested = folder('nested', 'nested');
        const document = {_id: 'doc-1', name: 'main.tex', _type: 'doc'};
        const nestedDocument = {_id: 'doc-2', name: 'appendix.tex', _type: 'doc'};
        const binary = {_id: 'file-1', name: 'figure.png', _type: 'file'};
        fixture.docs.push(document);
        fixture.fileRefs.push(binary);
        nested.docs.push(nestedDocument);
        fixture.folders.push(nested);
        root.folders.push(fixture);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileType: 'folder',
            fileEntity: fixture,
        });
        vfs.pathToUri = (...parts) => ({path: `/${parts.filter(Boolean).join('/')}`});
        vfs.readAuthoritativeFileWithoutRecovery = async uri => {
            const resolved = vfs.walk(entity => (
                entity._type === 'doc' || entity._type === 'file'
            )).find(candidate => candidate.path === uri.path);
            assert(resolved, `missing fixture leaf ${uri.path}`);
            return {
                entityId: resolved.entity._id,
                fileType: resolved.entity._type,
                content: new TextEncoder().encode(resolved.entity.name),
            };
        };
        const deleteCalls = [];
        vfs.api.deleteEntity = async () => {
            deleteCalls.push('destructive-delete');
            return {type: 'success'};
        };
        const retire = (entityId, fileType) => {
            const resolved = vfs._resolveById(entityId);
            assert(resolved);
            let parent = resolved.parentFolder;
            if (fileType === 'folder') {
                const folders = [root];
                for (let index = 0; index < folders.length; index++) {
                    const candidate = folders[index];
                    if (candidate.folders.some(child => child._id === entityId)) {
                        parent = candidate;
                        break;
                    }
                    folders.push(...candidate.folders);
                }
            }
            vfs.removeEntity(parent, fileType, resolved.fileEntity);
            events.push(`retire:${fileType}:${entityId}`);
        };
        vfs.removeDocumentAgainstBaseline = async (_uri, entityId) => retire(entityId, 'doc');
        vfs.removeResourceAgainstBaseline = async (_uri, entityId) => retire(entityId, 'file');
        vfs.removeFolderAgainstBaseline = async (uri, entityId, expectedHash) => {
            assert.strictEqual(expectedHash, require('../out/core/remoteFileSystemProvider')
                .EMPTY_REMOTE_FOLDER_TREE_HASH);
            assert(!uri.path.endsWith('/'), `folder retirement path was not canonical: ${uri.path}`);
            const resolved = vfs._resolveById(entityId);
            assert(resolved);
            assert.strictEqual(resolved.fileEntity.docs.length, 0);
            assert.strictEqual(resolved.fileEntity.fileRefs.length, 0);
            assert.strictEqual(resolved.fileEntity.folders.length, 0);
            retire(entityId, 'folder');
        };

        await vfs.removeWithRecoverySafety(remoteUri('fixture'), true);

        assert.deepStrictEqual(deleteCalls, []);
        assert.strictEqual(root.folders.includes(fixture), false);
        assert(events.indexOf('retire:doc:doc-2') < events.indexOf('retire:folder:nested'));
        assert.strictEqual(events.at(-1), 'retire:folder:fixture');
    });

    it('rejects stale source bytes and an authoritatively occupied destination before mutation', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const document = {_id: 'doc-1', name: 'main.tex', _type: 'doc'};
        root.docs.push(document);
        const baseline = new TextEncoder().encode('baseline');
        const changed = new TextEncoder().encode('changed remotely');
        const baselineHash = require('crypto').createHash('sha256').update(baseline).digest('hex');
        let mutations = 0;
        const occupiedRoot = folder('occupied-root', 'Project');
        occupiedRoot.docs.push(
            {...document},
            {_id: 'occupied-doc', name: 'renamed.tex', _type: 'doc'},
        );
        vfs.readFreshProjectSnapshot = async () => ({rootFolder: [occupiedRoot]});
        vfs.api = {
            projectEntitiesJson: async () => ({
                type: 'success',
                entities: [
                    {path: 'main.tex', type: 'doc'},
                    {path: 'renamed.tex', type: 'doc'},
                ],
            }),
            renameEntity: async () => { mutations++; return {type: 'success'}; },
        };
        vfs.readAuthoritativeFile = async () => ({
            entityId: 'doc-1', fileType: 'doc', content: changed,
        });

        await assert.rejects(
            vfs.moveResourceAgainstBaseline(
                remoteUri('main.tex'), remoteUri('renamed.tex'),
                'doc-1', 'doc', baselineHash,
            ),
            /Remote resource changed/,
        );
        assert.strictEqual(mutations, 0);

        vfs.readFreshProjectSnapshot = async () => ({rootFolder: [root]});
        await assert.rejects(
            vfs.moveResourceAgainstBaseline(
                remoteUri('main.tex'), remoteUri('renamed.tex'),
                'doc-1', 'doc', baselineHash,
            ),
            /Remote resource changed/,
        );
        assert.strictEqual(mutations, 0);
    });

    it('rolls back a cross-folder rename when the following move fails', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const sections = folder('sections', 'sections');
        const document = {_id: 'doc-1', name: 'main.tex', _type: 'doc'};
        root.docs.push(document);
        root.folders.push(sections);
        const content = new TextEncoder().encode('baseline');
        const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
        const mutations = [];
        vfs.readAuthoritativeFile = async () => ({
            entityId: 'doc-1', fileType: 'doc', content,
        });
        vfs.api = {
            projectEntitiesJson: async () => ({
                type: 'success', entities: projectEntities(root),
            }),
            renameEntity: async (_identity, _project, _type, _entityId, name) => {
                mutations.push(`rename:${name}`);
                return {type: 'success'};
            },
            moveEntity: async () => {
                mutations.push('move:failed');
                return {type: 'error', message: 'simulated move failure'};
            },
        };

        await assert.rejects(
            vfs.moveResourceAgainstBaseline(
                remoteUri('main.tex'), remoteUri('sections/renamed.tex'),
                'doc-1', 'doc', expectedHash,
            ),
            /simulated move failure/,
        );

        assert.deepStrictEqual(mutations, [
            'rename:renamed.tex',
            'move:failed',
            'rename:main.tex',
        ]);
        assert.strictEqual(document.name, 'main.tex');
        assert.strictEqual(root.docs[0], document);
        assert.strictEqual(sections.docs.length, 0);
    });

    it('verifies the same folder structure hash before and after a move', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const source = folder('source', 'draft');
        source.docs.push({_id: 'child-doc', name: 'part.tex', _type: 'doc'});
        const destination = folder('destination', 'sections');
        root.folders.push(source, destination);
        const expectedHash = require('crypto').createHash('sha256')
            .update(JSON.stringify([{path: 'part.tex', type: 'doc'}]))
            .digest('hex');
        const mutations = [];
        vfs.api = {
            projectEntitiesJson: async () => ({
                type: 'success', entities: projectEntities(root),
            }),
            renameEntity: async (_identity, _project, type, entityId, name) => {
                mutations.push(['rename', type, entityId, name]);
                return {type: 'success'};
            },
            moveEntity: async (_identity, _project, type, entityId, parentId) => {
                mutations.push(['move', type, entityId, parentId]);
                return {type: 'success'};
            },
        };

        await vfs.moveResourceAgainstBaseline(
            remoteUri('draft'), remoteUri('sections/final'),
            'source', 'folder', expectedHash,
        );

        assert.deepStrictEqual(mutations, [
            ['rename', 'folder', 'source', 'final'],
            ['move', 'folder', 'source', 'destination'],
        ]);
        assert.strictEqual(destination.folders[0], source);
        assert.strictEqual(source.name, 'final');
    });

    it('retries a transient document-session factory rejection instead of pinning it', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs.documentJournalDirectory = '/tmp/safe-sync-transient-factory';
        vfs.legacyDocumentStateStore = {load: async () => undefined};
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.socket.isUsingAlternativeConnectionScheme = false;
        let joins = 0;
        vfs.socket.joinDoc = async () => {
            joins++;
            if (joins <= 2) {
                throw new Error(`transient-${joins}`);
            }
            return {
                otType: 'sharejs-text-ot',
                docLines: ['reopened'],
                version: 1,
                updates: [],
            };
        };

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const durable = require('../out/sync/durableDocumentSession').DurableDocumentSession;
        const originalOpenExisting = journal.openExisting;
        const originalCreate = durable.create;
        let closes = 0;
        journal.openExisting = async () => ({
            revision: 1,
            bindPath: async () => {},
            close: async () => { closes++; },
        });
        durable.create = async () => ({isFailed: false});
        try {
            const doc = {_id: 'retry-doc', name: 'main.tex'};
            await assert.rejects(vfs.getDocumentSession(doc, '/main.tex'), /transient-1/);
            assert.strictEqual(vfs.documentSessions.has(doc._id), false);
            assert.match(vfs.documentSessionFailures.get(doc._id).message, /transient-1/);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, false);
            await assert.rejects(vfs.getDocumentSession(doc, '/main.tex'), /transient-2/);
            assert.match(vfs.documentSessionFailures.get(doc._id).message, /transient-2/);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, false);

            const reopened = await vfs.getDocumentSession(doc, '/main.tex');

            assert.strictEqual(reopened.isFailed, false);
            assert.strictEqual(vfs.documentSessionFailures.has(doc._id), false);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, true);
            assert.strictEqual(joins, 3);
            assert.strictEqual(closes, 2);
        } finally {
            journal.openExisting = originalOpenExisting;
            durable.create = originalCreate;
        }
    });

    it('isolates a failed runtime generation before reopening the same document id', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        vfs.connectionReady = true;
        vfs.documentJournalDirectory = '/tmp/safe-sync-runtime-failure';
        vfs.legacyDocumentStateStore = {load: async () => undefined};
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.socket.isUsingAlternativeConnectionScheme = false;
        vfs.socket.joinDoc = async () => ({
            otType: 'sharejs-text-ot',
            docLines: ['server'],
            version: 1,
            updates: [],
        });
        const doc = {_id: 'runtime-doc', name: 'main.tex'};
        vfs._resolveUri = async () => ({fileType: 'doc', fileEntity: doc});
        vfs._resolveById = () => ({path: '/main.tex'});

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const durable = require('../out/sync/durableDocumentSession').DurableDocumentSession;
        const originalOpenExisting = journal.openExisting;
        const originalCreate = durable.create;
        const generations = [];
        let closeCount = 0;
        journal.openExisting = async () => ({
            revision: 1,
            bindPath: async () => {},
            close: async () => {},
        });
        durable.create = async options => {
            let failed = false;
            const generation = {
                get isFailed() { return failed; },
                content: `generation-${generations.length + 1}`,
                revision: 1,
                suspendTransport() {},
                bindPath: async () => {},
                close: async () => { closeCount++; },
                acceptSnapshotAgainstAncestor: async (_ancestor, target) => ({
                    content: target,
                    acknowledgement: Promise.resolve(target),
                }),
                fail(error) {
                    failed = true;
                    options.onError(error);
                },
            };
            generations.push(generation);
            return generation;
        };
        try {
            await vfs.getDocumentSession(doc, '/main.tex');
            generations[0].fail(new Error('runtime OT invariant failed'));

            assert.strictEqual(vfs.isSafeSyncConnectionReady, false);
            await assert.rejects(
                vfs.readDocumentProjection({path: '/main.tex'}, doc._id),
                /runtime OT invariant failed/,
            );
            assert.strictEqual(closeCount, 1);
            assert.strictEqual(vfs.documentSessions.has(doc._id), false);
            assert.strictEqual(vfs.documentSessionFailures.has(doc._id), true);

            const reopened = await vfs.readDocumentProjection({path: '/main.tex'}, doc._id);

            assert.strictEqual(reopened.content, 'generation-2');
            assert.strictEqual(reopened.entityId, doc._id);
            assert.strictEqual(vfs.documentSessionFailures.has(doc._id), false);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, true);

            generations[1].fail(new Error('second runtime failure'));
            await assert.rejects(
                vfs.acceptDocumentSnapshotAgainstAncestor(
                    {path: '/main.tex'},
                    'server',
                    'target',
                    true,
                    doc._id,
                ),
                /second runtime failure/,
            );
            const accepted = await vfs.acceptDocumentSnapshotAgainstAncestor(
                {path: '/main.tex'},
                'server',
                'target',
                true,
                doc._id,
            );
            assert.strictEqual(accepted.content, 'target');
            assert.strictEqual(generations.length, 3);
            assert.strictEqual(vfs.documentSessionFailures.has(doc._id), false);
            assert.strictEqual(vfs.isSafeSyncConnectionReady, true);
        } finally {
            journal.openExisting = originalOpenExisting;
            durable.create = originalCreate;
        }
    });

    it('creates an empty binary through the explicit resource upload path', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: 'empty.bin',
            fileEntity: undefined,
            fileType: undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.assertAuthoritativePathAbsent = async () => {};
        let addDocCalls = 0;
        let explicitMimeType;
        vfs.api = {
            addDoc: async () => { addDocCalls++; return {type: 'success'}; },
            uploadFile: async (_identity, _project, _parent, name, content, mimeType) => {
                explicitMimeType = mimeType;
                assert.strictEqual(content.byteLength, 0);
                return {type: 'success', entity: {_id: 'empty-file', name, _type: 'file'}};
            },
            getFile: async () => ({type: 'success', content: new Uint8Array(0)}),
            moveEntity: async () => ({type: 'success'}),
        };

        const created = await vfs.createTypedFile(
            {path: '/Project/empty.bin'},
            new Uint8Array(0),
            'file',
        );

        assert.strictEqual(created.entityId, 'empty-file');
        assert.strictEqual(addDocCalls, 0);
        assert.strictEqual(explicitMimeType, 'application/octet-stream');
        assert.strictEqual(root.fileRefs[0]._id, 'empty-file');
    });

    it('retains the previous binary in hidden history while installing the staged entity', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const oldContent = new Uint8Array([1, 2, 3]);
        const newContent = new Uint8Array([4, 5, 6]);
        const old = {_id: 'old-file', name: 'figure.png', _type: 'file'};
        root.fileRefs.push(old);
        const contents = new Map([[old._id, oldContent]]);
        const moves = [];
        vfs._resolveUri = async () => {
            const entity = root.fileRefs.find(item => item.name === 'figure.png');
            return {
                parentFolder: root,
                fileName: 'figure.png',
                fileEntity: entity,
                fileType: entity ? 'file' : undefined,
            };
        };
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.readAuthoritativeFile = async () => {
            const entity = root.fileRefs.find(item => item.name === 'figure.png');
            if (!entity) throw new Error('not found');
            return {fileType: 'file', entityId: entity._id, content: contents.get(entity._id)};
        };
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.fileRefs.some(item => item.name === 'figure.png'), false);
        };
        vfs.api = {
            uploadFile: async (_identity, _project, _parent, name, content, mimeType) => {
                assert.strictEqual(mimeType, 'application/octet-stream');
                contents.set('incoming-file', content);
                return {type: 'success', entity: {_id: 'incoming-file', name, _type: 'file'}};
            },
            getFile: async (_identity, _project, entityId) => ({
                type: 'success', content: contents.get(entityId),
            }),
            moveEntity: async (_identity, _project, _type, entityId, destinationId) => {
                moves.push([entityId, destinationId]);
                return {type: 'success'};
            },
        };

        const expectedHash = require('crypto').createHash('sha256').update(oldContent).digest('hex');
        const entityId = await vfs.writeResourceAgainstBaseline(
            {path: '/Project/figure.png'}, old._id, expectedHash, newContent,
        );

        assert.strictEqual(entityId, 'incoming-file');
        assert.deepStrictEqual(moves, [
            ['old-file', 'previous'],
            ['incoming-file', 'root'],
        ]);
        assert.strictEqual(previous.fileRefs[0]._id, 'old-file');
        assert.strictEqual(root.fileRefs[0]._id, 'incoming-file');
        assert.strictEqual(typeof vfs.api.deleteEntity, 'undefined');
    });

    it('keeps both binary candidates when the original path is occupied before install', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const oldContent = new Uint8Array([1]);
        const newContent = new Uint8Array([2]);
        const old = {_id: 'old-file', name: 'figure.png', _type: 'file'};
        root.fileRefs.push(old);
        const contents = new Map([[old._id, oldContent]]);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: 'figure.png',
            fileEntity: root.fileRefs.find(item => item.name === 'figure.png'),
            fileType: root.fileRefs.some(item => item.name === 'figure.png') ? 'file' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'file', entityId: old._id, content: oldContent,
        });
        vfs.api = {
            uploadFile: async (_identity, _project, _parent, name, content) => {
                contents.set('incoming-file', content);
                return {type: 'success', entity: {_id: 'incoming-file', name, _type: 'file'}};
            },
            getFile: async (_identity, _project, entityId) => ({
                type: 'success', content: contents.get(entityId),
            }),
            moveEntity: async () => ({type: 'success'}),
            projectEntitiesJson: async () => ({
                type: 'success', entities: [{path: 'figure.png', type: 'file'}],
            }),
        };
        const {ConcurrentRemoteResourceError} = require('../out/core/remoteFileSystemProvider');
        vfs.assertAuthoritativePathAbsent = async uri => {
            throw new ConcurrentRemoteResourceError(uri.path);
        };
        const expectedHash = require('crypto').createHash('sha256').update(oldContent).digest('hex');

        await assert.rejects(
            vfs.writeResourceAgainstBaseline(
                {path: '/Project/figure.png', query: '', authority: ''},
                old._id,
                expectedHash,
                newContent,
            ),
            /Remote resource changed/,
        );

        assert.strictEqual(previous.fileRefs[0]._id, 'old-file');
        assert.strictEqual(incoming.fileRefs[0]._id, 'incoming-file');
        assert.strictEqual(root.fileRefs.length, 0);
    });

    it('rolls a changed binary candidate back when replace finds the original path empty', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const oldContent = new Uint8Array([1]);
        const changedRemoteContent = new Uint8Array([9]);
        const newContent = new Uint8Array([2]);
        const old = {_id: 'old-file', name: 'figure.png', _type: 'file'};
        root.fileRefs.push(old);
        const contents = new Map([[old._id, oldContent]]);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: old.name,
            fileEntity: root.fileRefs.find(item => item._id === old._id),
            fileType: root.fileRefs.some(item => item._id === old._id) ? 'file' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'file', entityId: old._id, content: oldContent,
        });
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.fileRefs.length, 0);
        };
        vfs.api = {
            uploadFile: async (_identity, _project, _parent, name, content) => {
                contents.set('incoming-file', content);
                return {type: 'success', entity: {_id: 'incoming-file', name, _type: 'file'}};
            },
            getFile: async (_identity, _project, entityId) => ({
                type: 'success',
                content: entityId === old._id ? changedRemoteContent : contents.get(entityId),
            }),
            moveEntity: async () => ({type: 'success'}),
        };
        const expectedHash = require('crypto').createHash('sha256').update(oldContent).digest('hex');

        await assert.rejects(
            vfs.writeResourceAgainstBaseline(
                {path: '/Project/figure.png'}, old._id, expectedHash, newContent,
            ),
            /Remote resource changed/,
        );

        assert.strictEqual(incoming.fileRefs[0]._id, 'incoming-file');
        assert.strictEqual(previous.fileRefs.length, 0);
        assert.strictEqual(root.fileRefs[0]._id, old._id);
    });

    it('returns a typed retained-candidate error when replace rollback finds the path occupied', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const oldContent = new Uint8Array([1]);
        const changedRemoteContent = new Uint8Array([9]);
        const newContent = new Uint8Array([2]);
        const old = {_id: 'old-file', name: 'figure.png', _type: 'file'};
        root.fileRefs.push(old);
        const contents = new Map([[old._id, oldContent]]);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: old.name,
            fileEntity: root.fileRefs.find(item => item._id === old._id),
            fileType: root.fileRefs.some(item => item._id === old._id) ? 'file' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'file', entityId: old._id, content: oldContent,
        });
        vfs.api = {
            uploadFile: async (_identity, _project, _parent, name, content) => {
                contents.set('incoming-file', content);
                return {type: 'success', entity: {_id: 'incoming-file', name, _type: 'file'}};
            },
            getFile: async (_identity, _project, entityId) => ({
                type: 'success',
                content: entityId === old._id ? changedRemoteContent : contents.get(entityId),
            }),
            moveEntity: async () => ({type: 'success'}),
            projectEntitiesJson: async () => ({
                type: 'success', entities: [{path: 'figure.png', type: 'file'}],
            }),
        };
        const {ConcurrentRemoteResourceError} = require('../out/core/remoteFileSystemProvider');
        vfs.assertAuthoritativePathAbsent = async uri => {
            throw new ConcurrentRemoteResourceError(uri.path);
        };
        const expectedHash = require('crypto').createHash('sha256').update(oldContent).digest('hex');
        const archivedHash = require('crypto').createHash('sha256')
            .update(changedRemoteContent).digest('hex');

        const error = await vfs.writeResourceAgainstBaseline(
            {path: '/Project/figure.png', query: '', authority: ''},
            old._id,
            expectedHash,
            newContent,
        ).catch(value => value);

        const {RetainedRemoteCandidateError} = require('../out/core/remoteFileSystemProvider');
        assert(error instanceof RetainedRemoteCandidateError);
        assert.strictEqual(error.uri.path, '/Project/figure.png');
        assert.strictEqual(error.entityId, old._id);
        assert.strictEqual(error.archivedHash, archivedHash);
        assert.strictEqual(error.fileType, 'file');
        assert.strictEqual(previous.fileRefs[0]._id, old._id);
        assert.strictEqual(incoming.fileRefs[0]._id, 'incoming-file');
        assert.strictEqual(root.fileRefs.length, 0);
    });

    it('moves a proven binary deletion into retained trash without deleting its entity', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const content = new Uint8Array([8, 9]);
        const old = {_id: 'old-file', name: 'obsolete.png', _type: 'file'};
        root.fileRefs.push(old);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: old.name,
            fileEntity: root.fileRefs.find(item => item._id === old._id),
            fileType: root.fileRefs.some(item => item._id === old._id) ? 'file' : undefined,
        });
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'file', entityId: old._id, content,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.fileRefs.length, 0);
        };
        let deleteCalls = 0;
        vfs.api = {
            getFile: async () => ({type: 'success', content}),
            moveEntity: async () => ({type: 'success'}),
            deleteEntity: async () => { deleteCalls++; return {type: 'success'}; },
        };
        const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');

        await vfs.removeResourceAgainstBaseline(
            {path: '/Project/obsolete.png'}, old._id, expectedHash,
        );

        assert.strictEqual(deleteCalls, 0);
        assert.strictEqual(root.fileRefs.length, 0);
        assert.strictEqual(previous.fileRefs[0]._id, old._id);
    });

    it('rolls a changed binary candidate back when delete finds the original path empty', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const baseline = new Uint8Array([3]);
        const changed = new Uint8Array([7]);
        const old = {_id: 'changed-delete-file', name: 'obsolete.png', _type: 'file'};
        root.fileRefs.push(old);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: old.name,
            fileEntity: root.fileRefs.find(item => item._id === old._id),
            fileType: root.fileRefs.some(item => item._id === old._id) ? 'file' : undefined,
        });
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'file', entityId: old._id, content: baseline,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.fileRefs.length, 0);
        };
        vfs.api = {
            getFile: async () => ({type: 'success', content: changed}),
            moveEntity: async () => ({type: 'success'}),
        };
        const expectedHash = require('crypto').createHash('sha256').update(baseline).digest('hex');

        await assert.rejects(
            vfs.removeResourceAgainstBaseline(
                {path: '/Project/obsolete.png'}, old._id, expectedHash,
            ),
            /Remote resource changed/,
        );

        assert.strictEqual(previous.fileRefs.length, 0);
        assert.strictEqual(root.fileRefs[0]._id, old._id);
    });

    it('discards a deleted document WAL by old entity id without touching its replacement', async () => {
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const vfs = createVfs(VirtualFileSystem, {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
        }, events);
        const current = {_id: 'new-doc', name: 'main.tex', _type: 'doc'};
        vfs.root = {rootFolder: [{
            _id: 'root', name: 'Project', docs: [current], fileRefs: [], folders: [],
        }]};
        vfs.acquireDocumentJournalOwnership = async () => {};
        vfs.documentJournalDirectory = '/tmp/safe-sync-deleted-document-id';
        const removed = [];
        vfs.legacyDocumentStateStore = {remove: async id => removed.push(`legacy:${id}`)};
        vfs.socket.leaveDoc = async id => { removed.push(`leave:${id}`); };
        vfs.documentSessions.set(current._id, Promise.resolve({close: async () => {}}));

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        const originalRemove = journal.remove;
        journal.openExisting = async (_directory, id) => id === 'old-doc'
            ? {hasPendingOperations: false, close: async () => {}}
            : undefined;
        journal.remove = async (_directory, id) => { removed.push(`wal:${id}`); };
        try {
            await vfs.discardDocumentRecoveryById('old-doc', 'main.tex');
            assert(removed.includes('wal:old-doc'));
            assert(removed.includes('legacy:old-doc'));
            assert.strictEqual(vfs.documentSessions.has(current._id), true);
            assert(!removed.some(item => item.includes('new-doc')));
        } finally {
            journal.openExisting = originalOpenExisting;
            journal.remove = originalRemove;
        }
    });

    it('moves a proven document deletion into retained trash and then retires its settled WAL', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const content = new TextEncoder().encode('remote text');
        const doc = {_id: 'old-doc', name: 'main.tex', _type: 'doc'};
        root.docs.push(doc);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: doc.name,
            fileEntity: root.docs.find(item => item._id === doc._id),
            fileType: root.docs.some(item => item._id === doc._id) ? 'doc' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'doc', entityId: doc._id, content,
        });
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.docs.length, 0);
        };
        vfs.documentJournalDirectory = '/tmp/safe-sync-doc-trash';
        const removed = [];
        vfs.legacyDocumentStateStore = {remove: async id => removed.push(`legacy:${id}`)};
        let deleteCalls = 0;
        const moves = [];
        vfs.api = {
            moveEntity: async (_identity, _project, fileType, entityId, destinationId) => {
                moves.push([fileType, entityId, destinationId]);
                return {type: 'success'};
            },
            deleteEntity: async () => { deleteCalls++; return {type: 'success'}; },
        };
        vfs.socket.leaveDoc = async id => { events.push(`leave:${id}`); };
        vfs.socket.joinDoc = async () => ({
            otType: 'sharejs-text-ot', docLines: ['remote text'], version: 3,
        });

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        const originalRemove = journal.remove;
        journal.openExisting = async () => undefined;
        journal.remove = async (_directory, id) => { removed.push(`wal:${id}`); };
        try {
            const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
            await vfs.removeDocumentAgainstBaseline(
                {path: '/Project/main.tex'}, doc._id, expectedHash,
            );

            assert.strictEqual(deleteCalls, 0);
            assert.deepStrictEqual(moves, [['doc', doc._id, 'previous']]);
            assert.strictEqual(previous.docs[0]._id, doc._id);
            assert(removed.includes(`wal:${doc._id}`));
            assert(removed.includes(`legacy:${doc._id}`));
        } finally {
            journal.openExisting = originalOpenExisting;
            journal.remove = originalRemove;
        }
    });

    it('does not start retained document deletion without the project WAL lease', async () => {
        const storage = await mkdtemp(path.join(os.tmpdir(), 'hershel-doc-trash-lock-'));
        const events = [];
        const VirtualFileSystem = loadVirtualFileSystem(async () => ({}), events);
        const options = {
            joinProject: async () => ({}),
            getProjectSettings: async () => ({settings: {}}),
            context: {globalStorageUri: {fsPath: storage}},
        };
        const owner = createVfs(VirtualFileSystem, options, events);
        const contender = createVfs(VirtualFileSystem, options, events);
        const parent = folder('root', 'Project');
        const doc = {_id: 'locked-doc', name: 'main.tex', _type: 'doc'};
        parent.docs.push(doc);
        contender._resolveUri = async () => ({
            parentFolder: parent,
            fileName: doc.name,
            fileEntity: doc,
            fileType: 'doc',
        });
        let transactions = 0;
        contender.createRemoteResourceTransaction = async () => {
            transactions++;
            return {incoming: folder('incoming', 'incoming'), previous: folder('previous', 'previous')};
        };
        try {
            await owner.acquireDocumentJournalOwnership();
            await assert.rejects(
                contender.removeDocumentAgainstBaseline(
                    {path: '/Project/main.tex'}, doc._id, 'baseline-hash',
                ),
                /already owned by process/,
            );
            assert.strictEqual(transactions, 0);
        } finally {
            await owner.releaseDocumentJournalOwnership();
            await contender.releaseDocumentJournalOwnership();
            await rm(storage, {recursive: true, force: true});
        }
    });

    it('rolls a changed document candidate back without removing its WAL', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        const baselineText = 'remote text';
        const changedText = 'collaborator text';
        const content = new TextEncoder().encode(baselineText);
        const doc = {_id: 'changed-doc', name: 'main.tex', _type: 'doc'};
        root.docs.push(doc);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: doc.name,
            fileEntity: root.docs.find(item => item._id === doc._id),
            fileType: root.docs.some(item => item._id === doc._id) ? 'doc' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'doc', entityId: doc._id, content,
        });
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.docs.length, 0);
        };
        vfs.documentJournalDirectory = '/tmp/safe-sync-doc-rollback';
        let legacyRemoves = 0;
        vfs.legacyDocumentStateStore = {remove: async () => { legacyRemoves++; }};
        vfs.api = {moveEntity: async () => ({type: 'success'})};
        vfs.socket.leaveDoc = async () => {};
        let joins = 0;
        vfs.socket.joinDoc = async () => ({
            otType: 'sharejs-text-ot',
            docLines: [joins++ === 0 ? baselineText : changedText],
            version: joins,
        });

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        const originalRemove = journal.remove;
        let walRemoves = 0;
        journal.openExisting = async () => undefined;
        journal.remove = async () => { walRemoves++; };
        try {
            const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
            await assert.rejects(
                vfs.removeDocumentAgainstBaseline(
                    {path: '/Project/main.tex'}, doc._id, expectedHash,
                ),
                /Remote resource changed/,
            );

            assert.strictEqual(previous.docs.length, 0);
            assert.strictEqual(root.docs[0]._id, doc._id);
            assert.strictEqual(walRemoves, 0);
            assert.strictEqual(legacyRemoves, 0);
        } finally {
            journal.openExisting = originalOpenExisting;
            journal.remove = originalRemove;
        }
    });

    it('refuses retained document deletion while a logical acknowledgement is pending', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        const content = new TextEncoder().encode('remote text');
        const doc = {_id: 'pending-ack-doc', name: 'main.tex', _type: 'doc'};
        root.docs.push(doc);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: doc.name,
            fileEntity: doc,
            fileType: 'doc',
        });
        vfs.createRemoteResourceTransaction = async () => ({
            incoming: folder('incoming', 'incoming'),
            previous: folder('previous', 'previous'),
        });
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'doc', entityId: doc._id, content,
        });
        vfs.logicalDocumentAcknowledgements.set(doc._id, new Set([new Promise(() => {})]));
        let moveCalls = 0;
        vfs.api = {moveEntity: async () => { moveCalls++; return {type: 'success'}; }};
        const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');

        await assert.rejects(
            vfs.removeDocumentAgainstBaseline(
                {path: '/Project/main.tex'}, doc._id, expectedHash,
            ),
            /pending journaled operation or acknowledgement/,
        );

        assert.strictEqual(moveCalls, 0);
        assert.strictEqual(root.docs[0]._id, doc._id);
        assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), false);
    });

    it('refuses retained document deletion when its detached WAL is pending', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        vfs.acquireDocumentJournalOwnership = async () => {};
        const content = new TextEncoder().encode('remote text');
        const doc = {_id: 'pending-wal-doc', name: 'main.tex', _type: 'doc'};
        root.docs.push(doc);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: doc.name,
            fileEntity: doc,
            fileType: 'doc',
        });
        vfs.createRemoteResourceTransaction = async () => ({
            incoming: folder('incoming', 'incoming'),
            previous: folder('previous', 'previous'),
        });
        vfs.readAuthoritativeFile = async () => ({
            fileType: 'doc', entityId: doc._id, content,
        });
        vfs.documentJournalDirectory = '/tmp/safe-sync-pending-doc-trash';
        let moveCalls = 0;
        vfs.api = {moveEntity: async () => { moveCalls++; return {type: 'success'}; }};

        const journal = require('../out/sync/documentJournal').DocumentJournal;
        const originalOpenExisting = journal.openExisting;
        journal.openExisting = async () => ({
            hasPendingOperations: true,
            close: async () => {},
        });
        try {
            const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
            await assert.rejects(
                vfs.removeDocumentAgainstBaseline(
                    {path: '/Project/main.tex'}, doc._id, expectedHash,
                ),
                /durable operations without a proven server disposition/,
            );
            assert.strictEqual(moveCalls, 0);
            assert.strictEqual(root.docs[0]._id, doc._id);
            assert.strictEqual(vfs.documentRecoveryFences.has(doc._id), false);
        } finally {
            journal.openExisting = originalOpenExisting;
        }
    });

    it('moves an empty proven folder into retained trash without deleting its subtree', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const target = folder('target-folder', 'sections');
        const internal = folder('internal', '.hershel-safe-sync');
        const trash = folder('trash', 'trash');
        const transaction = folder('transaction', 'transaction');
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        root.folders.push(target, internal);
        internal.folders.push(trash);
        trash.folders.push(transaction);
        transaction.folders.push(incoming, previous);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: target.name,
            fileEntity: root.folders.find(item => item._id === target._id),
            fileType: root.folders.some(item => item._id === target._id) ? 'folder' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        const {EMPTY_REMOTE_FOLDER_TREE_HASH} = require('../out/core/remoteFileSystemProvider');
        vfs.readAuthoritativeFolderTreeHashAtPath = async () => EMPTY_REMOTE_FOLDER_TREE_HASH;
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.folders.some(item => item._id === target._id), false);
        };
        const moves = [];
        let deleteCalls = 0;
        vfs.api = {
            moveEntity: async (_identity, _project, fileType, entityId, destinationId) => {
                moves.push([fileType, entityId, destinationId]);
                return {type: 'success'};
            },
            deleteEntity: async () => { deleteCalls++; return {type: 'success'}; },
        };

        await vfs.removeFolderAgainstBaseline(
            {path: '/Project/sections', query: '', authority: ''},
            target._id,
        );

        assert.strictEqual(deleteCalls, 0);
        assert.deepStrictEqual(moves, [['folder', target._id, previous._id]]);
        assert.strictEqual(previous.folders[0]._id, target._id);
    });

    it('waits for an acknowledged retained folder move before checking its tree', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const target = folder('eventual-folder', 'sections');
        const internal = folder('internal', '.hershel-safe-sync');
        const trash = folder('trash', 'trash');
        const transaction = folder('transaction', 'transaction');
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        root.folders.push(target, internal);
        internal.folders.push(trash);
        trash.folders.push(transaction);
        transaction.folders.push(incoming, previous);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: target.name,
            fileEntity: root.folders.find(item => item._id === target._id),
            fileType: root.folders.some(item => item._id === target._id) ? 'folder' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        const staleProject = {
            rootFolder: [JSON.parse(JSON.stringify(root))],
        };
        let moved = false;
        let stalePostMutationReads = 0;
        vfs.readFreshProjectSnapshot = async () => {
            if (moved && stalePostMutationReads < 2) {
                    stalePostMutationReads++;
                return staleProject;
            }
            return {rootFolder: [root]};
        };
        vfs.api = {
            projectEntitiesJson: async () => ({
                type: 'success', entities: projectEntities(staleProject.rootFolder[0]),
            }),
            moveEntity: async () => {
                moved = true;
                return {type: 'success'};
            },
        };

        await vfs.removeFolderAgainstBaseline(
            {path: '/Project/sections', query: '', authority: ''},
            target._id,
        );

        assert.strictEqual(stalePostMutationReads, 2);
        assert.strictEqual(root.folders.includes(target), false);
        assert.strictEqual(previous.folders[0], target);
    });

    it('waits for an acknowledged child removal before proving its folder empty', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const target = folder('eventual-empty-folder', 'sections');
        const internal = folder('internal', '.hershel-safe-sync');
        const trash = folder('trash', 'trash');
        const transaction = folder('transaction', 'transaction');
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        root.folders.push(target, internal);
        internal.folders.push(trash);
        trash.folders.push(transaction);
        transaction.folders.push(incoming, previous);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: target.name,
            fileEntity: root.folders.find(item => item._id === target._id),
            fileType: root.folders.some(item => item._id === target._id) ? 'folder' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        const staleRoot = JSON.parse(JSON.stringify(root));
        staleRoot.folders.find(item => item._id === target._id).docs.push({
            _id: 'removed-doc', name: 'removed.tex', _type: 'doc',
        });
        let stalePreMutationReads = 0;
        vfs.readFreshProjectSnapshot = async () => {
            if (stalePreMutationReads < 2) {
                    stalePreMutationReads++;
                return {rootFolder: [staleRoot]};
            }
            return {rootFolder: [root]};
        };
        vfs.api = {
            projectEntitiesJson: async () => ({
                type: 'success', entities: projectEntities(staleRoot),
            }),
            moveEntity: async () => ({type: 'success'}),
        };

        await vfs.removeFolderAgainstBaseline(
            {path: '/Project/sections', query: '', authority: ''},
            target._id,
        );

        assert.strictEqual(stalePreMutationReads, 2);
        assert.strictEqual(root.folders.includes(target), false);
        assert.strictEqual(previous.folders[0], target);
    });

    it('rolls back a folder with a concurrent child and carries that child with it', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const target = folder('target-folder', 'sections');
        const internal = folder('internal', '.hershel-safe-sync');
        const transaction = folder('transaction', 'transaction');
        const incoming = folder('incoming', 'incoming');
        const previous = folder('previous', 'previous');
        root.folders.push(target, internal);
        internal.folders.push(transaction);
        transaction.folders.push(incoming, previous);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: target.name,
            fileEntity: root.folders.find(item => item._id === target._id),
            fileType: root.folders.some(item => item._id === target._id) ? 'folder' : undefined,
        });
        vfs.createRemoteResourceTransaction = async () => ({incoming, previous});
        const {EMPTY_REMOTE_FOLDER_TREE_HASH} = require('../out/core/remoteFileSystemProvider');
        let treeReads = 0;
        vfs.readAuthoritativeFolderTreeHashAtPath = async () => {
            if (treeReads++ === 0) return EMPTY_REMOTE_FOLDER_TREE_HASH;
            target.fileRefs.push({_id: 'late-child', name: 'late.png', _type: 'file'});
            return 'changed-tree-hash';
        };
        vfs.assertAuthoritativePathAbsent = async () => {
            assert.strictEqual(root.folders.some(item => item._id === target._id), false);
        };
        const moves = [];
        vfs.api = {
            moveEntity: async (_identity, _project, fileType, entityId, destinationId) => {
                moves.push([fileType, entityId, destinationId]);
                return {type: 'success'};
            },
        };

        await assert.rejects(
            vfs.removeFolderAgainstBaseline(
                {path: '/Project/sections', query: '', authority: ''},
                target._id,
            ),
            /Remote resource changed/,
        );

        assert.deepStrictEqual(moves, [
            ['folder', target._id, previous._id],
            ['folder', target._id, root._id],
        ]);
        assert.strictEqual(root.folders.find(item => item._id === target._id).fileRefs[0]._id,
            'late-child');
        assert.strictEqual(previous.folders.length, 0);
    });

    it('refuses to create remote trash inside the folder being moved', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const internal = folder('internal', '.hershel-safe-sync');
        root.folders.push(internal);
        vfs._resolveUri = async () => ({
            parentFolder: root,
            fileName: internal.name,
            fileEntity: internal,
            fileType: 'folder',
        });
        let transactions = 0;
        vfs.createRemoteResourceTransaction = async () => {
            transactions++;
            return {incoming: folder('incoming', 'incoming'), previous: folder('previous', 'previous')};
        };

        await assert.rejects(
            vfs.removeFolderAgainstBaseline(
                {path: '/Project/.hershel-safe-sync'}, internal._id,
            ),
            /trash cannot be nested inside/,
        );
        assert.strictEqual(transactions, 0);
    });

    it('reads retained file and document candidates for visible conflict artifacts', async () => {
        const events = [];
        const {vfs} = resourceVfs(events);
        const {RetainedRemoteCandidateError} = require('../out/core/remoteFileSystemProvider');
        const fileBytes = new Uint8Array([1, 2]);
        vfs.api = {getFile: async () => ({type: 'success', content: fileBytes})};
        vfs.socket.joinDoc = async () => ({
            otType: 'sharejs-text-ot', docLines: ['retained doc'], version: 1,
        });
        vfs.socket.leaveDoc = async () => {};
        const fileError = new RetainedRemoteCandidateError(
            {path: '/Project/figure.png'}, 'retained-file', 'file-hash', 'file',
        );

        assert.deepStrictEqual(
            await vfs.readRetainedRemoteCandidate(fileError),
            fileBytes,
        );
        assert.strictEqual(
            new TextDecoder().decode(await vfs.readRetainedRemoteCandidate('retained-doc', 'doc')),
            'retained doc',
        );
    });

    it('inspects current bytes and hash for a retained history resource', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const internal = folder('internal', '.hershel-safe-sync');
        const history = folder('history', 'history');
        const transaction = folder('transaction', 'transaction');
        const previous = folder('previous', 'previous');
        const entity = {_id: 'retained-file', name: 'figure.png', _type: 'file'};
        root.folders.push(internal);
        internal.folders.push(history);
        history.folders.push(transaction);
        transaction.folders.push(previous);
        previous.fileRefs.push(entity);
        const content = new Uint8Array([4, 5, 6]);
        vfs.api = {getFile: async () => ({type: 'success', content})};

        const inspection = await vfs.inspectRetainedRemoteCandidate(entity._id);

        const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
        assert.strictEqual(inspection.fileType, 'file');
        assert.strictEqual(inspection.hash, expectedHash);
        assert.deepStrictEqual(inspection.content, content);
        assert.strictEqual(
            inspection.retainedPath,
            '.hershel-safe-sync/history/transaction/previous/figure.png',
        );
    });

    it('inspects current bytes and hash for a retained trash document', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const internal = folder('internal', '.hershel-safe-sync');
        const trash = folder('trash', 'trash');
        const transaction = folder('transaction', 'transaction');
        const previous = folder('previous', 'previous');
        const entity = {_id: 'retained-doc', name: 'main.tex', _type: 'doc'};
        root.folders.push(internal);
        internal.folders.push(trash);
        trash.folders.push(transaction);
        transaction.folders.push(previous);
        previous.docs.push(entity);
        vfs.socket.joinDoc = async () => ({
            otType: 'sharejs-text-ot', docLines: ['retained', 'document'], version: 4,
        });
        vfs.socket.leaveDoc = async id => events.push(`leave:${id}`);

        const inspection = await vfs.inspectRetainedRemoteCandidate(entity._id);

        const content = new TextEncoder().encode('retained\ndocument');
        const expectedHash = require('crypto').createHash('sha256').update(content).digest('hex');
        assert.strictEqual(inspection.fileType, 'doc');
        assert.strictEqual(inspection.hash, expectedHash);
        assert.deepStrictEqual(inspection.content, content);
        assert.strictEqual(
            inspection.retainedPath,
            '.hershel-safe-sync/trash/transaction/previous/main.tex',
        );
        assert(events.includes(`leave:${entity._id}`));
    });

    it('inspects a retained folder through its authoritative structure hash', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const internal = folder('internal', '.hershel-safe-sync');
        const trash = folder('trash', 'trash');
        const transaction = folder('transaction', 'transaction');
        const previous = folder('previous', 'previous');
        const entity = folder('retained-folder', 'sections');
        root.folders.push(internal);
        internal.folders.push(trash);
        trash.folders.push(transaction);
        transaction.folders.push(previous);
        previous.folders.push(entity);
        const treeHash = 'authoritative-tree-hash';
        let inspectedPath;
        vfs.readAuthoritativeFolderTreeHashAtPath = async (_identity, path) => {
            inspectedPath = path;
            return treeHash;
        };

        const inspection = await vfs.inspectRetainedRemoteCandidate(entity._id);

        assert.strictEqual(inspection.fileType, 'folder');
        assert.strictEqual(inspection.hash, treeHash);
        assert.strictEqual(inspection.content, undefined);
        assert.strictEqual(
            inspectedPath,
            '.hershel-safe-sync/trash/transaction/previous/sections',
        );
    });

    it('does not inspect an entity outside the retained history or trash namespace', async () => {
        const events = [];
        const {vfs, root} = resourceVfs(events);
        const visible = {_id: 'visible-file', name: 'figure.png', _type: 'file'};
        const lookalike = folder('lookalike', '.hershel-safe-sync-history');
        const lookalikeFile = {_id: 'lookalike-file', name: 'hidden.png', _type: 'file'};
        root.fileRefs.push(visible);
        root.folders.push(lookalike);
        lookalike.fileRefs.push(lookalikeFile);
        vfs.api = {getFile: async () => { throw new Error('must not read'); }};

        assert.strictEqual(await vfs.inspectRetainedRemoteCandidate(visible._id), undefined);
        assert.strictEqual(await vfs.inspectRetainedRemoteCandidate(lookalikeFile._id), undefined);
        assert.strictEqual(await vfs.inspectRetainedRemoteCandidate('missing-id'), undefined);
    });
});
