const assert = require('assert');
const Module = require('module');

function loadVirtualFileSystem() {
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
                    showErrorMessage: async () => undefined,
                    showWarningMessage: async () => undefined,
                    withProgress: async (_options, task) => task(),
                },
                commands: {executeCommand: () => undefined},
            };
        }
        if (request === '../utils/globalStateManager') {
            return {GlobalStateManager: {authenticate: async () => ({})}};
        }
        if (request === '../collaboration/clientManager') {
            return {ClientManager: class ClientManager {}};
        }
        if (request === '../scm/scmCollectionProvider') {
            return {SCMCollectionProvider: class SCMCollectionProvider {}};
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

/** Routine reclamation runs behind the operation that scheduled it. */
async function drainBackgroundPrune(vfs) {
    while (vfs.retainedRemotePrune) {
        await vfs.retainedRemotePrune;
    }
}

function folder(name, id) {
    return {_id: id ?? `id-${name}`, _type: 'folder', name, docs: [], fileRefs: [], folders: []};
}

/** The retention scheme stamps each transaction folder with its ISO instant. */
function transactionName(ageMs) {
    const stamp = new Date(Date.now() - ageMs).toISOString().replace(/[^0-9]/g, '');
    return `${stamp}-3f1d0f0a-1111-4222-8333-444455556666`;
}

function createVfs(VirtualFileSystem, options) {
    const trash = folder('trash');
    const internal = folder('.hershel-safe-sync');
    internal.folders.push(trash);
    for (const {ageMs, name} of options.retained) {
        trash.folders.push(folder(name ?? transactionName(ageMs)));
    }
    const deleted = [];
    const created = [];
    let refusals = options.refusals ?? 0;
    const vfs = Object.create(VirtualFileSystem.prototype);
    Object.assign(vfs, {
        projectId: 'test-project',
        serverName: 'test-server',
        context: {},
        remoteSafeSyncNamespacePromise: undefined,
        getOwnedRemoteSafeSyncNamespace: async () => internal,
        api: {
            addFolder: async (_identity, _projectId, name, parentId) => {
                if (refusals > 0) {
                    refusals--;
                    return {
                        type: 'error',
                        message: '400: "This project has reached the 2000 file limit"',
                    };
                }
                created.push(name);
                return {type: 'success', entity: folder(name, `created-${created.length}`)};
            },
            deleteEntity: async (_identity, _projectId, fileType, entityId) => {
                deleted.push(`${fileType}:${entityId}`);
                return {type: 'success'};
            },
        },
    });
    return {vfs, trash, deleted, created};
}

describe('remote retention window', () => {
    it('reclaims aged transactions so a full project can still accept a delete', async () => {
        const VirtualFileSystem = loadVirtualFileSystem();
        const day = 24 * 60 * 60 * 1000;
        const {vfs, trash, deleted} = createVfs(VirtualFileSystem, {
            retained: [
                ...Array.from({length: 6}, () => ({ageMs: 3 * day})),
                ...Array.from({length: 24}, () => ({ageMs: 60 * 1000})),
            ],
        });

        const transaction = await vfs.createRemoteResourceTransaction({}, 'trash');
        await drainBackgroundPrune(vfs);

        assert.strictEqual(deleted.length, 6);
        assert.ok(deleted.every(entry => entry.startsWith('folder:')));
        // 24 retained + the transaction this call opened.
        assert.strictEqual(trash.folders.length, 25);
        assert.strictEqual(transaction.incoming.name, 'incoming');
        assert.strictEqual(transaction.previous.name, 'previous');
    });

    it('keeps every transaction while the retention window is not full', async () => {
        const VirtualFileSystem = loadVirtualFileSystem();
        const day = 24 * 60 * 60 * 1000;
        const {vfs, deleted} = createVfs(VirtualFileSystem, {
            retained: Array.from({length: 20}, () => ({ageMs: 30 * day})),
        });

        await vfs.createRemoteResourceTransaction({}, 'trash');
        await drainBackgroundPrune(vfs);

        assert.deepStrictEqual(deleted, []);
    });

    it('never reclaims a folder this retention scheme did not name', async () => {
        const VirtualFileSystem = loadVirtualFileSystem();
        const {vfs, deleted} = createVfs(VirtualFileSystem, {
            retained: Array.from({length: 30}, (_value, index) => ({
                name: `someone-elses-folder-${index}`,
            })),
        });

        await vfs.createRemoteResourceTransaction({}, 'trash');
        await drainBackgroundPrune(vfs);

        assert.deepStrictEqual(deleted, []);
    });

    it('frees room and retries once when the project reports its file limit', async () => {
        const VirtualFileSystem = loadVirtualFileSystem();
        const {vfs, deleted, created} = createVfs(VirtualFileSystem, {
            // Nothing is old enough for routine reclamation, so the retry is
            // the only thing that can make room.
            retained: Array.from({length: 30}, () => ({ageMs: 10 * 60 * 1000})),
            refusals: 1,
        });

        const transaction = await vfs.createRemoteResourceTransaction({}, 'trash');
        await drainBackgroundPrune(vfs);

        assert.strictEqual(deleted.length, 6);
        assert.strictEqual(transaction.incoming.name, 'incoming');
        assert.deepStrictEqual(created.slice(-2), ['incoming', 'previous']);
    });

    it('refuses to reclaim transactions another endpoint may still be writing', async () => {
        const VirtualFileSystem = loadVirtualFileSystem();
        const {vfs, deleted} = createVfs(VirtualFileSystem, {
            retained: Array.from({length: 30}, () => ({ageMs: 5 * 1000})),
            refusals: 1,
        });

        await assert.rejects(
            vfs.createRemoteResourceTransaction({}, 'trash'),
            /file limit/,
        );
        await drainBackgroundPrune(vfs);
        assert.deepStrictEqual(deleted, []);
    });
});
