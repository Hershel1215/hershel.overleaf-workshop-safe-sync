const assert = require('assert');
const Module = require('module');

const tick = () => new Promise(resolve => setImmediate(resolve));

function deferred() {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return {promise, resolve, reject};
}

function loadCollection() {
    const ui = {quickPicks: [], infos: [], errors: [], statusBar: undefined, pick: undefined};
    const startup = deferred();
    class Disposable {
        constructor(onDispose) {
            this.onDispose = onDispose;
        }
        dispose() {
            this.onDispose?.();
        }
    }
    class FakeReplica {
        static label = 'Local Replica';
        static async readSettings() {
            return undefined;
        }
        constructor(vfs, baseUri) {
            this.baseUri = baseUri;
            this.iconPath = {id: 'folder'};
            this.status = {status: 'idle', message: ''};
            this.settingItems = [];
            this.disposed = false;
        }
        get scmKey() {
            return this.baseUri.toString();
        }
        get triggers() {
            return startup.promise;
        }
        dispose() {
            this.disposed = true;
        }
    }
    const vscode = {
        Disposable,
        StatusBarAlignment: {Left: 1},
        QuickPickItemKind: {Separator: -1},
        ThemeIcon: class ThemeIcon {
            constructor(id) {
                this.id = id;
            }
        },
        MarkdownString: class MarkdownString {
            constructor(value = '') {
                this.value = value;
            }
            appendMarkdown(value) {
                this.value += value;
            }
        },
        Uri: {
            file: fsPath => ({fsPath, scheme: 'file', toString: () => `file://${fsPath}`}),
            parse: value => ({toString: () => value}),
        },
        l10n: {t: (value, args = {}) => value.replace(/\{(\w+)\}/g, (_, key) => args[key] ?? '')},
        workspace: {workspaceFolders: []},
        window: {
            createStatusBarItem: () => {
                ui.statusBar = {text: '', show() {}, dispose() {}};
                return ui.statusBar;
            },
            showQuickPick: items => {
                ui.quickPicks.push(items);
                return Promise.resolve(ui.pick?.(items));
            },
            showInformationMessage: message => {
                ui.infos.push(message);
                return Promise.resolve(undefined);
            },
            showErrorMessage: message => {
                ui.errors.push(message);
                return Promise.resolve(undefined);
            },
            showWarningMessage: () => Promise.resolve(undefined),
        },
    };
    const stubs = {
        vscode,
        '../core/remoteFileSystemProvider': {},
        '.': {BaseSCM: class BaseSCM {}},
        './localReplicaSCM': {LocalReplicaSCMProvider: FakeReplica},
        './localGitBridgeSCM': {},
        './historyViewProvider': {HistoryViewProvider: class HistoryViewProvider {}},
        '../utils/globalStateManager': {
            GlobalStateManager: {
                getServerProjectSCMPersists: () => ({
                    replica: {label: 'Local Replica', baseUri: '/work/replica', enabled: true},
                }),
            },
        },
        '../utils/eventBus': {EventBus: {on: () => new Disposable()}},
        '../consts': {ROOT_NAME: 'test-root'},
    };
    const modulePath = require.resolve('../out/scm/scmCollectionProvider');
    const originalLoad = Module._load;
    Module._load = function(request, ...rest) {
        if (Object.prototype.hasOwnProperty.call(stubs, request)) {
            return stubs[request];
        }
        return originalLoad.call(this, request, ...rest);
    };
    try {
        delete require.cache[modulePath];
        const {SCMCollectionProvider} = require(modulePath);
        const vfs = {serverName: 'server', projectId: 'project', origin: {}, setProjectSCMPersist() {}};
        return {collection: new SCMCollectionProvider(vfs, {}), ui, startup};
    } finally {
        Module._load = originalLoad;
        delete require.cache[modulePath];
    }
}

describe('source control collection startup', () => {
    it('lists a local replica as starting before its startup scan finishes', async () => {
        const {collection, ui, startup} = loadCollection();
        await tick();

        collection.showSCMConfiguration();
        const [replicaItem] = ui.quickPicks.at(-1);
        assert.strictEqual(replicaItem.label, 'Local Replica');
        assert.match(replicaItem.description, /^Starting\.\.\. file:\/\/\/work\/replica$/);
        assert.strictEqual(ui.statusBar.text, '$(sync~spin)');

        startup.resolve([]);
        await tick();

        collection.showSCMConfiguration();
        const [readyItem] = ui.quickPicks.at(-1);
        assert.strictEqual(readyItem.description, 'file:///work/replica');
        assert.strictEqual(ui.statusBar.text, '$(cloud)');
    });

    it('explains that settings wait for startup instead of opening them', async () => {
        const {collection, ui} = loadCollection();
        await tick();
        ui.pick = items => items[0];

        collection.showSCMConfiguration();
        await tick();

        assert.strictEqual(ui.quickPicks.length, 1);
        assert.strictEqual(ui.infos.length, 1);
        assert.match(ui.infos[0], /"Local Replica" is still starting/);
    });

    it('drops a replica whose startup fails', async () => {
        const {collection, ui, startup} = loadCollection();
        await tick();

        startup.reject(new Error('server unreachable'));
        await tick();

        collection.showSCMConfiguration();
        const labels = ui.quickPicks.at(-1).map(item => item.label);
        assert(!labels.includes('Local Replica'));
        assert.match(ui.errors[0], /server unreachable/);
    });
});
