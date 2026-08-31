const assert = require('assert');
const Module = require('module');

function loadGlobalStateManager() {
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return {window: {showErrorMessage() {}}};
        }
        if (request === '../api/base') {
            return {BaseAPI: class BaseAPI { constructor(url) { this.url = url; } }};
        }
        if (request === '../api/socketio') {
            return {SocketIOAPI: class SocketIOAPI {}};
        }
        if (request === '../api/extendedBase') {
            return {ExtendedBaseAPI: class ExtendedBaseAPI { constructor(url) { this.url = url; } }};
        }
        return originalLoad.apply(this, arguments);
    };
    try {
        delete require.cache[require.resolve('../out/utils/globalStateManager')];
        return require('../out/utils/globalStateManager').GlobalStateManager;
    } finally {
        Module._load = originalLoad;
    }
}

describe('bound Local Replica authentication bootstrap', () => {
    it('restores missing public server state from the binding and SecretStorage', async () => {
        const GlobalStateManager = loadGlobalStateManager();
        let persisted = {};
        const context = {
            extension: {id: 'hershel.overleaf-workshop-safe-sync'},
            globalState: {
                get: (_key, fallback) => persisted ?? fallback,
                update: async (_key, value) => { persisted = value; },
            },
            secrets: {
                get: async () => JSON.stringify({csrfToken: 'csrf', cookies: 'cookie'}),
            },
        };

        await GlobalStateManager.restoreBoundServer(context, {
            uri: 'overleaf-workshop://10.23.45.67:4567/project?user%3Duser-1%26project%3Dproject-1',
            serverName: '10.23.45.67:4567',
        });

        assert.strictEqual(persisted['10.23.45.67:4567'].url, 'http://10.23.45.67:4567/');
        assert.strictEqual(persisted['10.23.45.67:4567'].login.userId, 'user-1');
        assert(GlobalStateManager.initSocketIOAPI(
            context,
            '10.23.45.67:4567',
            'project-1',
        ));
    });

    it('fails cleanly when a server record is absent', () => {
        const GlobalStateManager = loadGlobalStateManager();
        const context = {
            extension: {id: 'hershel.overleaf-workshop-safe-sync'},
            globalState: {get: (_key, fallback) => fallback},
        };
        assert.strictEqual(
            GlobalStateManager.initSocketIOAPI(context, 'missing', 'project'),
            undefined,
        );
    });

    it('does not send stored credentials to a mismatched binding URL', async () => {
        const GlobalStateManager = loadGlobalStateManager();
        const context = {
            extension: {id: 'hershel.overleaf-workshop-safe-sync'},
            globalState: {get: (_key, fallback) => fallback},
            secrets: {
                get: async () => JSON.stringify({csrfToken: 'csrf', cookies: 'cookie'}),
            },
        };
        await assert.rejects(
            () => GlobalStateManager.restoreBoundServer(context, {
                uri: 'overleaf-workshop://10.23.45.67:4567/project?user%3Duser-1',
                serverName: '10.23.45.67:4567',
                serverUrl: 'https://example.invalid/',
            }),
            /server URL does not match its server name/,
        );
    });
});
