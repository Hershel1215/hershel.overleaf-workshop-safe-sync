const assert = require('assert');
const {EventEmitter} = require('events');
const Module = require('module');

function loadSocketApi() {
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return {
                workspace: {
                    getConfiguration: () => ({get: (_key, fallback) => fallback}),
                },
                l10n: {t: value => value},
            };
        }
        return originalLoad.apply(this, arguments);
    };
    try {
        delete require.cache[require.resolve('../out/api/socketio')];
        delete require.cache[require.resolve('../out/api/socketioAlt')];
        return require('../out/api/socketio').SocketIOAPI;
    } finally {
        Module._load = originalLoad;
    }
}

class FakeSocket extends EventEmitter {
    constructor() {
        super();
        this.emit = (...args) => super.emit(...args);
    }

    disconnect() {}
}

describe('SocketIOAPI instance identity isolation', () => {
    it('does not publish one v2 socket source id to another socket handler', async () => {
        const SocketIOAPI = loadSocketApi();
        const sockets = [];
        const api = {
            _initSocketV0() {
                const socket = new FakeSocket();
                sockets.push(socket);
                return socket;
            },
        };
        const first = new SocketIOAPI('http://example.test', api, {}, 'project');
        const second = new SocketIOAPI('http://example.test', api, {}, 'project');
        const firstSources = [];
        const secondSources = [];
        first.updateEventHandlers({onConnectionAccepted: source => firstSources.push(source)});
        second.updateEventHandlers({onConnectionAccepted: source => secondSources.push(source)});

        first.scheme = 'v2';
        second.scheme = 'v2';
        first.init();
        second.init();
        const firstV2Socket = sockets[2];
        const secondV2Socket = sockets[3];
        firstV2Socket.emit('joinProjectResponse', {publicId: 'source-first', project: {}});
        await new Promise(resolve => setImmediate(resolve));
        assert.deepStrictEqual(firstSources, ['source-first']);
        assert.deepStrictEqual(secondSources, []);

        secondV2Socket.emit('joinProjectResponse', {publicId: 'source-second', project: {}});
        await new Promise(resolve => setImmediate(resolve));
        assert.deepStrictEqual(firstSources, ['source-first']);
        assert.deepStrictEqual(secondSources, ['source-second']);
        assert.strictEqual(first.sourceId, 'source-first');
        assert.strictEqual(second.sourceId, 'source-second');
    });
});
