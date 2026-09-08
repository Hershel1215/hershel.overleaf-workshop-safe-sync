const assert = require('assert');
const Module = require('module');
const io = require('socket.io-client');

function socketApi() {
    const original = Module._load;
    Module._load = function (name) {
        if (name === 'vscode') return {
            workspace: {getConfiguration: () => ({get: (_key, fallback) => fallback})},
            l10n: {t: x => x},
        };
        return original.apply(this, arguments);
    };
    try {
        return require('../out/api/socketio').SocketIOAPI;
    } finally {
        Module._load = original;
    }
}

describe('legacy Socket.IO transport lifecycle', () => {
    it('cancels a delayed handshake before it can reopen a retired client', () => {
        const SocketIOAPI = socketApi();
        const request = io.util.request;
        let xhr;
        let opens = 0;
        let aborts = 0;
        io.util.request = () => (xhr = {
            readyState: 1, status: 200, responseText: 'session:30:60:websocket',
            open() {}, send() {}, getAllResponseHeaders: () => '',
            abort() { aborts++; },
        });
        const manager = new io.Socket({'auto connect': false});
        manager.getTransport = () => ({
            name: 'websocket',
            ready: (_socket, ready) => ready(),
            open: () => { opens++; },
            close() {}, clearTimeouts() {},
        });
        const client = new SocketIOAPI('http://example.test', {
            _initSocketV0: () => { manager.connect(); return manager.of(''); },
        }, {}, 'project');
        try {
            const lateResponse = xhr.onreadystatechange;
            client.retire();
            // A response already queued when cancellation started must be harmless.
            xhr.readyState = 4;
            lateResponse();
            assert.strictEqual(opens, 0, 'retirement allowed a late HTTP handshake to open WS');
            assert.strictEqual(aborts, 1, 'the handshake request was not aborted');
        } finally {
            io.util.request = request;
            manager.options.reconnect = false;
            manager.disconnect();
            clearTimeout(manager.connectTimeoutTimer);
            clearTimeout(manager.heartbeatTimeoutTimer);
            clearTimeout(manager.reconnectionTimer);
        }
    });

    it('ignores packets from a replaced WebSocket instead of flushing the new transport', () => {
        const original = Module._load;
        class FakeWebSocket {
            send() { throw new Error('not opened'); }
            close() {}
        }
        Module._load = function (name) {
            return name === 'ws' ? FakeWebSocket : original.apply(this, arguments);
        };
        const manager = new io.Socket({'auto connect': false, reconnect: false});
        const old = new io.Transport.websocket(manager, 'old');
        manager.transport = old;
        try {
            old.open();
            const callback = old.websocket.onmessage;
            manager.buffer.push({type: 'event', name: 'joinProject', args: []});
            const next = new io.Transport.websocket(manager, 'next');
            manager.transport = next;
            next.open();
            assert.doesNotThrow(() => callback({data: '1::'}));
            assert.strictEqual(manager.connected, false);
            assert.strictEqual(manager.buffer.length, 1);
        } finally {
            Module._load = original;
            clearTimeout(manager.heartbeatTimeoutTimer);
            old.clearTimeouts();
            manager.transport.clearTimeouts();
        }
    });
});
