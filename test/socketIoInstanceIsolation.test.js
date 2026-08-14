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

class ControlledMembershipSocket extends EventEmitter {
    constructor() {
        super();
        this.activeMembershipCalls = 0;
        this.maxActiveMembershipCalls = 0;
        this.membershipStarts = [];
        this.pendingMembershipCalls = [];
        this.disconnectCalls = 0;
        this.emit = (event, ...args) => {
            if (event !== 'joinDoc' && event !== 'leaveDoc') {
                return super.emit(event, ...args);
            }
            const callback = args.at(-1);
            const docId = args[0];
            this.activeMembershipCalls += 1;
            this.maxActiveMembershipCalls = Math.max(
                this.maxActiveMembershipCalls,
                this.activeMembershipCalls,
            );
            this.membershipStarts.push(`${event}:${docId}`);
            this.pendingMembershipCalls.push({event, callback});
            return true;
        };
    }

    completeNextMembershipCall(error) {
        const pending = this.pendingMembershipCalls.shift();
        assert(pending, 'expected a pending membership RPC');
        this.activeMembershipCalls -= 1;
        if (pending.event === 'joinDoc') {
            pending.callback(error, [''], 0, [], {}, 'sharejs-text-ot');
        } else {
            pending.callback(error);
        }
    }

    disconnect() {
        this.disconnectCalls += 1;
    }
}

class TimedOutProjectSocket extends EventEmitter {
    constructor() {
        super();
        this.disconnectCalls = 0;
        this.emit = (event, ...args) => {
            if (event !== 'joinProject') {
                return super.emit(event, ...args);
            }
            const timeout = new Error('simulated project acknowledgement timeout');
            timeout.name = 'SocketIOTimeoutError';
            args.at(-1)(timeout);
            return true;
        };
    }

    disconnect() {
        this.disconnectCalls += 1;
    }
}

describe('SocketIOAPI instance identity isolation', () => {
    it('invalidates a socket whose project acknowledgement timed out', async () => {
        const SocketIOAPI = loadSocketApi();
        const socket = new TimedOutProjectSocket();
        const api = {_initSocketV0: () => socket};
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');

        await assert.rejects(
            () => client.joinProject('project'),
            /project acknowledgement timeout/,
        );
        assert.strictEqual(socket.disconnectCalls, 1);
        assert.strictEqual(client.needsReinit, true);
    });

    it('serializes joinDoc and leaveDoc membership RPCs on one socket', async () => {
        const SocketIOAPI = loadSocketApi();
        const socket = new ControlledMembershipSocket();
        const api = {_initSocketV0: () => socket};
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');

        const first = client.joinDoc('doc-a');
        const second = client.joinDoc('doc-b', 7);
        const third = client.leaveDoc('doc-a');
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(socket.membershipStarts, ['joinDoc:doc-a']);
        assert.strictEqual(socket.maxActiveMembershipCalls, 1);
        socket.completeNextMembershipCall();
        await first;
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(socket.membershipStarts, ['joinDoc:doc-a', 'joinDoc:doc-b']);
        assert.strictEqual(socket.maxActiveMembershipCalls, 1);
        socket.completeNextMembershipCall();
        await second;
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(socket.membershipStarts, [
            'joinDoc:doc-a',
            'joinDoc:doc-b',
            'leaveDoc:doc-a',
        ]);
        assert.strictEqual(socket.maxActiveMembershipCalls, 1);
        socket.completeNextMembershipCall();
        await third;
    });

    it('continues the membership queue after one RPC is rejected', async () => {
        const SocketIOAPI = loadSocketApi();
        const socket = new ControlledMembershipSocket();
        const api = {_initSocketV0: () => socket};
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');

        const rejected = client.joinDoc('doc-rejected');
        const following = client.joinDoc('doc-following');
        await new Promise(resolve => setImmediate(resolve));
        socket.completeNextMembershipCall(new Error('rejected for test'));
        await assert.rejects(rejected, /rejected for test/);
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(socket.membershipStarts, [
            'joinDoc:doc-rejected',
            'joinDoc:doc-following',
        ]);
        assert.strictEqual(socket.maxActiveMembershipCalls, 1);
        socket.completeNextMembershipCall();
        await following;
    });

    it('invalidates a socket whose membership acknowledgement timed out', async () => {
        const SocketIOAPI = loadSocketApi();
        const socket = new ControlledMembershipSocket();
        const api = {_initSocketV0: () => socket};
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');

        const joining = client.joinDoc('doc-timeout');
        await new Promise(resolve => setImmediate(resolve));
        const timeout = new Error('simulated acknowledgement timeout');
        timeout.name = 'SocketIOTimeoutError';
        socket.completeNextMembershipCall(timeout);

        await assert.rejects(joining, /joinDoc\(doc-timeout\).*timed out/);
        assert.strictEqual(socket.disconnectCalls, 1);
        assert.strictEqual(client.needsReinit, true);
    });

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

    it('retires every listener before a late socket event can reach a handler', () => {
        const SocketIOAPI = loadSocketApi();
        let socket;
        const api = {
            _initSocketV0() {
                socket = new FakeSocket();
                return socket;
            },
        };
        const updates = [];
        const sources = [];
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');
        client.updateEventHandlers({
            onFileChanged: update => updates.push(update),
            onConnectionAccepted: source => sources.push(source),
        });
        assert(socket.listenerCount('otUpdateApplied') > 0);
        assert(socket.listenerCount('connectionAccepted') > 0);

        client.retire();
        socket.emit('otUpdateApplied', {doc: 'doc-1', v: 1});
        socket.emit('connectionAccepted', undefined, 'late-source');

        assert.deepStrictEqual(client.handlers, []);
        assert.deepStrictEqual(socket.eventNames(), []);
        assert.deepStrictEqual(updates, []);
        assert.deepStrictEqual(sources, []);
        assert.strictEqual(client.sourceId, undefined);
    });

    it('names a rebuild rather than reporting a membership refusal as a 0 ms timeout', async () => {
        const SocketIOAPI = loadSocketApi();
        const socket = new ControlledMembershipSocket();
        const api = {_initSocketV0: () => socket};
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');

        // One real acknowledgement timeout poisons the transport.
        const timedOut = client.joinDoc('doc-a');
        const timeout = new Error('simulated acknowledgement timeout');
        timeout.name = 'SocketIOTimeoutError';
        await new Promise(resolve => setImmediate(resolve));
        socket.completeNextMembershipCall(timeout);
        await assert.rejects(() => timedOut, /timed out|timeout/);
        assert.strictEqual(client.needsReinit, true);

        // Every later membership RPC is refused without reaching the socket.
        // The refusal has to say a rebuild is owed: a caller that reads it as
        // an ordinary transport timeout retries this socket forever, and no
        // retry can succeed until something calls init().
        const refused = client.joinDoc('doc-a');
        await assert.rejects(() => refused, error => (
            /invalidated and must be rebuilt/.test(error.message)
            && !/0 ms/.test(error.message)
        ));
        // The refusal never reached the dead socket at all.
        assert.deepStrictEqual(socket.membershipStarts, ['joinDoc:doc-a']);
        assert.deepStrictEqual(socket.pendingMembershipCalls, []);

        // Rebuilding the socket clears the refusal.
        client.init();
        assert.strictEqual(client.needsReinit, false);
    });

    it('announces the disconnect itself when the dead socket emits none', async () => {
        const SocketIOAPI = loadSocketApi();
        const socket = new ControlledMembershipSocket();
        const api = {_initSocketV0: () => socket};
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');
        const disconnects = [];
        client.updateEventHandlers({onDisconnected: () => disconnects.push(Date.now())});

        const joining = client.joinDoc('doc-timeout');
        await new Promise(resolve => setImmediate(resolve));
        const timeout = new Error('simulated acknowledgement timeout');
        timeout.name = 'SocketIOTimeoutError';
        socket.completeNextMembershipCall(timeout);
        await assert.rejects(joining, /timed out/);

        // The socket is already closed, so disconnect() emits nothing. The
        // reconnect that clears the invalidation is driven by this handler
        // alone: without it the transport stays poisoned and every later
        // membership RPC is refused forever.
        assert.strictEqual(socket.disconnectCalls, 1);
        assert.deepStrictEqual(socket.eventNames().includes('disconnect'), true);
        await new Promise(resolve => setTimeout(resolve, 0));
        assert.strictEqual(disconnects.length, 1);
    });

    it('does not double-announce a disconnect the socket already reported', async () => {
        const SocketIOAPI = loadSocketApi();
        const socket = new ControlledMembershipSocket();
        // A socket that still believes it is connected reports the loss itself.
        socket.disconnect = () => {
            socket.disconnectCalls += 1;
            socket.emit('disconnect');
        };
        const api = {_initSocketV0: () => socket};
        const client = new SocketIOAPI('http://example.test', api, {}, 'project');
        const disconnects = [];
        client.updateEventHandlers({onDisconnected: () => disconnects.push(Date.now())});

        const joining = client.joinDoc('doc-timeout');
        await new Promise(resolve => setImmediate(resolve));
        const timeout = new Error('simulated acknowledgement timeout');
        timeout.name = 'SocketIOTimeoutError';
        socket.completeNextMembershipCall(timeout);
        await assert.rejects(joining, /timed out/);
        await new Promise(resolve => setTimeout(resolve, 0));

        assert.strictEqual(disconnects.length, 1);
    });
});
