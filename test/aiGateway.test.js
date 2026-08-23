const assert = require('assert');
const { mkdtemp, readFile, rm, writeFile } = require('fs/promises');
const os = require('os');
const path = require('path');
const {
    AiGatewayServer,
    sendAiGatewayRequest,
} = require('../out/sync/aiGateway');

describe('AI gateway transport', () => {
    let directory;
    let server;

    beforeEach(async () => {
        directory = await mkdtemp(path.join(os.tmpdir(), 'hershel-ai-gateway-'));
    });

    afterEach(async () => {
        await server?.stop();
        await rm(directory, { recursive: true, force: true });
    });

    it('authenticates one request and keeps metadata private', async () => {
        const metadataPath = path.join(directory, 'gateway.json');
        server = new AiGatewayServer(metadataPath, {
            handle: async request => ({ method: request.method, path: request.path }),
        }, path.join(directory, 'gateway.sock'));
        const metadata = await server.start();
        const response = await sendAiGatewayRequest(metadataPath, {
            method: 'begin',
            path: 'main.tex',
        });
        assert.deepStrictEqual(response, { method: 'begin', path: 'main.tex' });
        assert.strictEqual(JSON.parse(await readFile(metadataPath, 'utf8')).token, metadata.token);
        await server.stop();
        server = undefined;
    });

    it('keeps the request socket open while a queued handler completes', async () => {
        const metadataPath = path.join(directory, 'gateway.json');
        server = new AiGatewayServer(metadataPath, {
            handle: async () => {
                await new Promise(resolve => setTimeout(resolve, 100));
                return { delayed: true };
            },
        }, path.join(directory, 'gateway.sock'));
        await server.start();
        const response = await sendAiGatewayRequest(metadataPath, {
            method: 'commit',
            path: 'main.tex',
        }, 500);
        assert.deepStrictEqual(response, { delayed: true });
    });

    it('identifies a timed-out handoff by its replica path', async () => {
        const metadataPath = path.join(directory, 'gateway.json');
        server = new AiGatewayServer(metadataPath, {
            handle: async () => {
                await new Promise(resolve => setTimeout(resolve, 50));
                return { delayed: true };
            },
        }, path.join(directory, 'gateway.sock'));
        await server.start();

        await assert.rejects(
            () => sendAiGatewayRequest(metadataPath, {
                method: 'handoff',
                replicaPath: '/tmp/hershel-replica',
            }, 5),
            /handoff request timed out for \/tmp\/hershel-replica/,
        );
        await new Promise(resolve => setTimeout(resolve, 60));
    });

    it('preserves successor metadata and lets a handoff handler stop without deadlocking', async () => {
        const metadataPath = path.join(directory, 'gateway.json');
        server = new AiGatewayServer(metadataPath, {
            handle: async () => ({ ok: true }),
        }, path.join(directory, 'old.sock'));
        await server.start();
        const successor = {
            schemaVersion: 1,
            socketPath: path.join(directory, 'successor.sock'),
            token: 'successor-token',
        };
        await writeFile(metadataPath, JSON.stringify(successor));
        await server.stop();
        assert.deepStrictEqual(JSON.parse(await readFile(metadataPath, 'utf8')), successor);

        let handoffServer;
        const handoffSuccessor = {
            schemaVersion: 1,
            socketPath: path.join(directory, 'handoff-successor.sock'),
            token: 'handoff-successor-token',
        };
        handoffServer = new AiGatewayServer(metadataPath, {
            handle: async () => {
                await writeFile(metadataPath, JSON.stringify(handoffSuccessor));
                await handoffServer.stopForHandoff();
                return { released: true };
            },
        }, path.join(directory, 'handoff.sock'));
        server = handoffServer;
        await server.start();
        const response = await sendAiGatewayRequest(metadataPath, {
            method: 'handoff',
        }, 500);

        assert.deepStrictEqual(response, { released: true });
        assert.deepStrictEqual(
            JSON.parse(await readFile(metadataPath, 'utf8')),
            handoffSuccessor,
        );
    });

    it('rejects an unauthenticated socket request', async () => {
        const metadataPath = path.join(directory, 'gateway.json');
        server = new AiGatewayServer(metadataPath, { handle: async () => ({ ok: true }) }, path.join(directory, 'gateway.sock'));
        const metadata = await server.start();
        const net = require('net');
        const response = await new Promise(resolve => {
            const socket = net.createConnection(metadata.socketPath);
            let data = '';
            socket.on('connect', () => socket.end(JSON.stringify({
                token: 'wrong', requestId: 'bad', method: 'begin',
            }) + '\n'));
            socket.on('data', chunk => { data += chunk; });
            socket.on('close', () => resolve(JSON.parse(data.trim())));
        });
        assert.strictEqual(response.ok, false);
        assert.match(response.error, /authentication/);
    });
});
