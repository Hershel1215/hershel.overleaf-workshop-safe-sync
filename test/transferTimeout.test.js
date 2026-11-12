const assert = require('assert');
const http = require('http');
const {BaseAPI, transferTimeoutMs} = require('../out/api/base');

const identity = {csrfToken: 'csrf', cookies: 'session=value'};

async function withServer(handler, run) {
    const requests = [];
    const server = http.createServer((incoming, response) => {
        requests.push(incoming.url);
        handler(incoming, response);
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    try {
        const {port} = server.address();
        await run(`http://127.0.0.1:${port}/`, requests);
    } finally {
        server.closeAllConnections();
        await new Promise(resolve => server.close(resolve));
    }
}

describe('size-aware HTTP transfer deadlines', () => {
    it('adds the payload time at the floor rate to the base ceiling', () => {
        assert.strictEqual(transferTimeoutMs(0), 60_000);
        // A 7.4 MB figure on a 10 KB/s link needs about 12.5 minutes.
        assert(transferTimeoutMs(7_400_983) > 12.5 * 60_000);
        assert.strictEqual(
            transferTimeoutMs(64 * 1024, {baseTimeoutMs: 200, minBytesPerSecond: 64 * 1024}),
            1200,
        );
    });

    it('lets an upload outlast the base ceiling when its body needs the time', async function () {
        this.timeout(10_000);
        const payload = Buffer.alloc(256 * 1024, 7);
        await withServer((incoming, response) => {
            incoming.resume();
            incoming.on('end', () => setTimeout(() => {
                response.setHeader('Content-Type', 'application/json');
                response.end(JSON.stringify({success: true, entity_id: 'big-id', entity_type: 'file'}));
            }, 800));
        }, async url => {
            const api = new BaseAPI(url, {baseTimeoutMs: 200, minBytesPerSecond: 64 * 1024});
            const result = await api.uploadFile(identity, 'project-id', 'folder-id', 'figure.png', payload);
            assert.strictEqual(result.type, 'success', result.message);
            assert.strictEqual(result.entity._id, 'big-id');
        });
    });

    it('lets a download outlast the base ceiling when its announced size needs the time', async function () {
        this.timeout(10_000);
        const payload = Buffer.alloc(256 * 1024, 3);
        await withServer((incoming, response) => {
            response.writeHead(200, {'Content-Length': payload.length});
            response.flushHeaders();
            const chunk = payload.length / 4;
            let sent = 0;
            const timer = setInterval(() => {
                response.write(payload.subarray(sent, sent + chunk));
                sent += chunk;
                if (sent >= payload.length) {
                    clearInterval(timer);
                    response.end();
                }
            }, 200);
        }, async url => {
            const api = new BaseAPI(url, {baseTimeoutMs: 200, minBytesPerSecond: 64 * 1024});
            const result = await api.getFile(identity, 'project-id', 'file-id');
            assert.strictEqual(Buffer.compare(Buffer.from(result.content), payload), 0);
        });
    });

    it('does not resend a large body after its deadline expires', async function () {
        this.timeout(10_000);
        const payload = Buffer.alloc(1536 * 1024, 1);
        await withServer(incoming => {
            incoming.resume();
            // Never answer: the client deadline must end the attempt.
        }, async (url, requests) => {
            const api = new BaseAPI(url, {baseTimeoutMs: 300, minBytesPerSecond: 100 * 1024 * 1024});
            const result = await api.uploadFile(identity, 'project-id', 'folder-id', 'figure.png', payload);
            assert.strictEqual(result.type, 'error');
            assert.match(result.message, /aborted due to timeout/);
            assert.match(result.message, /upload did not finish within/);
            // A retry would arrive after the 1 s backoff.
            await new Promise(resolve => setTimeout(resolve, 1500));
            assert.strictEqual(requests.length, 1);
        });
    });
});
