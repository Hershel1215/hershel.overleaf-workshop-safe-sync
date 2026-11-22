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

    it('renews the chunk deadline during a streaming download without Content-Length', async function () {
        this.timeout(10_000);
        const chunks = Array.from({length: 4}, (_, index) => Buffer.alloc(16 * 1024, index + 1));
        await withServer((_incoming, response) => {
            response.writeHead(200, {'Content-Type': 'application/octet-stream'});
            let index = 0;
            const timer = setInterval(() => {
                response.write(chunks[index]);
                index += 1;
                if (index >= chunks.length) {
                    clearInterval(timer);
                    response.end();
                }
            }, 150);
        }, async url => {
            const api = new BaseAPI(url, {baseTimeoutMs: 300, minBytesPerSecond: 1024 * 1024});
            const result = await api.getFile(identity, 'project-id', 'chunked-file');
            assert.strictEqual(
                Buffer.compare(Buffer.from(result.content), Buffer.concat(chunks)),
                0,
            );
        });
    });

    it('retries downloads on transient timeout and HTTP 503 until a later attempt succeeds', async function () {
        this.timeout(10_000);
        const payload = Buffer.from('recovered-binary-payload');
        let attempt = 0;
        await withServer((_incoming, response) => {
            attempt += 1;
            if (attempt === 1) {
                // Let the first request stall past baseTimeoutMs.
                return;
            }
            if (attempt === 2) {
                response.writeHead(503, {'Content-Type': 'text/plain'});
                response.end('upstream busy');
                return;
            }
            response.writeHead(200, {'Content-Length': payload.length});
            response.end(payload);
        }, async (url, requests) => {
            const api = new BaseAPI(url, {
                baseTimeoutMs: 120,
                minBytesPerSecond: 1024 * 1024,
                retryBaseDelayMs: 20,
            });
            const result = await api.getFile(identity, 'project-id', 'retry-file');
            assert.strictEqual(requests.length, 3);
            assert.strictEqual(Buffer.compare(Buffer.from(result.content), payload), 0);
        });
    });

    it('rejects instead of returning empty bytes on HTTP 404 or exhausted HTTP 502 retries', async function () {
        this.timeout(10_000);
        await withServer((_incoming, response) => {
            response.writeHead(404, {'Content-Type': 'text/plain'});
            response.end('missing');
        }, async (url, requests) => {
            const api = new BaseAPI(url, {
                baseTimeoutMs: 200,
                minBytesPerSecond: 1024 * 1024,
                retryBaseDelayMs: 20,
            });
            await assert.rejects(
                api.getFile(identity, 'project-id', 'missing-file'),
                /HTTP 404 downloading .*missing/,
            );
            assert.strictEqual(requests.length, 1);
        });

        await withServer((_incoming, response) => {
            response.writeHead(502, {'Content-Type': 'text/plain'});
            response.end('bad gateway');
        }, async (url, requests) => {
            const api = new BaseAPI(url, {
                baseTimeoutMs: 200,
                minBytesPerSecond: 1024 * 1024,
                retryBaseDelayMs: 20,
            });
            await assert.rejects(
                api.getFile(identity, 'project-id', 'gateway-file'),
                /HTTP 502 downloading .*bad gateway/,
            );
            assert.strictEqual(requests.length, 3);
        });
    });

    it('retries a truncated download when fewer bytes arrive than Content-Length announced', async function () {
        this.timeout(10_000);
        const payload = Buffer.from('complete-binary-bytes-after-truncation');
        let attempt = 0;
        await withServer((_incoming, response) => {
            attempt += 1;
            if (attempt === 1) {
                response.writeHead(200, {'Content-Length': payload.length});
                response.write(payload.subarray(0, 8));
                response.socket.destroy();
                return;
            }
            response.writeHead(200, {'Content-Length': payload.length});
            response.end(payload);
        }, async (url, requests) => {
            const api = new BaseAPI(url, {
                baseTimeoutMs: 200,
                minBytesPerSecond: 1024 * 1024,
                retryBaseDelayMs: 20,
            });
            const result = await api.getFile(identity, 'project-id', 'truncated-file');
            assert.strictEqual(requests.length, 2);
            assert.strictEqual(Buffer.compare(Buffer.from(result.content), payload), 0);
        });
    });
});
