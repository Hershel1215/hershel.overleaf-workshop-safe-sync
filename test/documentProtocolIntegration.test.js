const assert = require('assert');
const { DocumentSession } = require('../out/sync/documentSession');
const { TextOperation } = require('../out/sync/textOperation');

async function flush() {
    await new Promise(resolve => setImmediate(resolve));
}

class VirtualOtServer {
    constructor(content) {
        this.content = content;
        this.snapshots = [content];
        this.history = [];
        this.clients = [];
        this.queue = [];
    }

    async addClient(id) {
        const persisted = [];
        const errors = [];
        const session = await DocumentSession.create({
            documentId: 'doc',
            content: this.content,
            revision: this.history.length,
            sendUpdate: async update => { this.queue.push({ id, update }); },
            persist: async state => { persisted.push(structuredClone(state)); },
            getSourceId: () => id,
            onChange: () => {},
            onError: error => errors.push(error),
            retryDelayMs: 100000,
            fatalDelayMs: 100000,
        });
        const client = { id, session, persisted, errors };
        this.clients.push(client);
        return client;
    }

    async process(queueIndex = 0) {
        const [{ id, update }] = this.queue.splice(queueIndex, 1);
        const base = this.snapshots[update.v];
        let operation = TextOperation.fromLegacyEdits(base, update.op);
        for (let version = update.v; version < this.history.length; version++) {
            [operation] = TextOperation.transform(operation, this.history[version]);
        }
        const version = this.history.length;
        const before = this.content;
        this.content = operation.apply(before);
        this.history.push(operation);
        this.snapshots.push(this.content);

        for (const client of this.clients) {
            if (client.id === id) {
                await client.session.receiveUpdate({ doc: 'doc', v: version });
            } else {
                await client.session.receiveUpdate({
                    doc: 'doc',
                    v: version,
                    op: operation.toLegacyEdits(before),
                    meta: { source: id },
                });
            }
        }
    }

    async drain(reverseFirstPair = false) {
        if (reverseFirstPair && this.queue.length >= 2) {
            await this.process(1);
        }
        while (this.queue.length > 0) {
            await this.process(0);
        }
    }

    dispose() {
        for (const client of this.clients) {
            client.session.dispose();
        }
    }
}

function seededRandom(seed) {
    return () => {
        seed |= 0;
        seed = seed + 0x6D2B79F5 | 0;
        let value = Math.imul(seed ^ seed >>> 15, 1 | seed);
        value = value + Math.imul(value ^ value >>> 7, 61 | value) ^ value;
        return ((value ^ value >>> 14) >>> 0) / 4294967296;
    };
}

function randomTarget(value, random) {
    const boundaries = [0];
    let offset = 0;
    for (const character of value) {
        offset += character.length;
        boundaries.push(offset);
    }
    const startIndex = Math.floor(random() * boundaries.length);
    const start = boundaries[startIndex];
    const maxDeletePoints = Math.min(3, boundaries.length - startIndex - 1);
    const deletePoints = Math.floor(random() * (maxDeletePoints + 1));
    const deleteCount = boundaries[startIndex + deletePoints] - start;
    const alphabet = ['L', 'R', '中', '😀', '\n'];
    const insert = alphabet[Math.floor(random() * alphabet.length)];
    return value.slice(0, start) + insert + value.slice(start + deleteCount);
}

describe('document protocol integration', () => {
    it('converges two clients when the server accepts their overlapping edits in reverse order', async () => {
        const server = new VirtualOtServer('ab');
        const left = await server.addClient('left');
        const right = await server.addClient('right');
        const leftWrite = left.session.submitSnapshot('aLb');
        const rightWrite = right.session.submitSnapshot('aRb');
        await flush();

        await server.drain(true);
        await Promise.all([leftWrite, rightWrite]);
        assert.strictEqual(left.session.content, server.content);
        assert.strictEqual(right.session.content, server.content);
        assert.strictEqual(server.content, 'aLRb');
        server.dispose();
    });

    it('converges randomized overlapping edits and composed pending edits', async () => {
        for (let run = 1; run <= 250; run++) {
            const random = seededRandom(run);
            const server = new VirtualOtServer('abcdef');
            const left = await server.addClient(`left-${run}`);
            const right = await server.addClient(`right-${run}`);
            const firstLeft = randomTarget(left.session.content, random);
            const firstRight = randomTarget(right.session.content, random);
            const writes = [
                left.session.submitSnapshot(firstLeft),
                right.session.submitSnapshot(firstRight),
            ];
            const secondLeft = randomTarget(firstLeft, random);
            writes.push(left.session.submitSnapshot(secondLeft));
            await flush();

            await server.drain(run % 2 === 0);
            await Promise.all(writes);
            assert.strictEqual(left.session.content, server.content, `left diverged for seed ${run}`);
            assert.strictEqual(right.session.content, server.content, `right diverged for seed ${run}`);
            assert.deepStrictEqual(left.errors, []);
            assert.deepStrictEqual(right.errors, []);
            server.dispose();
        }
    });
});
