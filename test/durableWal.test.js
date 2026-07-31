const assert = require('assert');
const { mkdtemp, readFile, rm, writeFile } = require('fs/promises');
const os = require('os');
const path = require('path');
const {
    AtomicCheckpointStore,
    DurableWal,
    WalCorruptionError,
} = require('../out/sync/durableWal');

describe('durable safe-sync WAL', () => {
    let directory;
    let walPath;

    beforeEach(async () => {
        directory = await mkdtemp(path.join(os.tmpdir(), 'hershel-wal-'));
        walPath = path.join(directory, 'replica.wal');
    });

    afterEach(async () => {
        await rm(directory, { recursive: true, force: true });
    });

    it('commits a batch in sequence and recovers the hash chain', async () => {
        const wal = await DurableWal.open(walPath, { idleCommitMs: 1000, maxCommitMs: 2000 });
        const first = wal.append({ kind: 'local', value: 1 });
        const second = wal.append({ kind: 'local', value: 2 });
        await wal.flush();
        const [firstRecord, secondRecord] = await Promise.all([first, second]);
        assert.strictEqual(firstRecord.sequence, 1);
        assert.strictEqual(secondRecord.sequence, 2);
        assert.strictEqual(secondRecord.previousHash, firstRecord.hash);
        await wal.close();

        const recovered = await DurableWal.open(walPath);
        assert.deepStrictEqual(
            recovered.records.map(record => record.payload.value),
            [1, 2],
        );
        await recovered.close();
    });

    it('uses a barrier to make an append durable immediately', async () => {
        const wal = await DurableWal.open(walPath, { idleCommitMs: 1000, maxCommitMs: 2000 });
        const record = await wal.append({ kind: 'barrier' }, true);
        assert.strictEqual(record.sequence, 1);
        assert.match(await readFile(walPath, 'utf8'), /barrier/);
        await wal.close();
    });

    it('discards only a torn final record', async () => {
        const wal = await DurableWal.open(walPath, { idleCommitMs: 1, maxCommitMs: 5 });
        await wal.append({ kind: 'complete' }, true);
        await wal.close();
        const valid = await readFile(walPath, 'utf8');
        await writeFile(walPath, `${valid}{"schemaVersion":1`, 'utf8');

        const recovered = await DurableWal.open(walPath);
        assert.strictEqual(recovered.records.length, 1);
        await recovered.close();
        assert.strictEqual(await readFile(walPath, 'utf8'), valid);
    });

    it('fails closed on a corrupted committed record', async () => {
        const wal = await DurableWal.open(walPath);
        await wal.append({ kind: 'original' }, true);
        await wal.close();
        const encoded = (await readFile(walPath, 'utf8')).replace('original', 'tampered');
        await writeFile(walPath, encoded, 'utf8');
        await assert.rejects(
            () => DurableWal.open(walPath),
            error => error instanceof WalCorruptionError && /chain/.test(error.message),
        );
    });

    it('atomically stores and validates checkpoints', async () => {
        const checkpointPath = path.join(directory, 'checkpoint.json');
        const store = new AtomicCheckpointStore(checkpointPath);
        assert.strictEqual(await store.load(), undefined);
        await store.save({ walSequence: 7, state: { phase: 'ready' } });
        assert.deepStrictEqual(await store.load(), {
            walSequence: 7,
            state: { phase: 'ready' },
        });

        const encoded = (await readFile(checkpointPath, 'utf8')).replace('ready', 'broken');
        await writeFile(checkpointPath, encoded, 'utf8');
        await assert.rejects(
            () => store.load(),
            error => error instanceof WalCorruptionError && /checksum/.test(error.message),
        );
    });
});
