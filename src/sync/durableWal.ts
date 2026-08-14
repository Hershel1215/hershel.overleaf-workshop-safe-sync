import { createHash, randomUUID } from 'crypto';
import { constants as fsConstants } from 'fs';
import { FileHandle, open, readFile, rename, rm, truncate } from 'fs/promises';
import * as path from 'path';
import { createDirectoryDurably, syncDirectoryDurably } from './durableFile';

const WAL_SCHEMA_VERSION = 1;
const CHECKPOINT_SCHEMA_VERSION = 1;
const ZERO_HASH = '0'.repeat(64);

export class WalCorruptionError extends Error {}

export interface WalRecord<T> {
    schemaVersion: 1;
    sequence: number;
    previousHash: string;
    payload: T;
    hash: string;
}

export interface DurableWalOptions {
    idleCommitMs?: number;
    maxCommitMs?: number;
}

interface PendingAppend<T> {
    payload: T;
    resolve: (record: WalRecord<T>) => void;
    reject: (error: Error) => void;
}

/**
 * A small append-only, hash-chained WAL with bounded group commit.
 *
 * Callers observe a successful append only after fdatasync completes. A torn
 * final line is discarded during recovery; corruption before the tail fails
 * closed.
 */
export class DurableWal<T> {
    private readonly pending: PendingAppend<T>[] = [];
    private readonly recoveredRecords: WalRecord<T>[];
    private readonly idleCommitMs: number;
    private readonly maxCommitMs: number;
    private writeChain: Promise<void> = Promise.resolve();
    private idleTimer?: NodeJS.Timeout;
    private maxTimer?: NodeJS.Timeout;
    private failure?: Error;
    private closed = false;
    private nextSequence: number;
    private previousHash: string;
    private durableBytes: number;

    private constructor(
        readonly filePath: string,
        private readonly handle: FileHandle,
        recoveredRecords: WalRecord<T>[],
        options: DurableWalOptions,
    ) {
        this.recoveredRecords = recoveredRecords;
        this.idleCommitMs = options.idleCommitMs ?? 50;
        this.maxCommitMs = options.maxCommitMs ?? 250;
        if (this.idleCommitMs < 0 || this.maxCommitMs <= 0 || this.idleCommitMs > this.maxCommitMs) {
            throw new Error('Invalid WAL group-commit window');
        }
        const last = recoveredRecords.at(-1);
        this.nextSequence = (last?.sequence ?? 0) + 1;
        this.previousHash = last?.hash ?? ZERO_HASH;
        this.durableBytes = recoveredRecords.reduce(
            (total, record) => total + Buffer.byteLength(`${JSON.stringify(record)}\n`),
            0,
        );
    }

    static async open<T>(
        filePath: string,
        options: DurableWalOptions = {},
    ): Promise<DurableWal<T>> {
        const directory = path.dirname(filePath);
        await createDirectoryDurably(directory, 0o700);
        let recovered: WalRecord<T>[];
        let handle: FileHandle | undefined;
        try {
            handle = await open(filePath, 'ax+');
            recovered = [];
            // fdatasync alone does not durably publish a newly created name.
            // The journaled contract starts only after both the empty inode
            // and its parent-directory entry survive a crash.
            await handle.datasync();
            await syncDirectoryDurably(directory);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
                await handle?.close().catch(() => {});
                throw error;
            }
            recovered = await recoverWal<T>(filePath);
            // Do not recreate a file that disappeared between recovery and
            // ownership. That race means another actor changed the WAL path.
            handle = await open(filePath, fsConstants.O_RDWR | fsConstants.O_APPEND);
        }
        if (!handle) {
            throw new Error(`WAL handle was not opened: ${filePath}`);
        }
        try {
            return new DurableWal(filePath, handle, recovered, options);
        } catch (error) {
            await handle.close().catch(() => {});
            throw error;
        }
    }

    /**
     * Durably create a new, empty WAL generation.
     *
     * Generation paths are unique, so publishing the file itself does not
     * select it. The caller must durably publish its generation selector only
     * after this promise resolves.
     */
    static async createEmpty(filePath: string): Promise<void> {
        const directory = path.dirname(filePath);
        await createDirectoryDurably(directory, 0o700);
        const handle = await open(filePath, 'wx');
        try {
            await handle.datasync();
        } finally {
            await handle.close();
        }
        await syncDirectoryDurably(directory);
    }

    /**
     * Atomically replace a closed WAL with an empty file.
     *
     * This deliberately never truncates the selected WAL in place: before the
     * rename recovery sees the old complete WAL, and afterwards it sees the
     * new empty WAL.
     */
    static async replaceWithEmpty(filePath: string): Promise<void> {
        const directory = path.dirname(filePath);
        await createDirectoryDurably(directory, 0o700);
        const temporaryPath = `${filePath}.${process.pid}.${randomUUID()}.empty`;
        const temporary = await open(temporaryPath, 'wx');
        try {
            await temporary.datasync();
        } finally {
            await temporary.close();
        }
        try {
            await rename(temporaryPath, filePath);
            await syncDirectoryDurably(directory);
        } catch (error) {
            await rm(temporaryPath, { force: true }).catch(() => {});
            throw error;
        }
    }

    get records(): readonly WalRecord<T>[] {
        return this.recoveredRecords;
    }

    get byteLength(): number {
        return this.durableBytes;
    }

    append(payload: T, barrier = false): Promise<WalRecord<T>> {
        if (this.closed) {
            return Promise.reject(new Error('WAL is closed'));
        }
        if (this.failure) {
            return Promise.reject(this.failure);
        }
        const result = new Promise<WalRecord<T>>((resolve, reject) => {
            this.pending.push({ payload, resolve, reject });
        });
        if (barrier) {
            void this.commitPending().catch(() => {});
        } else {
            this.scheduleCommit();
        }
        return result;
    }

    async flush(): Promise<void> {
        this.clearTimers();
        while (this.pending.length > 0) {
            await this.commitPending();
        }
        await this.writeChain;
        if (this.failure) {
            throw this.failure;
        }
    }

    async close(): Promise<void> {
        if (this.closed) {
            return;
        }
        try {
            await this.flush();
        } finally {
            this.closed = true;
            this.clearTimers();
            await this.handle.close();
        }
    }

    private scheduleCommit(): void {
        if (this.idleTimer) {
            clearTimeout(this.idleTimer);
        }
        this.idleTimer = setTimeout(() => void this.commitPending().catch(() => {}), this.idleCommitMs);
        if (!this.maxTimer) {
            this.maxTimer = setTimeout(() => void this.commitPending().catch(() => {}), this.maxCommitMs);
        }
    }

    private commitPending(): Promise<void> {
        this.clearTimers();
        const batch = this.pending.splice(0);
        if (batch.length === 0) {
            return this.writeChain;
        }

        const commit = this.writeChain.then(async () => {
            if (this.failure) {
                throw this.failure;
            }
            let nextSequence = this.nextSequence;
            let previousHash = this.previousHash;
            const records: WalRecord<T>[] = [];
            for (const item of batch) {
                const body = {
                    schemaVersion: WAL_SCHEMA_VERSION as 1,
                    sequence: nextSequence,
                    previousHash,
                    payload: item.payload,
                };
                const record: WalRecord<T> = {
                    ...body,
                    hash: hashJson(body),
                };
                records.push(record);
                nextSequence++;
                previousHash = record.hash;
            }

            const encoded = records.map(record => `${JSON.stringify(record)}\n`).join('');
            await this.handle.writeFile(encoded, { encoding: 'utf8' });
            await this.handle.datasync();

            this.nextSequence = nextSequence;
            this.previousHash = previousHash;
            this.durableBytes += Buffer.byteLength(encoded);
            this.recoveredRecords.push(...records);
            for (let index = 0; index < batch.length; index++) {
                batch[index].resolve(records[index]);
            }
        });

        const observed = commit.catch(error => {
            const normalized = error instanceof Error ? error : new Error(String(error));
            this.failure = normalized;
            for (const item of batch) {
                item.reject(normalized);
            }
            throw normalized;
        });
        this.writeChain = observed.then(() => undefined, () => undefined);
        return observed;
    }

    private clearTimers(): void {
        if (this.idleTimer) {
            clearTimeout(this.idleTimer);
            this.idleTimer = undefined;
        }
        if (this.maxTimer) {
            clearTimeout(this.maxTimer);
            this.maxTimer = undefined;
        }
    }
}

export interface DurableCheckpoint<T> {
    walSequence: number;
    state: T;
}

interface CheckpointEnvelope<T> extends DurableCheckpoint<T> {
    schemaVersion: 1;
    hash: string;
}

export class AtomicCheckpointStore<T> {
    constructor(readonly filePath: string) {}

    async load(): Promise<DurableCheckpoint<T> | undefined> {
        let encoded: string;
        try {
            encoded = await readFile(this.filePath, 'utf8');
        } catch (error) {
            if (isMissingFile(error)) {
                return undefined;
            }
            throw error;
        }
        let envelope: CheckpointEnvelope<T>;
        try {
            envelope = JSON.parse(encoded) as CheckpointEnvelope<T>;
        } catch (error) {
            throw new WalCorruptionError(`Invalid checkpoint JSON: ${String(error)}`);
        }
        const { hash, ...body } = envelope;
        if (
            body.schemaVersion !== CHECKPOINT_SCHEMA_VERSION
            || !Number.isInteger(body.walSequence)
            || body.walSequence < 0
            || hash !== hashJson(body)
        ) {
            throw new WalCorruptionError('Checkpoint checksum or schema is invalid');
        }
        return { walSequence: body.walSequence, state: body.state };
    }

    async save(checkpoint: DurableCheckpoint<T>): Promise<void> {
        if (!Number.isInteger(checkpoint.walSequence) || checkpoint.walSequence < 0) {
            throw new Error('Invalid checkpoint WAL sequence');
        }
        await createDirectoryDurably(path.dirname(this.filePath), 0o700);
        const body = {
            schemaVersion: CHECKPOINT_SCHEMA_VERSION as 1,
            walSequence: checkpoint.walSequence,
            state: checkpoint.state,
        };
        const envelope: CheckpointEnvelope<T> = { ...body, hash: hashJson(body) };
        const temporaryPath = `${this.filePath}.${process.pid}.${randomUUID()}.tmp`;
        const temporary = await open(temporaryPath, 'wx');
        try {
            await temporary.writeFile(JSON.stringify(envelope), { encoding: 'utf8' });
            await temporary.datasync();
        } finally {
            await temporary.close();
        }
        await rename(temporaryPath, this.filePath);
        await syncDirectoryDurably(path.dirname(this.filePath));
    }
}

async function recoverWal<T>(filePath: string): Promise<WalRecord<T>[]> {
    let content: Buffer;
    try {
        content = await readFile(filePath);
    } catch (error) {
        if (isMissingFile(error)) {
            return [];
        }
        throw error;
    }

    const records: WalRecord<T>[] = [];
    let offset = 0;
    let validBytes = 0;
    let expectedSequence = 1;
    let previousHash = ZERO_HASH;
    while (offset < content.length) {
        const newline = content.indexOf(0x0a, offset);
        if (newline === -1) {
            await truncate(filePath, validBytes);
            break;
        }
        const line = content.subarray(offset, newline).toString('utf8');
        offset = newline + 1;
        if (line.length === 0) {
            throw new WalCorruptionError(`Empty WAL record at byte ${validBytes}`);
        }
        let record: WalRecord<T>;
        try {
            record = JSON.parse(line) as WalRecord<T>;
        } catch (error) {
            throw new WalCorruptionError(`Invalid WAL JSON at byte ${validBytes}: ${String(error)}`);
        }
        const { hash, ...body } = record;
        if (
            body.schemaVersion !== WAL_SCHEMA_VERSION
            || body.sequence !== expectedSequence
            || body.previousHash !== previousHash
            || hash !== hashJson(body)
        ) {
            throw new WalCorruptionError(`Invalid WAL chain at sequence ${expectedSequence}`);
        }
        records.push(record);
        expectedSequence++;
        previousHash = hash;
        validBytes = offset;
    }
    return records;
}

function hashJson(value: unknown): string {
    return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}

function isMissingFile(error: unknown): boolean {
    return error instanceof Error && 'code' in error && error.code === 'ENOENT';
}
