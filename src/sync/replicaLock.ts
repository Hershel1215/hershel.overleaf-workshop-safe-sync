import { createHash, randomUUID } from 'crypto';
import { readFileSync, rmSync } from 'fs';
import { mkdir, readFile, rename, rm, stat, writeFile } from 'fs/promises';
import * as path from 'path';

interface LockOwner {
    instanceId: string;
    pid: number;
    heartbeat: number;
}

export class ReplicaLockedError extends Error {
    constructor(readonly owner?: LockOwner) {
        super(owner
            ? `Local replica is already owned by process ${owner.pid}`
            : 'Local replica is already owned by another extension host');
    }
}

/** Cross-process directory lease: one writer per local replica directory. */
export class ReplicaLock {
    private readonly instanceId = randomUUID();
    private readonly lockDirectory: string;
    private readonly ownerPath: string;
    private heartbeatTimer?: NodeJS.Timeout;
    private released = false;

    private constructor(
        rootDirectory: string,
        key: string,
        private readonly heartbeatMs: number,
        private readonly onLost: (error: Error) => void,
    ) {
        const digest = createHash('sha256').update(key).digest('hex');
        this.lockDirectory = path.join(rootDirectory, 'safe-sync', 'locks', `${digest}.lock`);
        this.ownerPath = path.join(this.lockDirectory, 'owner.json');
    }

    static async acquire(
        rootDirectory: string,
        key: string,
        options: {
            ttlMs?: number;
            heartbeatMs?: number;
            onLost?: (error: Error) => void;
        } = {},
    ): Promise<ReplicaLock> {
        const ttlMs = options.ttlMs ?? 15000;
        const heartbeatMs = options.heartbeatMs ?? 5000;
        const lock = new ReplicaLock(rootDirectory, key, heartbeatMs, options.onLost ?? (() => {}));
        await mkdir(path.dirname(lock.lockDirectory), { recursive: true, mode: 0o700 });

        for (let attempt = 0; attempt < 5; attempt++) {
            try {
                await mkdir(lock.lockDirectory, { mode: 0o700 });
                await lock.writeOwner();
                lock.startHeartbeat();
                return lock;
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
                    throw error;
                }
                const recoveryDirectory = `${lock.lockDirectory}.recovery`;
                try {
                    await mkdir(recoveryDirectory, { mode: 0o700 });
                } catch (recoveryError) {
                    if ((recoveryError as NodeJS.ErrnoException).code !== 'EEXIST') {
                        throw recoveryError;
                    }
                    try {
                        const recoveryStat = await stat(recoveryDirectory);
                        if (Date.now() - recoveryStat.mtimeMs > ttlMs) {
                            await rm(recoveryDirectory, { recursive: true, force: true });
                        }
                    } catch (statError) {
                        if ((statError as NodeJS.ErrnoException).code !== 'ENOENT') {
                            throw statError;
                        }
                    }
                    await new Promise(resolve => setTimeout(resolve, 25));
                    continue;
                }
                try {
                    const owner = await lock.readOwner();
                    if (owner && Date.now() - owner.heartbeat <= ttlMs) {
                        throw new ReplicaLockedError(owner);
                    }
                    await rm(lock.lockDirectory, { recursive: true, force: true });
                } finally {
                    await rm(recoveryDirectory, { recursive: true, force: true });
                }
            }
        }
        throw new ReplicaLockedError();
    }

    async release(): Promise<void> {
        if (this.released) {
            return;
        }
        this.released = true;
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = undefined;
        }
        const owner = await this.readOwner();
        if (owner?.instanceId === this.instanceId) {
            await rm(this.lockDirectory, { recursive: true, force: true });
        }
    }

    releaseSync(): void {
        if (this.released) {
            return;
        }
        this.released = true;
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = undefined;
        }
        try {
            const owner = JSON.parse(readFileSync(this.ownerPath, 'utf8')) as LockOwner;
            if (owner.instanceId === this.instanceId) {
                rmSync(this.lockDirectory, { recursive: true, force: true });
            }
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                throw error;
            }
        }
    }

    private startHeartbeat(): void {
        this.heartbeatTimer = setInterval(() => {
            this.heartbeat().catch(error => {
                if (!this.released) {
                    this.onLost(error instanceof Error ? error : new Error(String(error)));
                }
            });
        }, this.heartbeatMs);
        this.heartbeatTimer.unref();
    }

    private async heartbeat(): Promise<void> {
        const owner = await this.readOwner();
        if (owner?.instanceId !== this.instanceId) {
            throw new Error('Local replica lock ownership was lost');
        }
        await this.writeOwner();
    }

    private async writeOwner(): Promise<void> {
        const temporary = path.join(this.lockDirectory, `${this.instanceId}.tmp`);
        const owner: LockOwner = {
            instanceId: this.instanceId,
            pid: process.pid,
            heartbeat: Date.now(),
        };
        await writeFile(temporary, JSON.stringify(owner), { mode: 0o600 });
        try {
            await rename(temporary, this.ownerPath);
        } catch (error) {
            const code = (error as NodeJS.ErrnoException).code;
            if (code !== 'EEXIST' && code !== 'EPERM' && code !== 'ENOTEMPTY') {
                throw error;
            }
            await rm(this.ownerPath, { force: true });
            await rename(temporary, this.ownerPath);
        }
    }

    private async readOwner(): Promise<LockOwner | undefined> {
        try {
            return JSON.parse(await readFile(this.ownerPath, 'utf8')) as LockOwner;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                return undefined;
            }
            throw error;
        }
    }
}
