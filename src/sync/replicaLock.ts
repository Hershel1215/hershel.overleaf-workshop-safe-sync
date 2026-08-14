import { createHash, randomUUID } from 'crypto';
import { readFileSync, rmSync } from 'fs';
import { mkdir, readFile, rename, rm, writeFile } from 'fs/promises';
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
    private heartbeatInFlight?: Promise<void>;
    private releasePromise?: Promise<void>;
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
                await lock.publishOwner();
                lock.startHeartbeat();
                return lock;
            } catch (error) {
                const code = (error as NodeJS.ErrnoException).code;
                if (code !== 'EEXIST' && code !== 'ENOTEMPTY') {
                    throw error;
                }
                const recoveryDirectory = `${lock.lockDirectory}.recovery`;
                if (!await lock.acquireRecoveryGuard(recoveryDirectory)) {
                    await new Promise(resolve => setTimeout(resolve, 25));
                    continue;
                }
                try {
                    const owner = await lock.readOwner();
                    if (
                        owner
                        && (
                            Date.now() - owner.heartbeat <= ttlMs
                            || ReplicaLock.isProcessAlive(owner.pid)
                        )
                    ) {
                        throw new ReplicaLockedError(owner);
                    }
                    await rm(lock.lockDirectory, { recursive: true, force: true });
                } finally {
                    await lock.releaseRecoveryGuard(recoveryDirectory);
                }
            }
        }
        throw new ReplicaLockedError();
    }

    async release(): Promise<void> {
        if (this.releasePromise) {
            return this.releasePromise;
        }
        if (this.released) {
            return;
        }
        this.stopRenewing();
        const heartbeat = this.heartbeatInFlight;
        this.releasePromise = (async () => {
            if (heartbeat) {
                try {
                    await heartbeat;
                } catch {
                    // Ownership is checked again below after the renewal settles.
                }
            }
            const owner = await this.readOwner();
            if (owner?.instanceId === this.instanceId) {
                await rm(this.lockDirectory, { recursive: true, force: true });
            }
        })();
        return this.releasePromise;
    }

    releaseSync(): void {
        if (this.released) {
            return;
        }
        this.stopRenewing();
        if (this.heartbeatInFlight) {
            // A synchronous caller cannot safely delete a lease while an older
            // renewal may still write. Leave it for TTL recovery instead.
            return;
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

    /** Stop renewing without deleting the lease; a successor waits for TTL. */
    abandon(): void {
        if (this.released) {
            return;
        }
        this.stopRenewing();
    }

    private startHeartbeat(): void {
        this.heartbeatTimer = setInterval(() => {
            if (this.released || this.heartbeatInFlight) {
                return;
            }
            this.heartbeat().catch(error => {
                if (!this.released) {
                    this.onLost(error instanceof Error ? error : new Error(String(error)));
                }
            });
        }, this.heartbeatMs);
        this.heartbeatTimer.unref();
    }

    private heartbeat(): Promise<void> {
        if (this.released) {
            return Promise.resolve();
        }
        if (this.heartbeatInFlight) {
            return this.heartbeatInFlight;
        }
        let tracked: Promise<void>;
        tracked = this.renewHeartbeat().then(
            () => {
                if (this.heartbeatInFlight === tracked) {
                    this.heartbeatInFlight = undefined;
                }
            },
            error => {
                if (this.heartbeatInFlight === tracked) {
                    this.heartbeatInFlight = undefined;
                }
                throw error;
            },
        );
        this.heartbeatInFlight = tracked;
        return tracked;
    }

    private async renewHeartbeat(): Promise<void> {
        const owner = await this.readOwner();
        if (owner?.instanceId !== this.instanceId) {
            throw new Error('Local replica lock ownership was lost');
        }
        await this.writeOwner();
    }

    private stopRenewing(): void {
        this.released = true;
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = undefined;
        }
    }

    private async writeOwner(): Promise<void> {
        const temporary = path.join(this.lockDirectory, `${this.instanceId}.tmp`);
        await writeFile(temporary, JSON.stringify(this.currentOwner()), { mode: 0o600 });
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

    /** Publish the lock directory and owner identity in one atomic rename. */
    private async publishOwner(): Promise<void> {
        const temporary = `${this.lockDirectory}.${this.instanceId}.acquire`;
        await rm(temporary, {recursive: true, force: true});
        await mkdir(temporary, {mode: 0o700});
        await writeFile(
            path.join(temporary, 'owner.json'),
            JSON.stringify(this.currentOwner()),
            {mode: 0o600},
        );
        try {
            await rename(temporary, this.lockDirectory);
        } catch (error) {
            await rm(temporary, {recursive: true, force: true});
            throw error;
        }
    }

    private currentOwner(): LockOwner {
        return {
            instanceId: this.instanceId,
            pid: process.pid,
            heartbeat: Date.now(),
        };
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

    /**
     * Publish a complete recovery identity atomically. A live recovery owner
     * can never be evicted merely because its filesystem operation is slow.
     */
    private async acquireRecoveryGuard(recoveryDirectory: string): Promise<boolean> {
        const temporary = `${recoveryDirectory}.${this.instanceId}.tmp`;
        await rm(temporary, { recursive: true, force: true });
        await mkdir(temporary, { mode: 0o700 });
        await writeFile(
            path.join(temporary, 'owner.json'),
            JSON.stringify({
                instanceId: this.instanceId,
                pid: process.pid,
                heartbeat: Date.now(),
            } satisfies LockOwner),
            { mode: 0o600 },
        );
        try {
            await rename(temporary, recoveryDirectory);
            return true;
        } catch (error) {
            await rm(temporary, { recursive: true, force: true });
            const code = (error as NodeJS.ErrnoException).code;
            if (code !== 'EEXIST' && code !== 'ENOTEMPTY') {
                throw error;
            }
        }

        let owner: LockOwner;
        try {
            owner = JSON.parse(
                await readFile(path.join(recoveryDirectory, 'owner.json'), 'utf8'),
            ) as LockOwner;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                return false;
            }
            throw error;
        }
        if (ReplicaLock.isProcessAlive(owner.pid)) {
            return false;
        }
        const quarantine = `${recoveryDirectory}.${randomUUID()}.stale`;
        try {
            await rename(recoveryDirectory, quarantine);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                return false;
            }
            throw error;
        }
        await rm(quarantine, { recursive: true, force: true });
        return false;
    }

    private async releaseRecoveryGuard(recoveryDirectory: string): Promise<void> {
        try {
            const owner = JSON.parse(
                await readFile(path.join(recoveryDirectory, 'owner.json'), 'utf8'),
            ) as LockOwner;
            if (owner.instanceId === this.instanceId) {
                await rm(recoveryDirectory, { recursive: true, force: true });
            }
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                throw error;
            }
        }
    }

    /** A delayed heartbeat is not proof that a live local writer is dead. */
    private static isProcessAlive(pid: number): boolean {
        if (!Number.isSafeInteger(pid) || pid <= 0) {
            return false;
        }
        try {
            process.kill(pid, 0);
            return true;
        } catch (error) {
            return (error as NodeJS.ErrnoException).code !== 'ESRCH';
        }
    }
}
