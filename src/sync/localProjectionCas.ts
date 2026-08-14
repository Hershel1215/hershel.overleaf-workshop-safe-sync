import { createHash, randomUUID } from 'crypto';
import {
    readFileSync,
    readdirSync,
    readlinkSync,
    realpathSync,
    statSync,
} from 'fs';
import {
    link,
    open,
    readFile,
    readdir,
    rename,
    rm,
    stat,
} from 'fs/promises';
import * as path from 'path';
import { createDirectoryDurably, syncDirectoryDurably } from './durableFile';

interface ProjectionTransaction {
    schemaVersion: 1;
    operation: 'replace' | 'delete';
    relativePath: string;
    expectedHash?: string;
    targetHash: string;
}

export interface LocalProjectionResult {
    applied: boolean;
    actual?: Uint8Array;
    /**
     * The returned bytes were read from the inode installed by this replace,
     * and that inode still owned the visible path when the read completed.
     */
    actualWasReadFromInstalledTarget?: boolean;
    recoveryCandidate?: LocalProjectionRecoveryCandidate;
}

export interface LocalProjectionRecoveryCandidate {
    transactionId: string;
    path: string;
    operation: 'replace' | 'delete';
    artifactDirectory: string;
    localFile: string;
    localHash: string;
    expectedHash: string;
    targetHash: string;
    retainTransaction?: boolean;
}

export type LocalProjectionRecoveryInspection =
    | {status: 'missing'}
    | {status: 'busy'}
    | {status: 'ready'; candidate: LocalProjectionRecoveryCandidate};

export interface LocalProjectionCasOptions {
    /** Deterministic race injection for tests. */
    onPhase?: (
        phase: 'prepared' | 'displaced' | 'installed',
        relativePath: string,
    ) => Promise<void>;
    /** Override Linux /proc probing in deterministic portability tests. */
    hasWritableHandle?: (filePath: string) => Promise<boolean>;
    /** Crash-boundary injection after cleanup ownership is atomically renamed. */
    afterCleanupRename?: (transactionId: string) => Promise<void>;
}

const COMPLETED_TRANSACTION_PREFIX = '.completed-';

/**
 * Crash-recoverable compare-and-swap for plugin-originated disk projections.
 *
 * The target and transaction directory share one filesystem. Moving the old
 * path aside is atomic, and linking the staged target back is an atomic
 * no-replace operation. An uncoordinated writer therefore wins the pathname
 * instead of being overwritten between a preflight read and a writeFile call.
 */
export class LocalProjectionCas {
    private readonly transactionRoot: string;
    private activeTransitions = 0;
    private transitionEpoch = 0;

    constructor(
        private readonly replicaRoot: string,
        private readonly options: LocalProjectionCasOptions = {},
    ) {
        this.transactionRoot = path.join(replicaRoot, '.overleaf', 'projections');
    }

    /** Changes whenever a live projection can invalidate a filesystem scan. */
    get observationEpoch(): number {
        return this.transitionEpoch;
    }

    get hasActiveTransition(): boolean {
        return this.activeTransitions > 0;
    }

    async recover(): Promise<LocalProjectionRecoveryCandidate[]> {
        let entries: string[];
        try {
            entries = await readdir(this.transactionRoot);
        } catch (error) {
            if (errorCode(error) === 'ENOENT') {
                return [];
            }
            throw error;
        }
        const candidates: LocalProjectionRecoveryCandidate[] = [];
        for (const entry of entries) {
            const transactionDirectory = path.join(this.transactionRoot, entry);
            if (entry.startsWith(COMPLETED_TRANSACTION_PREFIX)) {
                await rm(transactionDirectory, {recursive: true, force: true});
                continue;
            }
            let metadata: ProjectionTransaction;
            try {
                metadata = this.parseManifest(await readFile(
                    path.join(transactionDirectory, 'manifest.json'),
                    'utf8',
                ));
            } catch (error) {
                // A manifest is made durable before the target can move. An
                // incomplete manifest directory therefore owns no user bytes.
                if (!await exists(path.join(transactionDirectory, 'displaced'))) {
                    await this.cleanupTransaction(transactionDirectory);
                    continue;
                }
                throw new Error(
                    `Cannot recover local projection transaction ${entry}: ${String(error)}`,
                );
            }
            const candidate = await this.recoverTransaction(
                transactionDirectory,
                metadata,
                true,
            );
            if (candidate) {
                candidates.push(candidate);
            }
        }
        await syncDirectoryDurably(this.transactionRoot);
        return candidates;
    }

    /** Release a recovered transaction only after its conflict fence is durable. */
    async acknowledgeRecovery(transactionId: string): Promise<void> {
        const normalized = normalizeTransactionId(transactionId);
        await this.cleanupTransaction(path.join(this.transactionRoot, normalized));
    }

    /**
     * Refresh a retained recovery candidate after an inode writer has closed.
     * The transaction remains owned by the caller until acknowledgeRecovery().
     */
    async inspectRecovery(
        transactionId: string,
    ): Promise<LocalProjectionRecoveryInspection> {
        const normalized = normalizeTransactionId(transactionId);
        const transactionDirectory = path.join(this.transactionRoot, normalized);
        const manifestPath = path.join(transactionDirectory, 'manifest.json');
        const displacedPath = path.join(transactionDirectory, 'displaced');
        let metadata: ProjectionTransaction;
        try {
            metadata = this.parseManifest(await readFile(manifestPath, 'utf8'));
        } catch (error) {
            if (errorCode(error) === 'ENOENT') {
                return {status: 'missing'};
            }
            throw error;
        }
        if (!await exists(displacedPath)) {
            return {status: 'missing'};
        }
        if (await this.hasWritableHandle(displacedPath)) {
            return {status: 'busy'};
        }
        const refreshed = await readFile(displacedPath);
        // A descriptor may become writable between the first probe and read.
        // Do not publish a candidate until one stable closed-handle sample exists.
        if (await this.hasWritableHandle(displacedPath)) {
            return {status: 'busy'};
        }
        const candidate = await this.archiveUnexpectedDisplaced(
            metadata.relativePath,
            refreshed,
            normalized,
            metadata,
            true,
        );
        candidate.retainTransaction = false;
        return {status: 'ready', candidate};
    }

    async replace(
        relativePath: string,
        expectedHash: string | undefined,
        content: Uint8Array,
    ): Promise<LocalProjectionResult> {
        const normalized = normalizeSafeRelativePath(relativePath);
        const targetPath = path.join(this.replicaRoot, ...normalized.split('/'));
        const transactionDirectory = path.join(this.transactionRoot, randomUUID());
        const stagedPath = path.join(transactionDirectory, 'target');
        const displacedPath = path.join(transactionDirectory, 'displaced');
        const metadata: ProjectionTransaction = {
            schemaVersion: 1,
            operation: 'replace',
            relativePath: normalized,
            expectedHash,
            targetHash: hash(content),
        };

        await createDirectoryDurably(this.transactionRoot, 0o700);
        await createDirectoryDurably(transactionDirectory, 0o700);
        await writeDurable(stagedPath, content);
        await writeDurable(
            path.join(transactionDirectory, 'manifest.json'),
            Buffer.from(JSON.stringify(metadata)),
        );
        await syncDirectoryDurably(transactionDirectory);
        await syncDirectoryDurably(this.transactionRoot);
        await createDirectoryDurably(path.dirname(targetPath));
        await this.options.onPhase?.('prepared', normalized);

        // An open writable handle is answered before the displacement window
        // opens, never inside it. The probe walks every process descriptor and
        // takes hundreds of milliseconds; running it between the rename and the
        // link would leave the pathname absent from disk for that whole scan,
        // which is exactly what an editor observes as a vanished file. Asked
        // here, the same question costs the same answer while the file is still
        // in place. A handle opened between this probe and the rename is not
        // seen, so the recovery paths below still probe the displaced inode
        // before letting a transaction unlink it.
        if (expectedHash !== undefined && await this.hasWritableHandle(targetPath)) {
            const actual = await readOptional(targetPath);
            await this.cleanupTransaction(transactionDirectory);
            return {applied: false, actual};
        }

        this.beginTransition();
        try {
            if (expectedHash === undefined) {
                try {
                    await link(stagedPath, targetPath);
                } catch (error) {
                    if (errorCode(error) !== 'EEXIST') {
                        throw error;
                    }
                    const actual = await readOptional(targetPath);
                    await this.cleanupTransaction(transactionDirectory);
                    return {applied: false, actual};
                }
            } else {
                try {
                    await rename(targetPath, displacedPath);
                } catch (error) {
                    if (errorCode(error) !== 'ENOENT') {
                        throw error;
                    }
                    await this.cleanupTransaction(transactionDirectory);
                    return {applied: false};
                }
                await syncDirectoryDurably(path.dirname(targetPath));
                await syncDirectoryDurably(transactionDirectory);
                await this.options.onPhase?.('displaced', normalized);

                const displaced = await readFile(displacedPath);
                if (hash(displaced) !== expectedHash) {
                    const restored = await this.restoreDisplaced(
                        targetPath,
                        displacedPath,
                        displaced,
                    );
                    let recoveryCandidate: LocalProjectionRecoveryCandidate | undefined;
                    if (!restored.restored) {
                        recoveryCandidate = await this.archiveRuntimeCandidate(
                            transactionDirectory,
                            metadata,
                            displaced,
                            await this.hasWritableHandle(displacedPath),
                        );
                    } else {
                        await this.cleanupTransaction(transactionDirectory);
                    }
                    return {applied: false, actual: restored.actual, recoveryCandidate};
                }
                const confirmed = await readFile(displacedPath);
                if (hash(confirmed) !== expectedHash) {
                    const restored = await this.restoreDisplaced(
                        targetPath,
                        displacedPath,
                        confirmed,
                    );
                    let recoveryCandidate: LocalProjectionRecoveryCandidate | undefined;
                    if (!restored.restored) {
                        recoveryCandidate = await this.archiveRuntimeCandidate(
                            transactionDirectory,
                            metadata,
                            confirmed,
                            await this.hasWritableHandle(displacedPath),
                        );
                    } else {
                        await this.cleanupTransaction(transactionDirectory);
                    }
                    return {applied: false, actual: restored.actual, recoveryCandidate};
                }
                try {
                    await link(stagedPath, targetPath);
                } catch (error) {
                    if (errorCode(error) !== 'EEXIST') {
                        throw error;
                    }
                    // Someone put a new inode at this pathname inside the
                    // window. Their bytes stay; ours are abandoned. The
                    // displaced inode is only unlinked once it is proven that
                    // no writer is still holding it open.
                    const actual = await readOptional(targetPath);
                    if (await this.hasWritableHandle(displacedPath)) {
                        const recoveryCandidate = await this.archiveRuntimeCandidate(
                            transactionDirectory,
                            metadata,
                            displaced,
                            true,
                        );
                        return {applied: false, actual, recoveryCandidate};
                    }
                    await this.cleanupTransaction(transactionDirectory);
                    return {applied: false, actual};
                }
            }

            await syncDirectoryDurably(path.dirname(targetPath));
            await writeDurable(path.join(transactionDirectory, 'COMMITTED'), Buffer.from('1\n'));
            await syncDirectoryDurably(transactionDirectory);
            await this.options.onPhase?.('installed', normalized);
            const observed = await readOptionalWithInstalledIdentity(
                targetPath,
                stagedPath,
            );
            const actual = observed.actual;
            if (!actual || hash(actual) !== metadata.targetHash) {
                await this.cleanupTransaction(transactionDirectory);
                return {
                    applied: false,
                    actual,
                    actualWasReadFromInstalledTarget: observed.matchesInstalledTarget,
                };
            }
            await this.cleanupTransaction(transactionDirectory);
            return {applied: true, actual};
        } catch (error) {
            // Ordinary I/O failures recover synchronously. A process crash is
            // handled by recover() before filesystem semantics are observed.
            await this.recoverTransaction(transactionDirectory, metadata).catch(() => {});
            throw error;
        } finally {
            this.endTransition();
        }
    }

    async remove(
        relativePath: string,
        expectedHash: string,
    ): Promise<LocalProjectionResult> {
        const normalized = normalizeSafeRelativePath(relativePath);
        const targetPath = path.join(this.replicaRoot, ...normalized.split('/'));
        const transactionDirectory = path.join(this.transactionRoot, randomUUID());
        const displacedPath = path.join(transactionDirectory, 'displaced');
        const metadata: ProjectionTransaction = {
            schemaVersion: 1,
            operation: 'delete',
            relativePath: normalized,
            expectedHash,
            targetHash: hash(Buffer.alloc(0)),
        };

        await createDirectoryDurably(this.transactionRoot, 0o700);
        await createDirectoryDurably(transactionDirectory, 0o700);
        await writeDurable(
            path.join(transactionDirectory, 'manifest.json'),
            Buffer.from(JSON.stringify(metadata)),
        );
        await syncDirectoryDurably(transactionDirectory);
        await syncDirectoryDurably(this.transactionRoot);
        await this.options.onPhase?.('prepared', normalized);

        // Asked before the displacement window for the same reason as replace():
        // this probe is far too slow to hold a pathname out of the filesystem.
        if (await this.hasWritableHandle(targetPath)) {
            const actual = await readOptional(targetPath);
            await this.cleanupTransaction(transactionDirectory);
            return {applied: false, actual};
        }

        this.beginTransition();
        try {
            try {
                await rename(targetPath, displacedPath);
            } catch (error) {
                if (errorCode(error) !== 'ENOENT') {
                    throw error;
                }
                await this.cleanupTransaction(transactionDirectory);
                return {applied: false};
            }
            await syncDirectoryDurably(path.dirname(targetPath));
            await syncDirectoryDurably(transactionDirectory);
            await this.options.onPhase?.('displaced', normalized);
            const displaced = await readFile(displacedPath);
            if (hash(displaced) !== expectedHash) {
                const restored = await this.restoreDisplaced(
                    targetPath,
                    displacedPath,
                    displaced,
                );
                let recoveryCandidate: LocalProjectionRecoveryCandidate | undefined;
                if (!restored.restored) {
                    recoveryCandidate = await this.archiveRuntimeCandidate(
                        transactionDirectory,
                        metadata,
                        displaced,
                        await this.hasWritableHandle(displacedPath),
                    );
                } else {
                    await this.cleanupTransaction(transactionDirectory);
                }
                return {applied: false, actual: restored.actual, recoveryCandidate};
            }
            const confirmed = await readFile(displacedPath);
            if (hash(confirmed) !== expectedHash) {
                const restored = await this.restoreDisplaced(
                    targetPath,
                    displacedPath,
                    confirmed,
                );
                let recoveryCandidate: LocalProjectionRecoveryCandidate | undefined;
                if (!restored.restored) {
                    recoveryCandidate = await this.archiveRuntimeCandidate(
                        transactionDirectory,
                        metadata,
                        confirmed,
                        await this.hasWritableHandle(displacedPath),
                    );
                } else {
                    await this.cleanupTransaction(transactionDirectory);
                }
                return {applied: false, actual: restored.actual, recoveryCandidate};
            }

            const concurrent = await readOptional(targetPath);
            if (concurrent) {
                // A new inode took the pathname inside the window, so this
                // delete no longer describes what is on disk. As in replace(),
                // the displaced inode outlives the transaction while a writer
                // still holds it open.
                if (await this.hasWritableHandle(displacedPath)) {
                    const recoveryCandidate = await this.archiveRuntimeCandidate(
                        transactionDirectory,
                        metadata,
                        displaced,
                        true,
                    );
                    return {applied: false, actual: concurrent, recoveryCandidate};
                }
                await this.cleanupTransaction(transactionDirectory);
                return {applied: false, actual: concurrent};
            }
            await writeDurable(path.join(transactionDirectory, 'COMMITTED'), Buffer.from('1\n'));
            await syncDirectoryDurably(transactionDirectory);
            await this.options.onPhase?.('installed', normalized);
            const actual = await readOptional(targetPath);
            await this.cleanupTransaction(transactionDirectory);
            return actual
                ? {applied: false, actual}
                : {applied: true};
        } catch (error) {
            await this.recoverTransaction(transactionDirectory, metadata).catch(() => {});
            throw error;
        } finally {
            this.endTransition();
        }
    }

    private beginTransition(): void {
        this.activeTransitions++;
        this.transitionEpoch++;
    }

    private endTransition(): void {
        this.activeTransitions = Math.max(0, this.activeTransitions - 1);
        this.transitionEpoch++;
    }

    private async recoverTransaction(
        transactionDirectory: string,
        metadata: ProjectionTransaction,
        deferUnexpectedCleanup = false,
    ): Promise<LocalProjectionRecoveryCandidate | undefined> {
        const targetPath = path.join(
            this.replicaRoot,
            ...normalizeSafeRelativePath(metadata.relativePath).split('/'),
        );
        const displacedPath = path.join(transactionDirectory, 'displaced');
        const committed = await exists(path.join(transactionDirectory, 'COMMITTED'));
        const target = await readOptional(targetPath);
        const displaced = await readOptional(displacedPath);

        let restored = Boolean(
            !committed
            && target
            && displaced
            && hash(target) === hash(displaced),
        );
        if (!target && displaced && !committed) {
            await createDirectoryDurably(path.dirname(targetPath));
            try {
                await link(displacedPath, targetPath);
                await syncDirectoryDurably(path.dirname(targetPath));
                restored = true;
            } catch (error) {
                if (errorCode(error) !== 'EEXIST') {
                    throw error;
                }
            }
        }

        let candidate: LocalProjectionRecoveryCandidate | undefined;
        if (
            !restored
            && displaced
            && metadata.expectedHash !== undefined
            && hash(displaced) !== metadata.expectedHash
        ) {
            candidate = await this.archiveUnexpectedDisplaced(
                metadata.relativePath,
                displaced,
                path.basename(transactionDirectory),
                metadata,
                deferUnexpectedCleanup,
            );
            candidate.retainTransaction = await this.hasWritableHandle(displacedPath);
        }
        if (!candidate || !deferUnexpectedCleanup) {
            await this.cleanupTransaction(transactionDirectory);
        }
        return candidate;
    }

    private async archiveRuntimeCandidate(
        transactionDirectory: string,
        metadata: ProjectionTransaction,
        content: Uint8Array,
        retainTransaction: boolean,
    ): Promise<LocalProjectionRecoveryCandidate> {
        const candidate = await this.archiveUnexpectedDisplaced(
            metadata.relativePath,
            content,
            path.basename(transactionDirectory),
            metadata,
            true,
        );
        candidate.retainTransaction = retainTransaction;
        return candidate;
    }

    private async restoreDisplaced(
        targetPath: string,
        displacedPath: string,
        displaced: Uint8Array,
    ): Promise<{actual: Uint8Array; restored: boolean}> {
        try {
            await link(displacedPath, targetPath);
            await syncDirectoryDurably(path.dirname(targetPath));
            return {actual: displaced, restored: true};
        } catch (error) {
            if (errorCode(error) !== 'EEXIST') {
                throw error;
            }
            return {actual: await readFile(targetPath), restored: false};
        }
    }

    private async archiveUnexpectedDisplaced(
        relativePath: string,
        content: Uint8Array,
        transactionId: string = randomUUID(),
        metadata?: ProjectionTransaction,
        unresolved = false,
    ): Promise<LocalProjectionRecoveryCandidate> {
        const normalizedTransactionId = normalizeTransactionId(transactionId);
        const normalizedPath = normalizeSafeRelativePath(relativePath);
        const artifactDirectory = path.posix.join(
            unresolved
                ? '.overleaf/conflicts/unresolved'
                : '.overleaf/conflicts/resolved',
            unresolved
                ? `projection-recovery-${normalizedTransactionId}`
                : `projection-recovery-${new Date().toISOString().replace(/[:.]/g, '-')}-${normalizedTransactionId}`,
        );
        let localFile = path.posix.join(artifactDirectory, 'local-recovery', normalizedPath);
        let archivePath = path.join(this.replicaRoot, ...localFile.split('/'));
        await createDirectoryDurably(path.dirname(archivePath), 0o700);
        const existing = await readOptional(archivePath);
        if (existing) {
            if (hash(existing) !== hash(content)) {
                localFile = path.posix.join(
                    artifactDirectory,
                    'local-recovery-revisions',
                    `${new Date().toISOString().replace(/[:.]/g, '-')}-${randomUUID()}`,
                    normalizedPath,
                );
                archivePath = path.join(this.replicaRoot, ...localFile.split('/'));
                await createDirectoryDurably(path.dirname(archivePath), 0o700);
                await writeDurable(archivePath, content);
            }
        } else {
            await writeDurable(archivePath, content);
        }
        await syncDirectoryDurably(path.dirname(archivePath));
        return {
            transactionId: normalizedTransactionId,
            path: normalizedPath,
            operation: metadata?.operation ?? 'replace',
            artifactDirectory,
            localFile,
            localHash: hash(content),
            expectedHash: metadata?.expectedHash ?? hash(content),
            targetHash: metadata?.targetHash ?? hash(content),
        };
    }

    private hasWritableHandle(filePath: string): Promise<boolean> {
        return this.options.hasWritableHandle?.(filePath)
            ?? hasLinuxWritableHandle(filePath);
    }

    private async cleanupTransaction(transactionDirectory: string): Promise<void> {
        const transactionId = path.basename(transactionDirectory);
        if (transactionId.startsWith(COMPLETED_TRANSACTION_PREFIX)) {
            await rm(transactionDirectory, {recursive: true, force: true});
            await syncDirectoryDurably(this.transactionRoot);
            return;
        }
        const completedDirectory = path.join(
            this.transactionRoot,
            `${COMPLETED_TRANSACTION_PREFIX}${transactionId}-${randomUUID()}`,
        );
        try {
            await rename(transactionDirectory, completedDirectory);
        } catch (error) {
            if (errorCode(error) === 'ENOENT') {
                return;
            }
            throw error;
        }
        // Once this rename is durable, arbitrary interruption during recursive
        // unlink leaves only a recognizable cleanup tombstone. Recovery never
        // interprets its remaining children as a live projection transaction.
        await syncDirectoryDurably(this.transactionRoot);
        await this.options.afterCleanupRename?.(transactionId);
        await rm(completedDirectory, {recursive: true, force: true});
        await syncDirectoryDurably(this.transactionRoot);
    }

    private parseManifest(encoded: string): ProjectionTransaction {
        const parsed = JSON.parse(encoded) as Partial<ProjectionTransaction>;
        if (
            parsed.schemaVersion !== 1
            || (parsed.operation !== 'replace' && parsed.operation !== 'delete')
            || typeof parsed.relativePath !== 'string'
            || typeof parsed.targetHash !== 'string'
            || !/^[a-f0-9]{64}$/.test(parsed.targetHash)
            || (
                parsed.expectedHash !== undefined
                && !/^[a-f0-9]{64}$/.test(parsed.expectedHash)
            )
        ) {
            throw new Error('Invalid local projection transaction manifest');
        }
        return {
            schemaVersion: 1,
            operation: parsed.operation,
            relativePath: normalizeSafeRelativePath(parsed.relativePath),
            expectedHash: parsed.expectedHash,
            targetHash: parsed.targetHash,
        };
    }
}

function normalizeSafeRelativePath(relativePath: string): string {
    const normalized = relativePath.replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
    if (
        normalized === ''
        || normalized.split('/').some(part => part === '' || part === '.' || part === '..')
    ) {
        throw new Error(`Unsafe local projection path: ${relativePath}`);
    }
    return normalized;
}

function normalizeTransactionId(transactionId: string): string {
    if (
        transactionId === ''
        || transactionId === '.'
        || transactionId === '..'
        || !/^[a-zA-Z0-9._-]+$/.test(transactionId)
    ) {
        throw new Error(`Unsafe local projection transaction id: ${transactionId}`);
    }
    return transactionId;
}

async function writeDurable(filePath: string, content: Uint8Array): Promise<void> {
    const handle = await open(filePath, 'wx', 0o600);
    try {
        await handle.writeFile(content);
        await handle.sync();
    } finally {
        await handle.close();
    }
}

async function readOptional(filePath: string): Promise<Uint8Array | undefined> {
    try {
        return await readFile(filePath);
    } catch (error) {
        if (errorCode(error) === 'ENOENT') {
            return undefined;
        }
        throw error;
    }
}

async function readOptionalWithInstalledIdentity(
    filePath: string,
    installedPath: string,
): Promise<{actual?: Uint8Array; matchesInstalledTarget: boolean}> {
    let handle;
    try {
        handle = await open(filePath, 'r');
    } catch (error) {
        if (errorCode(error) === 'ENOENT') {
            return {matchesInstalledTarget: false};
        }
        throw error;
    }
    try {
        const openedBefore = await handle.stat({bigint: true});
        const installed = await stat(installedPath, {bigint: true});
        const actual = await handle.readFile();
        const openedAfter = await handle.stat({bigint: true});
        let visible;
        try {
            visible = await stat(filePath, {bigint: true});
        } catch (error) {
            if (errorCode(error) === 'ENOENT') {
                return {actual, matchesInstalledTarget: false};
            }
            throw error;
        }
        return {
            actual,
            matchesInstalledTarget: sameFileIdentity(openedBefore, installed)
                && sameFileIdentity(openedBefore, openedAfter)
                && sameFileIdentity(openedBefore, visible),
        };
    } finally {
        await handle.close();
    }
}

function sameFileIdentity(
    left: {dev: bigint; ino: bigint},
    right: {dev: bigint; ino: bigint},
): boolean {
    return left.dev === right.dev && left.ino === right.ino;
}

async function exists(filePath: string): Promise<boolean> {
    try {
        await stat(filePath);
        return true;
    } catch (error) {
        if (errorCode(error) === 'ENOENT') {
            return false;
        }
        throw error;
    }
}

function hash(content: Uint8Array): string {
    return createHash('sha256').update(content).digest('hex');
}

function errorCode(error: unknown): string | undefined {
    return (error as NodeJS.ErrnoException)?.code;
}

/** Processes read between two yields back to the host's event loop. */
const WRITABLE_HANDLE_SCAN_BATCH = 64;

/**
 * Whether any process holds this file open for writing.
 *
 * Answering means one tiny /proc read per descriptor of every process on the
 * machine — around eight thousand of them here. Two properties make that
 * affordable enough to ask before each projection.
 *
 * The reads are synchronous. Asked asynchronously they queue behind libuv's
 * four worker threads and the scan takes half a second of wall time; issued
 * directly they cost about a tenth of that. Yielding every
 * WRITABLE_HANDLE_SCAN_BATCH processes keeps any single stretch of blocked
 * event loop in the single-digit milliseconds, so the host stays responsive
 * across a scan it never notices.
 *
 * The per-descriptor test is readlink(), which reports the name a descriptor
 * was opened by without resolving anything; stat() on the same descriptor
 * costs a full path walk. Only a matching name is confirmed by device and
 * inode, so a recycled pathname is never mistaken for this file. A descriptor
 * opened through a second hard link reports that other name and is not
 * matched; the replica tree has no such aliases.
 */
async function hasLinuxWritableHandle(filePath: string): Promise<boolean> {
    if (process.platform !== 'linux') {
        return false;
    }
    let target;
    let resolved;
    try {
        target = statSync(filePath);
        resolved = realpathSync(filePath);
    } catch {
        // A pathname that resolves to nothing is a pathname nobody holds open.
        return false;
    }
    // The kernel appends this once the name a descriptor was opened by is gone.
    const unlinked = `${resolved} (deleted)`;
    let processes;
    try {
        processes = readdirSync('/proc').filter(name => /^\d+$/.test(name));
    } catch {
        return false;
    }
    for (let index = 0; index < processes.length; index += WRITABLE_HANDLE_SCAN_BATCH) {
        const batch = processes.slice(index, index + WRITABLE_HANDLE_SCAN_BATCH);
        for (const processName of batch) {
            const fdDirectory = path.join('/proc', processName, 'fd');
            let descriptors;
            try {
                descriptors = readdirSync(fdDirectory);
            } catch {
                continue;
            }
            for (const descriptor of descriptors) {
                const descriptorPath = path.join(fdDirectory, descriptor);
                try {
                    const name = readlinkSync(descriptorPath);
                    if (name !== resolved && name !== unlinked) {
                        continue;
                    }
                    const opened = statSync(descriptorPath);
                    if (opened.dev !== target.dev || opened.ino !== target.ino) {
                        continue;
                    }
                    const info = readFileSync(
                        path.join('/proc', processName, 'fdinfo', descriptor),
                        'utf8',
                    );
                    const encodedFlags = /^flags:\s*([0-7]+)/m.exec(info)?.[1];
                    if (encodedFlags && (Number.parseInt(encodedFlags, 8) & 0b11) !== 0) {
                        return true;
                    }
                } catch {
                    // The process or descriptor may disappear while /proc is read.
                }
            }
        }
        await new Promise(resolve => setImmediate(resolve));
    }
    return false;
}
