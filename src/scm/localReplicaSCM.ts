import { randomUUID } from 'crypto';
import { lstat } from 'fs/promises';
import * as path from 'path';
import * as vscode from 'vscode';
import { BaseSCM, CommitItem, SettingItem } from '.';
import {
    ConcurrentRemoteResourceError,
    VirtualFileSystem,
} from '../core/remoteFileSystemProvider';
import { decideReconciliation, mergeConcurrentText, ReconcileDecision } from '../sync/reconcile';
import {
    attachTreeHashes,
    findReplicaMove,
    isPathConflicted,
    refreshConvergedBaseline,
    ReplicaSnapshot,
    validatePendingRecovery,
} from '../sync/replicaPlanner';
import { ReplicaLock } from '../sync/replicaLock';
import {
    BaselineEntry,
    ConflictRecord,
    PendingReplicaAction,
    ReplicaActionKind,
    ReplicaState,
    ReplicaStateStore,
} from '../sync/replicaStateStore';
import {
    contentHash,
    DEFAULT_IGNORE_PATTERNS,
    isEditableTextCandidate,
    normalizeRelativePath,
    shouldIgnore,
} from '../sync/syncPolicy';

const IGNORE_SETTING_KEY = 'ignore-patterns';
const TEXT_DEBOUNCE_MS = 350;
const BINARY_STABILITY_MS = 500;
const MAX_RECONCILE_ACTIONS = 100;

type ReconcileStep = 'acted' | 'waiting' | 'none';

class StaleLocalSnapshotError extends Error {
    constructor(message: string, readonly actualContent?: Uint8Array) {
        super(message);
    }
}

class BaselineSelectionChangedError extends Error {}

interface ScanPair {
    local: Map<string, ReplicaSnapshot>;
    remote: Map<string, ReplicaSnapshot>;
}

/**
 * A durable, fail-closed local replica of one Overleaf project.
 *
 * Text changes are transformed through the VFS document session. Binary and
 * tree changes use a persisted common baseline and never use timestamps as a
 * winner signal.
 */
export class LocalReplicaSCMProvider extends BaseSCM {
    public static readonly label = vscode.l10n.t('Local Replica');

    public readonly iconPath: vscode.ThemeIcon = new vscode.ThemeIcon('shield');

    private readonly stateStore: ReplicaStateStore;
    private readonly output: vscode.OutputChannel;
    private readonly binaryStability = new Map<string, { hash: string; since: number }>();
    private readonly pendingReasons = new Set<string>();
    private state?: ReplicaState;
    private lock?: ReplicaLock;
    private vfsWatcher?: vscode.FileSystemWatcher;
    private localWatcher?: vscode.FileSystemWatcher;
    private reconcileTimer?: NodeJS.Timeout;
    private reconcileQueue: Promise<void> = Promise.resolve();
    private pausedError?: Error;
    private disposed = false;
    private lastScan?: ScanPair;
    private lastReport = 'Safe sync has not scanned this replica yet.';

    constructor(
        protected readonly vfs: VirtualFileSystem,
        public readonly baseUri: vscode.Uri,
        private readonly context: vscode.ExtensionContext,
    ) {
        super(vfs, baseUri);
        this.stateStore = new ReplicaStateStore(
            context.globalStorageUri.fsPath,
            vfs.serverName,
            vfs.projectId,
            baseUri.fsPath,
        );
        this.output = vscode.window.createOutputChannel('Hershel Overleaf Safe Sync');
    }

    private static sanitizeProjectFolderName(projectName: string): string {
        let sanitized = projectName;
        if (process.platform === 'win32') {
            sanitized = projectName
                .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
                .replace(/[. ]+$/g, '');
            if (/^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$/i.test(sanitized)) {
                sanitized = `${sanitized}_`;
            }
        } else {
            sanitized = projectName.replace(/[\/\x00]/g, '_');
        }
        if (sanitized === '' || sanitized === '.' || sanitized === '..') {
            sanitized = 'untitled-project';
        }
        return sanitized;
    }

    public static async validateBaseUri(uri: string, projectName?: string): Promise<vscode.Uri> {
        try {
            let baseUri = vscode.Uri.file(uri);
            const folderName = projectName === undefined
                ? undefined
                : LocalReplicaSCMProvider.sanitizeProjectFolderName(projectName);
            try {
                const stat = await vscode.workspace.fs.stat(baseUri);
                if (stat.type !== vscode.FileType.Directory) {
                    throw new Error('Not a folder');
                }
                if (folderName !== undefined && !baseUri.path.endsWith(`/${folderName}`)) {
                    baseUri = vscode.Uri.joinPath(baseUri, folderName);
                }
            } catch {
                // The selected path itself may be the not-yet-created replica.
            }
            await vscode.workspace.fs.createDirectory(baseUri);
            await vscode.workspace.fs.stat(baseUri);
            return baseUri;
        } catch (error) {
            vscode.window.showErrorMessage(vscode.l10n.t(
                'Invalid Path. Please make sure the absolute path to a folder with read/write permissions is used.',
            ));
            return Promise.reject(error);
        }
    }

    public static async pathToUri(filePath: string): Promise<vscode.Uri | undefined> {
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0].uri;
        if (workspaceRoot === undefined || workspaceRoot.scheme !== 'file') {
            return undefined;
        }
        try {
            await vscode.workspace.fs.stat(vscode.Uri.joinPath(workspaceRoot, '.overleaf/settings.json'));
            return vscode.Uri.joinPath(workspaceRoot, filePath);
        } catch {
            return undefined;
        }
    }

    public static async uriToPath(uri: vscode.Uri): Promise<string | undefined> {
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0].uri;
        if (workspaceRoot === undefined || workspaceRoot.scheme !== 'file') {
            return undefined;
        }
        try {
            await vscode.workspace.fs.stat(vscode.Uri.joinPath(workspaceRoot, '.overleaf/settings.json'));
            return uri.path.slice(workspaceRoot.path.length);
        } catch {
            return undefined;
        }
    }

    public static async readSettings(): Promise<any | undefined> {
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0].uri;
        if (vscode.workspace.workspaceFolders?.length !== 1 || workspaceRoot?.scheme !== 'file') {
            return undefined;
        }
        try {
            const content = await vscode.workspace.fs.readFile(
                vscode.Uri.joinPath(workspaceRoot, '.overleaf/settings.json'),
            );
            return JSON.parse(new TextDecoder().decode(content));
        } catch {
            return undefined;
        }
    }

    private get ignorePatterns(): string[] {
        const configured = this.getSetting<string[]>(IGNORE_SETTING_KEY) ?? [];
        return [...new Set([...DEFAULT_IGNORE_PATTERNS, ...configured])];
    }

    private isIgnored(filePath: string): boolean {
        return shouldIgnore(filePath, this.ignorePatterns);
    }

    private async initWatch(): Promise<vscode.Disposable[]> {
        this.disposed = false;
        if (this.baseUri.scheme !== 'file') {
            throw new Error('Safe local replicas require a file:// directory');
        }
        this.lock = await ReplicaLock.acquire(
            this.context.globalStorageUri.fsPath,
            this.baseUri.fsPath,
            { onLost: error => this.failProject(error) },
        );
        await this.vfs.init();
        try {
            this.state = await this.stateStore.load();
        } catch (error) {
            this.failProject(this.asError(error));
        }

        this.vfsWatcher = vscode.workspace.createFileSystemWatcher(
            new vscode.RelativePattern(this.vfs.origin, '**/*'),
        );
        this.localWatcher = vscode.workspace.createFileSystemWatcher(
            new vscode.RelativePattern(this.baseUri.fsPath, '**/*'),
        );
        const listeners: vscode.Disposable[] = [
            this.vfsWatcher.onDidChange(() => this.scheduleReconcile('remote change', 100)),
            this.vfsWatcher.onDidCreate(() => this.scheduleReconcile('remote create', 100)),
            this.vfsWatcher.onDidDelete(() => this.scheduleReconcile('remote delete', 100)),
            this.localWatcher.onDidChange(uri => this.onLocalFileEvent(uri, 'change')),
            this.localWatcher.onDidCreate(uri => this.onLocalFileEvent(uri, 'create')),
            this.localWatcher.onDidDelete(uri => this.onLocalFileEvent(uri, 'delete')),
            vscode.workspace.onDidChangeTextDocument(event => {
                const relative = this.relativeLocalPath(event.document.uri);
                if (relative !== undefined && !this.isIgnored(relative)) {
                    this.scheduleReconcile(`text edit: ${relative}`, TEXT_DEBOUNCE_MS);
                }
            }),
            vscode.workspace.onDidSaveTextDocument(document => {
                const relative = this.relativeLocalPath(document.uri);
                if (relative !== undefined && !this.isIgnored(relative)) {
                    this.scheduleReconcile(`text save: ${relative}`, 100);
                }
            }),
        ];

        if (!this.pausedError) {
            await this.reconcileUntilStable('startup');
        }

        return [
            this.vfsWatcher,
            this.localWatcher,
            ...listeners,
            new vscode.Disposable(() => {
                this.disposed = true;
                if (this.reconcileTimer) {
                    clearTimeout(this.reconcileTimer);
                    this.reconcileTimer = undefined;
                }
                try {
                    this.lock?.releaseSync();
                } catch (error) {
                    console.error('Failed to release safe-sync replica lock', error);
                }
            }),
        ];
    }

    public dispose(): void {
        this.disposed = true;
        if (this.reconcileTimer) {
            clearTimeout(this.reconcileTimer);
            this.reconcileTimer = undefined;
        }
        try {
            this.lock?.releaseSync();
        } catch (error) {
            console.error('Failed to release safe-sync replica lock', error);
        }
        this.output.dispose();
    }

    private onLocalFileEvent(uri: vscode.Uri, kind: string): void {
        const relative = this.relativeLocalPath(uri);
        if (relative === undefined || this.isIgnored(relative)) {
            return;
        }
        this.scheduleReconcile(`local ${kind}: ${relative}`, TEXT_DEBOUNCE_MS);
    }

    private scheduleReconcile(reason: string, delayMs = TEXT_DEBOUNCE_MS): void {
        if (this.disposed || this.pausedError) {
            return;
        }
        this.pendingReasons.add(reason);
        if (this.reconcileTimer) {
            clearTimeout(this.reconcileTimer);
        }
        this.reconcileTimer = setTimeout(() => {
            this.reconcileTimer = undefined;
            const reasons = [...this.pendingReasons].join(', ');
            this.pendingReasons.clear();
            const next = this.reconcileQueue.then(() => this.reconcileUntilStable(reasons));
            this.reconcileQueue = next.catch(error => this.handleReconcileError(error));
        }, delayMs);
    }

    private async reconcileUntilStable(reason: string): Promise<void> {
        if (this.disposed || this.pausedError || !this.state) {
            return;
        }
        for (let actionCount = 0; actionCount < MAX_RECONCILE_ACTIONS; actionCount++) {
            const scan = await this.scanBoth();
            this.lastScan = scan;
            this.renderReport(scan, reason);

            if (this.state.pendingAction) {
                validatePendingRecovery(this.state.pendingAction, scan.local, scan.remote);
                if (this.pendingActionCompleted(this.state.pendingAction, scan)) {
                    refreshConvergedBaseline(this.state, scan.local, scan.remote);
                    this.state.pendingAction = undefined;
                    await this.stateStore.save(this.state);
                } else if (await this.resumePendingAction(this.state.pendingAction, scan)) {
                    continue;
                }
            }

            const capability = this.vfs.safeSyncCapability;
            if (!capability.writable) {
                this.status = { status: 'need-attention', message: `Read-only: ${capability.reason}` };
                return;
            }
            if (!this.state.initialized) {
                this.status = {
                    status: 'need-attention',
                    message: 'Safe-sync baseline selection required; no endpoint was modified',
                };
                return;
            }

            const step = await this.reconcileOne(scan);
            if (step === 'waiting') {
                return;
            }
            if (step === 'none') {
                if (this.state.pendingAction) {
                    throw new Error(
                        `Pending ${this.state.pendingAction.kind} could not be proven complete`,
                    );
                }
                refreshConvergedBaseline(this.state, scan.local, scan.remote);
                await this.stateStore.save(this.state);
                const conflictCount = Object.keys(this.state.conflicts).length;
                this.status = conflictCount > 0
                    ? { status: 'need-attention', message: `${conflictCount} path conflict(s) paused` }
                    : { status: 'idle', message: 'Safe sync converged' };
                return;
            }
        }
        throw new Error(`Safe sync exceeded ${MAX_RECONCILE_ACTIONS} actions without converging`);
    }

    private async reconcileOne(scan: ScanPair): Promise<ReconcileStep> {
        const state = this.requireState();
        const localWriterOverlap = await this.reconcileLocalWriterOverlap(scan);
        if (localWriterOverlap !== 'none') {
            return localWriterOverlap;
        }
        const activeBinaryConflict = await this.reconcileActiveBinaryConflict(scan);
        if (activeBinaryConflict !== 'none') {
            return activeBinaryConflict;
        }
        const move = findReplicaMove(state.entries, scan.local, scan.remote);
        if (move && !isPathConflicted(move.oldPath, state)) {
            if (move.action === 'conflict') {
                await this.createConflict(move.oldPath, move.reason, scan);
                return 'acted';
            }
            if (move.action === 'adopt-move') {
                const previous = state.entries[move.oldPath];
                delete state.entries[move.oldPath];
                if (previous) {
                    state.entries[move.newPath] = previous;
                }
                await this.stateStore.save(state);
                return 'acted';
            }
            if (move.action === 'move-local') {
                await this.ensureLocalParent(move.newPath);
                await this.journalAction(
                    'move-local',
                    move.oldPath,
                    scan.local.get(move.oldPath),
                    scan.remote.get(move.newPath),
                    scan.local.get(move.oldPath)?.hash,
                    async () => vscode.workspace.fs.rename(
                        this.localUri(move.oldPath),
                        this.localUri(move.newPath),
                        { overwrite: false },
                    ),
                    move.newPath,
                    move.entityId,
                );
                return 'acted';
            }
            const parent = this.parentPath(move.newPath);
            if (parent && !scan.remote.has(parent)) {
                const localParent = scan.local.get(parent);
                if (!localParent || localParent.fileType !== 'folder') {
                    await this.createConflict(
                        move.oldPath,
                        `The remote parent for local move ${move.newPath} does not exist`,
                        scan,
                    );
                    return 'acted';
                }
                await this.journalAction(
                    'push',
                    parent,
                    localParent,
                    undefined,
                    localParent.hash,
                    () => this.vfs.mkdir(this.remoteUri(parent)),
                );
                return 'acted';
            }
            if (scan.remote.get(move.oldPath)?.readonly) {
                await this.createConflict(
                    move.oldPath,
                    'The remote entity is read-only and cannot follow the local move',
                    scan,
                );
                return 'acted';
            }
            await this.journalAction(
                'move-remote',
                move.oldPath,
                scan.local.get(move.newPath),
                scan.remote.get(move.oldPath),
                scan.local.get(move.newPath)?.hash,
                () => this.vfs.rename(
                    this.remoteUri(move.oldPath),
                    this.remoteUri(move.newPath),
                    false,
                ),
                move.newPath,
                move.entityId,
            );
            return 'acted';
        }

        const paths = new Set([
            ...Object.keys(state.entries),
            ...scan.local.keys(),
            ...scan.remote.keys(),
        ]);
        const orderedPaths = [...paths].sort((left, right) => {
            const leftDepth = left.split('/').length;
            const rightDepth = right.split('/').length;
            return leftDepth - rightDepth || left.localeCompare(right);
        });

        for (const filePath of orderedPaths) {
            if (isPathConflicted(filePath, state)) {
                continue;
            }
            const baseline = state.entries[filePath];
            const local = scan.local.get(filePath);
            const remote = scan.remote.get(filePath);
            if (
                baseline
                && remote
                && remote.entityId !== baseline.entityId
                && local?.hash !== baseline.hash
            ) {
                await this.createConflict(
                    filePath,
                    'The remote entity identity changed while local content also changed',
                    scan,
                );
                return 'acted';
            }
            const decision = decideReconciliation(baseline, local, remote);
            const step = await this.applyDecision(filePath, baseline, local, remote, decision, scan);
            if (step !== 'none') {
                return step;
            }
        }
        return 'none';
    }

    private async reconcileLocalWriterOverlap(scan: ScanPair): Promise<ReconcileStep> {
        const state = this.requireState();
        for (const [filePath, local] of scan.local) {
            const variants = local.localVariants;
            if (!variants || isPathConflicted(filePath, state)) {
                continue;
            }
            if (!variants.mergedContent) {
                await this.createConflict(
                    filePath,
                    'The editor buffer and on-disk file changed without a trusted common text ancestor',
                    scan,
                );
                return 'acted';
            }
            await this.archiveLocalWriterMerge(filePath, variants);
            await this.journalAction(
                'converge-local',
                filePath,
                local,
                scan.remote.get(filePath),
                contentHash(variants.mergedContent),
                () => this.writeLocalContent(
                    filePath,
                    variants.mergedContent!,
                    variants.editorHash,
                    true,
                ),
            );
            return 'acted';
        }
        return 'none';
    }

    private async reconcileActiveBinaryConflict(scan: ScanPair): Promise<ReconcileStep> {
        const state = this.requireState();
        for (const record of Object.values(state.conflicts)) {
            const local = scan.local.get(record.path);
            const remote = scan.remote.get(record.path);
            if (
                local?.fileType !== 'file'
                || remote?.fileType !== 'file'
                || local.hash === remote.hash
            ) {
                continue;
            }
            if (!this.isBinaryStable(record.path, local.hash)) {
                this.status = {
                    status: 'need-attention',
                    message: `Waiting to archive stable binary conflict: ${record.path}`,
                };
                this.scheduleReconcile(
                    `active binary conflict stability: ${record.path}`,
                    BINARY_STABILITY_MS,
                );
                return 'waiting';
            }
            if (record.localHash !== local.hash && local.content) {
                const revisionPath = `${record.artifactDirectory}/local-revisions/`
                    + `${this.timestamp()}/${record.path}`;
                await this.writeArtifact(revisionPath, local.content);
                record.localFile = revisionPath;
                record.localHash = local.hash;
            }
            if (record.remoteHash !== remote.hash && remote.content) {
                const revisionPath = `${record.artifactDirectory}/remote-revisions/`
                    + `${this.timestamp()}/${record.path}`;
                await this.writeArtifact(revisionPath, remote.content);
                record.remoteFile = revisionPath;
                record.remoteHash = remote.hash;
            }
            await this.writeArtifact(
                `${record.artifactDirectory}/manifest.json`,
                new TextEncoder().encode(JSON.stringify(record, null, 2)),
            );
            await this.stateStore.save(state);
            await this.journalAction(
                'pull', record.path, local, remote, remote.hash,
                () => this.writeLocalSnapshot(remote, local.hash),
            );
            return 'acted';
        }
        return 'none';
    }

    private async applyDecision(
        filePath: string,
        baseline: BaselineEntry | undefined,
        local: ReplicaSnapshot | undefined,
        remote: ReplicaSnapshot | undefined,
        decision: ReconcileDecision,
        scan: ScanPair,
    ): Promise<ReconcileStep> {
        switch (decision.action) {
            case 'noop':
            case 'baseline':
                return 'none';
            case 'pull':
                if (!remote) {
                    throw new Error(`Pull decision has no remote snapshot for ${filePath}`);
                }
                await this.journalAction(
                    'pull', filePath, local, remote, remote.hash,
                    () => remote.fileType === 'doc'
                        && local?.fileType === 'doc'
                        && remote.text !== undefined
                        && local.text !== undefined
                        ? this.convergeTextToLocal(
                            filePath,
                            remote.text,
                            local.text!,
                            local.hash,
                        )
                        : this.writeLocalSnapshot(remote, local?.hash),
                );
                return 'acted';
            case 'push':
                if (!local) {
                    throw new Error(`Push decision has no local snapshot for ${filePath}`);
                }
                if (remote?.readonly) {
                    await this.createConflict(
                        filePath,
                        'The remote entity is read-only and cannot accept the local change',
                        scan,
                    );
                    return 'acted';
                }
                if (local.fileType === 'file' && !this.isBinaryStable(filePath, local.hash)) {
                    this.status = { status: 'push', message: `Waiting for stable binary write: ${filePath}` };
                    this.scheduleReconcile(`binary stability: ${filePath}`, BINARY_STABILITY_MS);
                    return 'waiting';
                }
                await this.pushLocalSnapshot(filePath, baseline, local, remote);
                return 'acted';
            case 'delete-local':
                await this.journalAction(
                    'delete-local', filePath, local, remote, undefined,
                    () => this.deleteLocalSnapshot(filePath, local),
                );
                return 'acted';
            case 'delete-remote':
                if (remote?.readonly) {
                    await this.createConflict(
                        filePath,
                        'The remote entity is read-only and cannot be deleted',
                        scan,
                    );
                    return 'acted';
                }
                await this.journalAction(
                    'delete-remote', filePath, local, remote, undefined,
                    () => this.vfs.remove(this.remoteUri(filePath), true),
                );
                return 'acted';
            case 'merge-text': {
                if (!local || !remote || remote.text === undefined || local.text === undefined) {
                    throw new Error(`Text merge lacks content for ${filePath}`);
                }
                if (remote.readonly) {
                    await this.createConflict(
                        filePath,
                        'The remote document is read-only and cannot accept the merged operation',
                        scan,
                    );
                    return 'acted';
                }
                const targetBytes = new TextEncoder().encode(decision.content);
                const targetHash = contentHash(targetBytes);
                await this.journalAction(
                    'push', filePath, local, remote, targetHash,
                    async () => {
                        const actual = await this.vfs.writeDocumentAgainstAncestor(
                            this.remoteUri(filePath),
                            remote.text!,
                            decision.content,
                        );
                        await this.updatePendingTarget(new TextEncoder().encode(actual));
                        await this.convergeTextToLocal(
                            filePath,
                            actual,
                            local.text!,
                            local.hash,
                        );
                    },
                );
                return 'acted';
            }
            case 'conflict':
                if (
                    baseline?.fileType === 'file'
                    && local?.fileType === 'file'
                    && remote?.fileType === 'file'
                    && !this.isBinaryStable(filePath, local.hash)
                ) {
                    this.status = { status: 'need-attention', message: `Waiting to archive stable binary: ${filePath}` };
                    this.scheduleReconcile(`binary conflict stability: ${filePath}`, BINARY_STABILITY_MS);
                    return 'waiting';
                }
                await this.createConflict(filePath, decision.reason, scan);
                if (
                    baseline?.fileType === 'file'
                    && local?.fileType === 'file'
                    && remote?.fileType === 'file'
                    && local.hash !== remote.hash
                ) {
                    await this.journalAction(
                        'pull', filePath, local, remote, remote.hash,
                        () => this.writeLocalSnapshot(remote, local.hash),
                    );
                }
                return 'acted';
        }
    }

    private async pushLocalSnapshot(
        filePath: string,
        baseline: BaselineEntry | undefined,
        local: ReplicaSnapshot,
        remote: ReplicaSnapshot | undefined,
    ): Promise<void> {
        await this.journalAction(
            'push', filePath, local, remote, local.hash,
            async () => {
                if (local.fileType === 'folder') {
                    await this.vfs.mkdir(this.remoteUri(filePath));
                    return;
                }
                if (!local.content) {
                    throw new Error(`Local content was not scanned for ${filePath}`);
                }
                if (!remote) {
                    const created = await this.vfs.createTypedFile(
                        this.remoteUri(filePath),
                        local.content,
                        local.fileType,
                    );
                    await this.updatePendingTarget(created.content);
                    if (local.fileType === 'doc' && local.text !== undefined) {
                        await this.convergeTextToLocal(
                            filePath,
                            new TextDecoder().decode(created.content),
                            local.text,
                            local.hash,
                        );
                    } else {
                        await this.advanceBinaryBaselineIfLocalChanged(
                            filePath,
                            local,
                            created.entityId,
                            created.content,
                        );
                    }
                    return;
                }
                if (remote.fileType === 'doc') {
                    if (remote.text === undefined || local.text === undefined) {
                        throw new Error(`Document is not valid UTF-8: ${filePath}`);
                    }
                    const actual = await this.vfs.writeDocumentAgainstAncestor(
                        this.remoteUri(filePath),
                        remote.text,
                        local.text,
                    );
                    await this.updatePendingTarget(new TextEncoder().encode(actual));
                    await this.convergeTextToLocal(
                        filePath,
                        actual,
                        local.text,
                        local.hash,
                    );
                    return;
                }
                const entityId = await this.vfs.writeResourceAgainstBaseline(
                    this.remoteUri(filePath),
                    remote.entityId!,
                    remote.hash,
                    local.content,
                );
                await this.advanceBinaryBaselineIfLocalChanged(
                    filePath,
                    local,
                    entityId,
                    local.content,
                );
            },
        );
    }

    private async journalAction(
        kind: ReplicaActionKind,
        filePath: string,
        local: ReplicaSnapshot | undefined,
        remote: ReplicaSnapshot | undefined,
        targetHash: string | undefined,
        action: () => Thenable<unknown> | Promise<unknown>,
        targetPath?: string,
        entityId?: string,
    ): Promise<void> {
        const state = this.requireState();
        state.pendingAction = {
            id: randomUUID(),
            kind,
            path: filePath,
            targetPath,
            startedAt: new Date().toISOString(),
            expectedLocalHash: local?.hash,
            expectedRemoteHash: remote?.hash,
            targetHash,
            entityId,
        };
        await this.stateStore.save(state);
        this.status = {
            status: kind.includes('local') || kind === 'pull' ? 'pull' : 'push',
            message: `${kind}: ${filePath}${targetPath ? ` -> ${targetPath}` : ''}`,
        };
        try {
            await action();
        } catch (error) {
            if (
                error instanceof StaleLocalSnapshotError
                && (kind === 'pull' || kind === 'converge-local')
            ) {
                // A pull has not modified the remote endpoint. Discard its
                // journal entry so the fresh local bytes can be reconciled as
                // a genuine concurrent change on the next scan.
                state.pendingAction = undefined;
                await this.stateStore.save(state);
            }
            throw error;
        }
    }

    private pendingActionCompleted(pending: PendingReplicaAction, scan: ScanPair): boolean {
        const local = scan.local.get(pending.path);
        const remote = scan.remote.get(pending.path);
        switch (pending.kind) {
            case 'push':
            case 'pull':
            case 'resolve-conflict':
                return local !== undefined
                    && remote !== undefined
                    && local.fileType === remote.fileType
                    && local.hash === remote.hash;
            case 'delete-local':
                return local === undefined;
            case 'delete-remote':
                return remote === undefined;
            case 'move-local':
                return pending.targetPath !== undefined
                    && !local
                    && scan.local.has(pending.targetPath);
            case 'move-remote':
                return pending.targetPath !== undefined
                    && !remote
                    && scan.remote.get(pending.targetPath)?.entityId === pending.entityId;
            case 'converge-local':
                return local !== undefined
                    && local.localVariants === undefined
                    && local.hash === pending.targetHash;
        }
    }

    private async resumePendingAction(
        pending: PendingReplicaAction,
        scan: ScanPair,
    ): Promise<boolean> {
        if (pending.kind !== 'push' && pending.kind !== 'pull' && pending.kind !== 'resolve-conflict') {
            return false;
        }
        const local = scan.local.get(pending.path);
        const remote = scan.remote.get(pending.path);
        if (
            remote
            && remote.hash === pending.targetHash
            && local?.hash === pending.expectedLocalHash
        ) {
            await this.writeLocalSnapshot(remote, local?.hash);
            return true;
        }
        if (
            remote
            && remote.hash === pending.targetHash
            && !local
            && pending.expectedLocalHash === undefined
        ) {
            await this.writeLocalSnapshot(remote, undefined);
            return true;
        }
        return false;
    }

    private async updatePendingTarget(content: Uint8Array): Promise<void> {
        const state = this.requireState();
        if (!state.pendingAction) {
            throw new Error('Remote write completed without a durable pending action');
        }
        state.pendingAction.targetHash = contentHash(content);
        await this.stateStore.save(state);
    }

    private async scanBoth(): Promise<ScanPair> {
        await this.vfs.init();
        const remote = await this.scanRemote();
        const local = await this.scanLocal(remote);
        return { local, remote };
    }

    private async scanRemote(): Promise<Map<string, ReplicaSnapshot>> {
        const result = new Map<string, ReplicaSnapshot>();
        const entities = this.vfs.walk(entity => entity._type !== 'outputs');
        for (const { entity, path: entityPath } of entities) {
            const filePath = normalizeRelativePath(entityPath);
            if (!filePath || this.isIgnored(filePath)) {
                continue;
            }
            this.assertSafePath(filePath);
            if (entity._type === 'folder') {
                result.set(filePath, {
                    path: filePath,
                    fileType: 'folder',
                    entityId: entity._id,
                    hash: 'folder',
                    readonly: entity.readonly,
                });
                continue;
            }
            if (entity._type !== 'doc' && entity._type !== 'file') {
                continue;
            }
            const snapshot = await this.vfs.readAuthoritativeFile(this.remoteUri(filePath));
            const text = snapshot.fileType === 'doc'
                ? new TextDecoder('utf-8', { fatal: true }).decode(snapshot.content)
                : undefined;
            result.set(filePath, {
                path: filePath,
                fileType: snapshot.fileType,
                entityId: snapshot.entityId,
                hash: contentHash(snapshot.content),
                content: snapshot.content,
                text,
                readonly: entity.readonly,
            });
        }
        attachTreeHashes(result);
        return result;
    }

    private async scanLocal(
        remote: Map<string, ReplicaSnapshot>,
    ): Promise<Map<string, ReplicaSnapshot>> {
        const result = new Map<string, ReplicaSnapshot>();
        const queue = [''];
        const openDocuments = new Map(
            vscode.workspace.textDocuments
                .filter(document => this.relativeLocalPath(document.uri) !== undefined)
                .map(document => [document.uri.fsPath, document]),
        );
        while (queue.length > 0) {
            const parent = queue.shift()!;
            const entries = await vscode.workspace.fs.readDirectory(this.localUri(parent));
            for (const [name, type] of entries) {
                const filePath = normalizeRelativePath(parent ? `${parent}/${name}` : name);
                if (this.isIgnored(filePath)) {
                    continue;
                }
                this.assertSafePath(filePath);
                const uri = this.localUri(filePath);
                if ((type & vscode.FileType.SymbolicLink) !== 0) {
                    throw new Error(`Symbolic links are not supported by safe sync: ${filePath}`);
                }
                const identity = await this.localIdentity(uri);
                if ((type & vscode.FileType.Directory) !== 0) {
                    result.set(filePath, {
                        path: filePath,
                        fileType: 'folder',
                        hash: 'folder',
                        localIdentity: identity,
                    });
                    queue.push(filePath);
                    continue;
                }
                if ((type & vscode.FileType.File) === 0) {
                    throw new Error(`Unsupported local filesystem entity: ${filePath}`);
                }
                const openDocument = openDocuments.get(uri.fsPath);
                const diskContent = await vscode.workspace.fs.readFile(uri);
                const knownType = this.state?.entries[filePath]?.fileType
                    ?? remote.get(filePath)?.fileType;
                const fileType = knownType === 'doc' || knownType === 'file'
                    ? knownType
                    : (isEditableTextCandidate(filePath, diskContent) ? 'doc' : 'file');
                let content = diskContent;
                let localVariants: ReplicaSnapshot['localVariants'];
                if (openDocument?.isDirty && fileType === 'doc') {
                    const editorContent = new TextEncoder().encode(openDocument.getText());
                    const diskHash = contentHash(diskContent);
                    const editorHash = contentHash(editorContent);
                    if (diskHash === editorHash) {
                        content = editorContent;
                    } else {
                        const storedBaseline = this.state?.entries[filePath];
                        const remoteBaseline = remote.get(filePath);
                        const ancestor = storedBaseline?.fileType === 'doc'
                            ? storedBaseline.text
                            : remoteBaseline?.fileType === 'doc' && remoteBaseline.hash === diskHash
                                ? remoteBaseline.text
                                : undefined;
                        if (ancestor !== undefined && diskHash === storedBaseline?.hash) {
                            // Only the editor changed; it is the logical local snapshot.
                            content = editorContent;
                        } else {
                            let mergedContent: Uint8Array | undefined;
                            if (ancestor !== undefined) {
                                try {
                                    const diskText = new TextDecoder('utf-8', { fatal: true }).decode(diskContent);
                                    const editorText = new TextDecoder('utf-8', { fatal: true }).decode(editorContent);
                                    mergedContent = new TextEncoder().encode(
                                        mergeConcurrentText(ancestor, diskText, editorText),
                                    );
                                } catch {
                                    mergedContent = undefined;
                                }
                            }
                            content = editorContent;
                            localVariants = {
                                diskContent,
                                editorContent,
                                diskHash,
                                editorHash,
                                mergedContent,
                            };
                        }
                    }
                }
                let text: string | undefined;
                if (fileType === 'doc') {
                    try {
                        text = new TextDecoder('utf-8', { fatal: true }).decode(content);
                    } catch {
                        text = undefined;
                    }
                }
                result.set(filePath, {
                    path: filePath,
                    fileType,
                    hash: contentHash(content),
                    content,
                    text,
                    localIdentity: identity,
                    localVariants,
                });
            }
        }
        attachTreeHashes(result);
        return result;
    }

    private async localIdentity(uri: vscode.Uri): Promise<string> {
        const stat = await lstat(uri.fsPath);
        return `${stat.dev}:${stat.ino}`;
    }

    private async writeLocalSnapshot(
        remote: ReplicaSnapshot,
        expectedLocalHash?: string,
    ): Promise<void> {
        if (remote.fileType === 'folder') {
            if (expectedLocalHash === undefined) {
                await vscode.workspace.fs.createDirectory(this.localUri(remote.path));
            }
            return;
        }
        if (!remote.content) {
            throw new Error(`Remote content was not scanned for ${remote.path}`);
        }
        await this.writeLocalContent(
            remote.path,
            remote.content,
            expectedLocalHash,
            remote.fileType === 'doc',
        );
    }

    private async writeLocalContent(
        filePath: string,
        content: Uint8Array,
        expectedLocalHash?: string,
        asText = false,
    ): Promise<void> {
        const uri = this.localUri(filePath);
        const openDocument = asText
            ? vscode.workspace.textDocuments.find(document => document.uri.fsPath === uri.fsPath)
            : undefined;
        const current = await this.readCurrentLocalBytes(uri, openDocument);
        if (
            (current === undefined) !== (expectedLocalHash === undefined)
            || (current !== undefined && contentHash(current) !== expectedLocalHash)
        ) {
            throw new StaleLocalSnapshotError(
                `Local content changed during scan: ${filePath}`,
                current,
            );
        }
        if (
            current !== undefined
            && contentHash(current) === contentHash(content)
            && !openDocument?.isDirty
        ) {
            return;
        }
        await vscode.workspace.fs.createDirectory(vscode.Uri.file(path.dirname(uri.fsPath)));
        if (openDocument) {
            const edit = new vscode.WorkspaceEdit();
            const end = openDocument.positionAt(openDocument.getText().length);
            edit.replace(uri, new vscode.Range(new vscode.Position(0, 0), end), new TextDecoder().decode(content));
            if (!await vscode.workspace.applyEdit(edit) || !await openDocument.save()) {
                throw new Error(`VS Code could not save synchronized content for ${filePath}`);
            }
        } else {
            await vscode.workspace.fs.writeFile(uri, content);
        }
        const diskAfter = await vscode.workspace.fs.readFile(uri);
        const visibleAfter = openDocument
            ? new TextEncoder().encode(openDocument.getText())
            : diskAfter;
        if (
            contentHash(diskAfter) !== contentHash(content)
            || contentHash(visibleAfter) !== contentHash(content)
        ) {
            throw new StaleLocalSnapshotError(
                `Local content changed while saving synchronized text: ${filePath}`,
                visibleAfter,
            );
        }
    }

    private async convergeTextToLocal(
        filePath: string,
        authoritativeText: string,
        expectedLocalText: string,
        expectedLocalHash: string,
    ): Promise<void> {
        let remoteText = authoritativeText;
        let localAncestor = expectedLocalText;
        let localHash = expectedLocalHash;
        for (let attempt = 0; attempt < 10; attempt++) {
            try {
                await this.writeLocalContent(
                    filePath,
                    new TextEncoder().encode(remoteText),
                    localHash,
                    true,
                );
                return;
            } catch (error) {
                if (!(error instanceof StaleLocalSnapshotError) || !error.actualContent) {
                    throw error;
                }
                const newerLocalText = new TextDecoder('utf-8', { fatal: true })
                    .decode(error.actualContent);
                const merged = mergeConcurrentText(localAncestor, newerLocalText, remoteText);
                remoteText = await this.vfs.writeDocumentAgainstAncestor(
                    this.remoteUri(filePath),
                    remoteText,
                    merged,
                );
                await this.updatePendingTarget(new TextEncoder().encode(remoteText));
                localAncestor = newerLocalText;
                localHash = contentHash(error.actualContent);
            }
        }
        throw new Error(`Local text kept changing during synchronization: ${filePath}`);
    }

    private async advanceBinaryBaselineIfLocalChanged(
        filePath: string,
        scannedLocal: ReplicaSnapshot,
        remoteEntityId: string,
        authoritativeContent: Uint8Array,
    ): Promise<void> {
        const uri = this.localUri(filePath);
        const current = await this.readCurrentLocalBytes(uri);
        if (current && contentHash(current) === scannedLocal.hash) {
            return;
        }
        const state = this.requireState();
        state.entries[filePath] = {
            entityId: remoteEntityId,
            fileType: 'file',
            hash: contentHash(authoritativeContent),
            localIdentity: current ? await this.localIdentity(uri) : scannedLocal.localIdentity,
        };
        state.pendingAction = undefined;
        await this.stateStore.save(state);
    }

    private async readCurrentLocalBytes(
        uri: vscode.Uri,
        openDocument?: vscode.TextDocument,
    ): Promise<Uint8Array | undefined> {
        if (openDocument) {
            return new TextEncoder().encode(openDocument.getText());
        }
        try {
            return await vscode.workspace.fs.readFile(uri);
        } catch (error) {
            if (this.isFileNotFound(error)) {
                return undefined;
            }
            throw error;
        }
    }

    private async deleteLocalSnapshot(
        filePath: string,
        expected: ReplicaSnapshot | undefined,
    ): Promise<void> {
        if (!expected) {
            return;
        }
        const uri = this.localUri(filePath);
        if (expected.fileType !== 'folder') {
            const current = await this.readCurrentLocalBytes(uri);
            if (!current || contentHash(current) !== expected.hash) {
                throw new StaleLocalSnapshotError(`Local content changed before deletion: ${filePath}`);
            }
        }
        await vscode.workspace.fs.delete(uri, { recursive: true, useTrash: false });
    }

    private async ensureLocalParent(filePath: string): Promise<void> {
        const parent = this.parentPath(filePath);
        if (parent) {
            await vscode.workspace.fs.createDirectory(this.localUri(parent));
        }
    }

    private isBinaryStable(filePath: string, hash: string): boolean {
        const now = Date.now();
        const previous = this.binaryStability.get(filePath);
        if (!previous || previous.hash !== hash) {
            this.binaryStability.set(filePath, { hash, since: now });
            return false;
        }
        return now - previous.since >= BINARY_STABILITY_MS;
    }

    private async createConflict(filePath: string, reason: string, scan: ScanPair): Promise<void> {
        const state = this.requireState();
        if (state.conflicts[filePath]) {
            return;
        }
        const id = `${this.timestamp()}-${randomUUID()}`;
        const artifactDirectory = `.overleaf/conflicts/unresolved/${id}`;
        const localFile = await this.archiveSubtree(artifactDirectory, 'local', filePath, scan.local);
        const remoteFile = await this.archiveSubtree(artifactDirectory, 'remote', filePath, scan.remote);
        const record: ConflictRecord = {
            path: filePath,
            artifactDirectory,
            reason,
            createdAt: new Date().toISOString(),
            localFile,
            remoteFile,
            localHash: scan.local.get(filePath)?.hash,
            remoteHash: scan.remote.get(filePath)?.hash,
        };
        state.conflicts[filePath] = record;
        await this.writeArtifact(
            `${artifactDirectory}/manifest.json`,
            new TextEncoder().encode(JSON.stringify(record, null, 2)),
        );
        await this.stateStore.save(state);
        this.status = { status: 'need-attention', message: `Conflict paused: ${filePath}` };
    }

    private async archiveLocalWriterMerge(
        filePath: string,
        variants: NonNullable<ReplicaSnapshot['localVariants']>,
    ): Promise<void> {
        const root = `.overleaf/conflicts/resolved/local-writer-merge-`
            + `${this.timestamp()}-${randomUUID()}`;
        await this.writeArtifact(`${root}/disk/${filePath}`, variants.diskContent);
        await this.writeArtifact(`${root}/editor/${filePath}`, variants.editorContent);
        if (variants.mergedContent) {
            await this.writeArtifact(`${root}/merged/${filePath}`, variants.mergedContent);
        }
        await this.writeArtifact(
            `${root}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                createdAt: new Date().toISOString(),
                path: filePath,
                reason: 'Concurrent editor-buffer and on-disk text changes were OT-merged',
                diskHash: variants.diskHash,
                editorHash: variants.editorHash,
                mergedHash: variants.mergedContent
                    ? contentHash(variants.mergedContent)
                    : undefined,
            }, null, 2)),
        );
    }

    private async archiveSubtree(
        artifactDirectory: string,
        side: 'local' | 'remote',
        filePath: string,
        snapshots: Map<string, ReplicaSnapshot>,
    ): Promise<string | undefined> {
        const entries = [...snapshots]
            .filter(([candidate]) => candidate === filePath || candidate.startsWith(`${filePath}/`))
            .sort(([left], [right]) => left.localeCompare(right));
        if (entries.length === 0) {
            return undefined;
        }
        const root = `${artifactDirectory}/${side}/${filePath}`;
        for (const [candidate, snapshot] of entries) {
            const target = `${artifactDirectory}/${side}/${candidate}`;
            if (snapshot.fileType === 'folder') {
                await vscode.workspace.fs.createDirectory(this.localUri(target));
            } else if (snapshot.content) {
                await this.writeArtifact(target, snapshot.content);
            }
            if (side === 'local' && snapshot.localVariants) {
                await this.writeArtifact(
                    `${artifactDirectory}/local-disk/${candidate}`,
                    snapshot.localVariants.diskContent,
                );
                await this.writeArtifact(
                    `${artifactDirectory}/local-editor/${candidate}`,
                    snapshot.localVariants.editorContent,
                );
            }
        }
        return root;
    }

    private async writeArtifact(filePath: string, content: Uint8Array): Promise<void> {
        const uri = this.localUri(filePath);
        await vscode.workspace.fs.createDirectory(vscode.Uri.file(path.dirname(uri.fsPath)));
        await vscode.workspace.fs.writeFile(uri, content);
    }

    private renderReport(scan: ScanPair, reason: string): void {
        const allPaths = new Set([...scan.local.keys(), ...scan.remote.keys()]);
        const localOnly: string[] = [];
        const remoteOnly: string[] = [];
        const different: string[] = [];
        const localWriterOverlaps = [...scan.local]
            .filter(([, snapshot]) => snapshot.localVariants !== undefined)
            .map(([filePath]) => filePath);
        let equal = 0;
        for (const filePath of [...allPaths].sort()) {
            const local = scan.local.get(filePath);
            const remote = scan.remote.get(filePath);
            if (!local) {
                remoteOnly.push(filePath);
            } else if (!remote) {
                localOnly.push(filePath);
            } else if (local.fileType !== remote.fileType || local.hash !== remote.hash) {
                different.push(filePath);
            } else {
                equal++;
            }
        }
        const capability = this.vfs.safeSyncCapability;
        const lines = [
            `Hershel Overleaf safe-sync report`,
            `Generated: ${new Date().toISOString()}`,
            `Reason: ${reason}`,
            `Server: ${this.vfs.serverName}`,
            `Project: ${this.vfs.projectName} (${this.vfs.projectId})`,
            `Local replica: ${this.baseUri.fsPath}`,
            `Protocol: ${capability.protocol} (${capability.reason})`,
            `Baseline: ${this.state?.initialized ? 'initialized' : 'NOT SELECTED - READ ONLY'}`,
            `Equal: ${equal}; local only: ${localOnly.length}; remote only: ${remoteOnly.length}; different: ${different.length}`,
            `Active conflicts: ${Object.keys(this.state?.conflicts ?? {}).length}`,
            `Local editor/disk overlaps: ${localWriterOverlaps.length}`,
            '',
            ...this.reportGroup('Local only', localOnly),
            ...this.reportGroup('Remote only', remoteOnly),
            ...this.reportGroup('Different', different),
            ...this.reportGroup('Local editor/disk overlaps', localWriterOverlaps),
        ];
        this.lastReport = lines.join('\n');
        this.output.clear();
        this.output.appendLine(this.lastReport);
    }

    private reportGroup(label: string, paths: string[]): string[] {
        return paths.length === 0
            ? [`${label}: (none)`, '']
            : [`${label}:`, ...paths.map(filePath => `  ${filePath}`), ''];
    }

    private async initializeBaseline(): Promise<void> {
        if (!this.state || this.pausedError) {
            return;
        }
        if (this.state.pendingAction || Object.keys(this.state.conflicts).length > 0) {
            vscode.window.showErrorMessage(
                'Finish the pending action and resolve all active conflicts before selecting a new baseline.',
            );
            return;
        }
        const scan = await this.scanBoth();
        this.lastScan = scan;
        this.renderReport(scan, 'explicit baseline preview');
        this.output.show(true);
        const remoteChoice = 'Trust Overleaf and preserve differing local files (Recommended)';
        const localChoice = 'Trust local replica and preserve differing Overleaf files';
        const selection = await vscode.window.showQuickPick(
            [
                { label: remoteChoice, description: 'Overleaf becomes canonical; local differences are archived first.' },
                { label: localChoice, description: 'Local files become canonical; remote differences are archived first.' },
            ],
            {
                ignoreFocusOut: true,
                title: 'Select the first trusted safe-sync baseline',
                placeHolder: 'No endpoint will be modified until you make this explicit selection.',
            },
        );
        if (!selection) {
            return;
        }
        if (!this.vfs.safeSyncCapability.writable && selection.label === localChoice) {
            vscode.window.showErrorMessage('This server is read-only because its OT capability was not verified.');
            return;
        }
        if (selection.label === localChoice) {
            const blocked = [...scan.remote.values()].find(remote => {
                const local = scan.local.get(remote.path);
                return remote.readonly
                    && (!local || local.fileType !== remote.fileType || local.hash !== remote.hash);
            });
            if (blocked) {
                vscode.window.showErrorMessage(
                    `Local baseline cannot replace read-only remote entity: ${blocked.path}`,
                );
                return;
            }
        }
        const confirmation = await vscode.window.showWarningMessage(
            selection.label === remoteChoice
                ? 'Initialize from Overleaf? Differing local files will be archived before replacement.'
                : 'Initialize from local files? Differing remote files will be archived before replacement.',
            { modal: true },
            'Initialize',
        );
        if (confirmation !== 'Initialize') {
            return;
        }
        try {
            if (selection.label === remoteChoice) {
                await this.initializeFromRemote(scan);
            } else {
                await this.initializeFromLocal(scan);
            }
            this.scheduleReconcile('baseline initialized', 0);
        } catch (error) {
            if (error instanceof BaselineSelectionChangedError) {
                const state = this.requireState();
                state.initialized = false;
                state.pendingAction = undefined;
                await this.stateStore.save(state);
                this.status = {
                    status: 'need-attention',
                    message: 'Baseline selection changed during copy; review a fresh report',
                };
                vscode.window.showWarningMessage(error.message);
                return;
            }
            this.handleReconcileError(error);
        }
    }

    private async initializeFromRemote(scan: ScanPair): Promise<void> {
        const state = this.requireState();
        await this.archiveBootstrap('overleaf-baseline', 'local', scan.local, scan.remote);
        const localExtras = [...scan.local.keys()]
            .filter(filePath => !scan.remote.has(filePath))
            .sort((left, right) => right.split('/').length - left.split('/').length);
        for (const filePath of localExtras) {
            const snapshot = scan.local.get(filePath)!;
            await this.deleteLocalSnapshot(filePath, snapshot);
        }
        const remoteEntries = [...scan.remote.values()].sort((left, right) => (
            left.path.split('/').length - right.path.split('/').length
        ));
        for (const remote of remoteEntries) {
            const local = scan.local.get(remote.path);
            if (local?.fileType !== remote.fileType && local) {
                await this.deleteLocalSnapshot(remote.path, local);
            }
            if (!local || local.fileType !== remote.fileType || local.hash !== remote.hash) {
                await this.writeLocalSnapshot(
                    remote,
                    local?.fileType === remote.fileType ? local.hash : undefined,
                );
            }
        }
        const finalScan = await this.scanBoth();
        this.assertEndpointsEqual(finalScan);
        state.initialized = true;
        state.pendingAction = undefined;
        refreshConvergedBaseline(state, finalScan.local, finalScan.remote);
        await this.stateStore.save(state);
    }

    private async initializeFromLocal(scan: ScanPair): Promise<void> {
        const state = this.requireState();
        await this.archiveBootstrap('local-baseline', 'remote', scan.remote, scan.local);
        const localVariants = new Map(
            [...scan.local].filter(([, snapshot]) => snapshot.localVariants !== undefined),
        );
        if (localVariants.size > 0) {
            await this.archiveBootstrap(
                'local-writer-variants',
                'local',
                localVariants,
                new Map(),
            );
        }
        for (const [filePath, local] of scan.local) {
            if (local.localVariants) {
                const selectedContent = local.localVariants.mergedContent
                    ?? local.localVariants.editorContent;
                await this.writeLocalContent(
                    filePath,
                    selectedContent,
                    local.localVariants.editorHash,
                    true,
                );
            }
        }
        const working = await this.scanBoth();
        const remoteExtrasOrMismatches = [...working.remote.values()]
            .filter(remote => {
                const local = working.local.get(remote.path);
                return !local || local.fileType !== remote.fileType;
            })
            .sort((left, right) => right.path.split('/').length - left.path.split('/').length);
        for (const remote of remoteExtrasOrMismatches) {
            try {
                await this.vfs.remove(this.remoteUri(remote.path), true);
            } catch (error) {
                if (!this.isFileNotFound(error)) {
                    throw error;
                }
            }
        }
        const localEntries = [...working.local.values()].sort((left, right) => (
            left.path.split('/').length - right.path.split('/').length
        ));
        for (const local of localEntries) {
            const remote = working.remote.get(local.path);
            if (local.fileType === 'folder') {
                if (!remote || remote.fileType !== 'folder') {
                    await this.vfs.mkdir(this.remoteUri(local.path));
                }
                continue;
            }
            if (!local.content) {
                throw new Error(`Local content was not scanned for ${local.path}`);
            }
            if (!remote || remote.fileType !== local.fileType) {
                await this.vfs.createTypedFile(this.remoteUri(local.path), local.content, local.fileType);
            } else if (remote.hash !== local.hash && local.fileType === 'doc') {
                if (remote.text === undefined || local.text === undefined) {
                    throw new Error(`Document is not valid UTF-8: ${local.path}`);
                }
                const actual = await this.vfs.writeDocumentAgainstAncestor(
                    this.remoteUri(local.path), remote.text, local.text,
                );
                if (actual !== local.text) {
                    throw new BaselineSelectionChangedError(
                        `Remote document changed during baseline selection: ${local.path}`,
                    );
                }
            } else if (remote.hash !== local.hash) {
                await this.vfs.writeResourceAgainstBaseline(
                    this.remoteUri(local.path), remote.entityId!, remote.hash, local.content,
                );
            }
        }
        const finalScan = await this.scanBoth();
        this.assertEndpointsEqual(finalScan);
        state.initialized = true;
        state.pendingAction = undefined;
        refreshConvergedBaseline(state, finalScan.local, finalScan.remote);
        await this.stateStore.save(state);
    }

    private async archiveBootstrap(
        label: string,
        side: 'local' | 'remote',
        source: Map<string, ReplicaSnapshot>,
        canonical: Map<string, ReplicaSnapshot>,
    ): Promise<void> {
        const root = `.overleaf/conflicts/resolved/bootstrap-${label}-${this.timestamp()}-${randomUUID()}`;
        const differing = new Map(
            [...source].filter(([filePath, snapshot]) => {
                const other = canonical.get(filePath);
                return snapshot.localVariants !== undefined
                    || !other
                    || other.fileType !== snapshot.fileType
                    || other.hash !== snapshot.hash;
            }),
        );
        for (const [filePath, snapshot] of differing) {
            const target = `${root}/${side}/${filePath}`;
            if (snapshot.fileType === 'folder') {
                await vscode.workspace.fs.createDirectory(this.localUri(target));
            } else if (snapshot.content) {
                await this.writeArtifact(target, snapshot.content);
            }
            if (snapshot.localVariants) {
                await this.writeArtifact(
                    `${root}/${side}-disk/${filePath}`,
                    snapshot.localVariants.diskContent,
                );
                await this.writeArtifact(
                    `${root}/${side}-editor/${filePath}`,
                    snapshot.localVariants.editorContent,
                );
            }
        }
        await this.writeArtifact(
            `${root}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                createdAt: new Date().toISOString(),
                selectedBaseline: label,
                archivedSide: side,
                files: [...differing.keys()],
            }, null, 2)),
        );
    }

    private assertEndpointsEqual(scan: ScanPair): void {
        const paths = new Set([...scan.local.keys(), ...scan.remote.keys()]);
        for (const filePath of paths) {
            const local = scan.local.get(filePath);
            const remote = scan.remote.get(filePath);
            if (
                !local
                || !remote
                || local.localVariants
                || local.fileType !== remote.fileType
                || local.hash !== remote.hash
            ) {
                throw new BaselineSelectionChangedError(
                    `Endpoints changed while selecting the baseline: ${filePath}`,
                );
            }
        }
    }

    private async resolveConflict(): Promise<void> {
        const state = this.requireState();
        const records = Object.values(state.conflicts);
        const selected = await vscode.window.showQuickPick(
            records.map(record => ({ label: record.path, description: record.reason, record })),
            { ignoreFocusOut: true, title: 'Resolve a paused safe-sync conflict' },
        );
        if (!selected) {
            return;
        }
        const useRemote = 'Use current Overleaf version';
        const useLocal = 'Use archived local version';
        const keepBoth = 'Keep Overleaf version and restore local copy beside it';
        const resolution = await vscode.window.showQuickPick(
            [useRemote, useLocal, keepBoth],
            { ignoreFocusOut: true, title: `Resolve ${selected.record.path}` },
        );
        if (!resolution) {
            return;
        }
        const scan = await this.scanBoth();
        const record = selected.record;
        if (resolution === useRemote) {
            await this.restoreRemotePath(record.path, scan);
        } else {
            await this.restoreRemotePath(record.path, scan);
            delete state.conflicts[record.path];
            const canonicalScan = await this.scanBoth();
            refreshConvergedBaseline(state, canonicalScan.local, canonicalScan.remote);
            if (record.localFile) {
                const target = resolution === keepBoth
                    ? this.conflictSibling(record.path)
                    : record.path;
                await this.copyLocalTree(record.localFile, target);
            } else if (resolution === useLocal) {
                const local = canonicalScan.local.get(record.path);
                if (local) {
                    await this.deleteLocalSnapshot(record.path, local);
                }
            }
        }
        delete state.conflicts[record.path];
        await this.moveConflictToResolved(record);
        await this.stateStore.save(state);
        this.scheduleReconcile(`conflict resolved: ${record.path}`, 0);
    }

    private async restoreRemotePath(
        filePath: string,
        _scan: ScanPair,
        attempt = 0,
    ): Promise<void> {
        let current = await this.scanBoth();
        const remoteEntries = [...current.remote.values()]
            .filter(snapshot => snapshot.path === filePath || snapshot.path.startsWith(`${filePath}/`))
            .sort((left, right) => left.path.split('/').length - right.path.split('/').length);
        const localRoot = current.local.get(filePath);
        if (remoteEntries.length === 0) {
            if (localRoot) {
                await this.deleteLocalSnapshot(filePath, localRoot);
            }
            const verification = await this.scanBoth();
            if (this.isSubtreeConverged(filePath, verification)) {
                return;
            }
            if (attempt >= 4) {
                throw new Error(`Overleaf kept changing while resolving ${filePath}; conflict remains paused`);
            }
            return this.restoreRemotePath(filePath, verification, attempt + 1);
        }
        const localExtras = [...current.local.values()]
            .filter(snapshot => (
                (snapshot.path === filePath || snapshot.path.startsWith(`${filePath}/`))
                && !current.remote.has(snapshot.path)
            ))
            .sort((left, right) => right.path.split('/').length - left.path.split('/').length);
        for (const extra of localExtras) {
            await this.deleteLocalSnapshot(extra.path, extra);
        }
        current = await this.scanBoth();
        for (const remote of remoteEntries) {
            let local = current.local.get(remote.path);
            if (local && local.fileType !== remote.fileType) {
                await this.deleteLocalSnapshot(remote.path, local);
                current = await this.scanBoth();
                local = current.local.get(remote.path);
            }
            if (!local || local.fileType !== remote.fileType || local.hash !== remote.hash) {
                await this.writeLocalSnapshot(remote, local?.fileType === remote.fileType ? local.hash : undefined);
                current = await this.scanBoth();
            }
        }
        const verification = await this.scanBoth();
        if (!this.isSubtreeConverged(filePath, verification)) {
            if (attempt >= 4) {
                throw new Error(`Overleaf kept changing while resolving ${filePath}; conflict remains paused`);
            }
            return this.restoreRemotePath(filePath, verification, attempt + 1);
        }
    }

    private isSubtreeConverged(filePath: string, scan: ScanPair): boolean {
        const paths = new Set(
            [...scan.local.keys(), ...scan.remote.keys()]
                .filter(candidate => candidate === filePath || candidate.startsWith(`${filePath}/`)),
        );
        for (const candidate of paths) {
            const local = scan.local.get(candidate);
            const remote = scan.remote.get(candidate);
            if (
                !local
                || !remote
                || local.localVariants
                || local.fileType !== remote.fileType
                || local.hash !== remote.hash
            ) {
                return false;
            }
        }
        return true;
    }

    private async copyLocalTree(sourcePath: string, targetPath: string): Promise<void> {
        const source = this.localUri(sourcePath);
        const stat = await vscode.workspace.fs.stat(source);
        if ((stat.type & vscode.FileType.Directory) !== 0) {
            await vscode.workspace.fs.createDirectory(this.localUri(targetPath));
            for (const [name] of await vscode.workspace.fs.readDirectory(source)) {
                await this.copyLocalTree(`${sourcePath}/${name}`, `${targetPath}/${name}`);
            }
        } else {
            const content = await vscode.workspace.fs.readFile(source);
            await this.writeLocalContent(
                targetPath,
                content,
                undefined,
                isEditableTextCandidate(targetPath, content),
            );
        }
    }

    private async moveConflictToResolved(record: ConflictRecord): Promise<void> {
        const target = record.artifactDirectory.replace(
            '.overleaf/conflicts/unresolved/',
            '.overleaf/conflicts/resolved/',
        );
        await vscode.workspace.fs.createDirectory(this.localUri(this.parentPath(target)));
        await vscode.workspace.fs.rename(
            this.localUri(record.artifactDirectory),
            this.localUri(target),
            { overwrite: false },
        );
    }

    private conflictSibling(filePath: string): string {
        const extension = path.posix.extname(filePath);
        const stem = extension ? filePath.slice(0, -extension.length) : filePath;
        return `${stem}.local-conflict-${this.timestamp()}${extension}`;
    }

    private async cleanupResolvedArtifacts(): Promise<void> {
        const confirmation = await vscode.window.showWarningMessage(
            'Permanently delete all resolved safe-sync artifacts?',
            { modal: true },
            'Delete resolved artifacts',
        );
        if (confirmation !== 'Delete resolved artifacts') {
            return;
        }
        try {
            await vscode.workspace.fs.delete(
                this.localUri('.overleaf/conflicts/resolved'),
                { recursive: true, useTrash: false },
            );
        } catch (error) {
            if (!this.isFileNotFound(error)) {
                throw error;
            }
        }
    }

    private handleReconcileError(error: unknown): void {
        const normalized = this.asError(error);
        if (normalized instanceof StaleLocalSnapshotError
            || normalized instanceof ConcurrentRemoteResourceError) {
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, 100);
            return;
        }
        this.failProject(normalized);
    }

    private failProject(error: Error): void {
        if (this.pausedError) {
            return;
        }
        this.pausedError = error;
        this.status = { status: 'need-attention', message: `Project paused: ${error.message}` };
        this.output.appendLine(`[PROJECT PAUSED] ${error.stack ?? error.message}`);
        vscode.window.showErrorMessage(`Hershel safe sync paused this project: ${error.message}`);
    }

    private async retryPausedProject(): Promise<void> {
        this.pausedError = undefined;
        try {
            this.state = await this.stateStore.load();
            await this.reconcileUntilStable('manual retry');
        } catch (error) {
            this.failProject(this.asError(error));
        }
    }

    private requireState(): ReplicaState {
        if (!this.state) {
            throw new Error('Replica state is unavailable');
        }
        return this.state;
    }

    private localUri(filePath: string): vscode.Uri {
        const normalized = normalizeRelativePath(filePath);
        return normalized
            ? vscode.Uri.joinPath(this.baseUri, ...normalized.split('/'))
            : this.baseUri;
    }

    private remoteUri(filePath: string): vscode.Uri {
        const normalized = normalizeRelativePath(filePath);
        return normalized
            ? this.vfs.pathToUri(...normalized.split('/'))
            : this.vfs.origin;
    }

    private relativeLocalPath(uri: vscode.Uri): string | undefined {
        if (uri.scheme !== 'file') {
            return undefined;
        }
        const relative = path.relative(this.baseUri.fsPath, uri.fsPath);
        if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
            return relative === '' ? '' : undefined;
        }
        return normalizeRelativePath(relative);
    }

    private parentPath(filePath: string): string {
        const parent = path.posix.dirname(normalizeRelativePath(filePath));
        return parent === '.' ? '' : parent;
    }

    private assertSafePath(filePath: string): void {
        if (filePath.split('/').some(part => part === '' || part === '.' || part === '..')) {
            throw new Error(`Unsafe project path: ${filePath}`);
        }
    }

    private timestamp(): string {
        return new Date().toISOString().replace(/[:.]/g, '-');
    }

    private asError(error: unknown): Error {
        return error instanceof Error ? error : new Error(String(error));
    }

    private isFileNotFound(error: unknown): boolean {
        return error instanceof vscode.FileSystemError && error.code === 'FileNotFound';
    }

    writeFile(filePath: string, content: Uint8Array): Thenable<void> {
        return vscode.workspace.fs.writeFile(this.localUri(filePath), content);
    }

    async readFile(filePath: string): Promise<Uint8Array | undefined> {
        try {
            return await vscode.workspace.fs.readFile(this.localUri(filePath));
        } catch (error) {
            if (this.isFileNotFound(error)) {
                return undefined;
            }
            throw error;
        }
    }

    get triggers(): Promise<vscode.Disposable[]> {
        return this.initWatch();
    }

    public static get baseUriInputBox(): vscode.QuickPick<vscode.QuickPickItem> {
        const separator = path.sep;
        const inputBox = vscode.window.createQuickPick();
        inputBox.placeholder = vscode.l10n.t('e.g., /home/user/empty/local/folder');
        inputBox.value = require('os').homedir() + separator;
        inputBox.onDidChangeValue(async value => {
            try {
                inputBox.busy = true;
                const parent = value.split(separator).slice(0, -1).join(separator);
                const items = await vscode.workspace.fs.readDirectory(vscode.Uri.file(parent));
                const subDirectories = items
                    .filter(([, type]) => type === vscode.FileType.Directory)
                    .filter(([name]) => `${parent}${separator}${name}`.startsWith(value));
                const candidates = subDirectories.map(([name]) => ({
                    label: name, alwaysShow: true, picked: false,
                }));
                if (parent !== '') {
                    candidates.unshift({ label: '..', alwaysShow: true, picked: false });
                }
                inputBox.items = candidates;
            } finally {
                inputBox.busy = false;
                inputBox.activeItems = [];
            }
        });
        inputBox.onDidAccept(() => {
            if (inputBox.activeItems.length !== 0) {
                const selected = inputBox.selectedItems[0];
                const parent = inputBox.value.split(separator).slice(0, -1).join(separator);
                inputBox.value = selected.label === '..'
                    ? parent
                    : `${parent}${separator}${selected.label}${separator}`;
            }
        });
        return inputBox;
    }

    get settingItems(): SettingItem[] {
        const items: SettingItem[] = [
            {
                label: 'Show safe-sync report',
                callback: async () => {
                    if (this.lastScan) {
                        this.renderReport(this.lastScan, 'manual report');
                    }
                    this.output.show(true);
                },
            },
            {
                label: 'Synchronize now',
                callback: async () => {
                    try {
                        await this.reconcileUntilStable('manual synchronization');
                    } catch (error) {
                        this.handleReconcileError(error);
                    }
                },
            },
            {
                label: this.state?.initialized
                    ? 'Re-select safe-sync baseline ...'
                    : 'Initialize safe-sync baseline ...',
                callback: () => this.initializeBaseline(),
            },
            {
                label: 'Configure sync ignore patterns ...',
                callback: () => this.configureIgnorePatterns(),
            },
            {
                label: 'Clean up resolved conflict artifacts ...',
                callback: () => this.cleanupResolvedArtifacts(),
            },
        ];
        if (Object.keys(this.state?.conflicts ?? {}).length > 0) {
            items.splice(3, 0, {
                label: 'Resolve safe-sync conflict ...',
                callback: () => this.resolveConflict(),
            });
        }
        if (this.pausedError) {
            items.unshift({
                label: 'Retry paused safe sync',
                callback: () => this.retryPausedProject(),
            });
        }
        return items;
    }

    private async configureIgnorePatterns(): Promise<void> {
        const customPatterns = [...(this.getSetting<string[]>(IGNORE_SETTING_KEY) ?? [])];
        const patterns = () => [...new Set([...DEFAULT_IGNORE_PATTERNS, ...customPatterns])].sort();
        const quickPick = vscode.window.createQuickPick();
        quickPick.ignoreFocusOut = true;
        quickPick.title = 'Build-artifact defaults are fixed; custom patterns can be added or removed.';
        const refresh = () => {
            quickPick.items = patterns().map(pattern => ({
                label: pattern,
                description: DEFAULT_IGNORE_PATTERNS.includes(pattern) ? 'built-in' : 'custom',
                buttons: DEFAULT_IGNORE_PATTERNS.includes(pattern)
                    ? []
                    : [{ iconPath: new vscode.ThemeIcon('trash') }],
            }));
        };
        refresh();
        quickPick.onDidTriggerItemButton(async ({ item }) => {
            const index = customPatterns.indexOf(item.label);
            if (index >= 0) {
                customPatterns.splice(index, 1);
                this.setSetting(IGNORE_SETTING_KEY, customPatterns);
                refresh();
            }
        });
        quickPick.onDidAccept(async () => {
            if (quickPick.selectedItems.length === 0 && quickPick.value) {
                customPatterns.push(quickPick.value);
                this.setSetting(IGNORE_SETTING_KEY, customPatterns);
                quickPick.value = '';
                refresh();
            }
        });
        quickPick.show();
    }

    list(): Iterable<CommitItem> { return []; }
    async apply(_commitItem: CommitItem): Promise<void> { return Promise.resolve(); }
    async syncFromSCM(_commits: Iterable<CommitItem>): Promise<void> { return Promise.resolve(); }
}
