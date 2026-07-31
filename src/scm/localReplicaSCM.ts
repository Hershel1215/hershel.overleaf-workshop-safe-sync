import { randomUUID } from 'crypto';
import { lstat, readFile } from 'fs/promises';
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
import { ReplicaLock, ReplicaLockedError } from '../sync/replicaLock';
import { SerialTaskQueue } from '../sync/serialTaskQueue';
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
import { createLocalReplicaBinding, ensureLocalReplicaBinding } from '../sync/localBinding';
import {
    TextReplicaIsolation,
    TextReplicaStateMachine,
} from '../sync/textReplicaStateMachine';
import {
    AiGatewayRequest,
    AiGatewayServer,
} from '../sync/aiGateway';
import {
    AiWriterLease,
    AiWriterLeaseManager,
} from '../sync/aiWriterLease';
import { FairTaskScheduler } from '../sync/fairTaskScheduler';
import { WalCorruptionError } from '../sync/durableWal';

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

export interface LocalReplicaRuntimeStatus {
    active: boolean;
    lockHeld: boolean;
    startupSettled: boolean;
    paused: boolean;
    initialized: boolean;
    pendingAction: boolean;
    conflictCount: number;
    settled: boolean;
    idle: boolean;
}

export interface LocalReplicaNamespaceCleanup {
    localRemoved: boolean;
    remoteRemoved: boolean;
    removedBaselineEntries: number;
}

type ConflictResolution = 'remote' | 'local' | 'both';

/**
 * A durable, fail-closed local replica of one Overleaf project.
 *
 * Text changes are transformed through the VFS document session. Binary and
 * tree changes use a persisted common baseline and never use timestamps as a
 * winner signal.
 */
export class LocalReplicaSCMProvider extends BaseSCM {
    public static readonly label = vscode.l10n.t('Local Replica');

    private static readonly activeInstances = new Map<string, LocalReplicaSCMProvider>();

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
    private readonly reconcileQueue = new SerialTaskQueue();
    private readonly textReplicas = new Map<string, TextReplicaStateMachine>();
    private readonly textSchedulers = new Map<string, FairTaskScheduler>();
    private readonly aiLeases = new AiWriterLeaseManager();
    private readonly aiTransactions = new Map<string, {
        lease: AiWriterLease;
        path: string;
        baseContent: string;
        entityId: string;
    }>();
    private aiGateway?: AiGatewayServer;
    private observationReady = false;
    private readonly recoveryObservations = new Set<string>();
    private pausedError?: Error;
    private disposed = false;
    private lockHeld = false;
    private startupSettled = false;
    private activeReconcileCount = 0;
    private lastScan?: ScanPair;
    private lastReport = 'Safe sync has not scanned this replica yet.';
    private currentActionPath?: string;

    public static getRuntimeStatus(basePath: string): LocalReplicaRuntimeStatus | undefined {
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            return undefined;
        }
        const pendingAction = instance.state?.pendingAction !== undefined;
        const conflictCount = Object.keys(instance.state?.conflicts ?? {}).length;
        const active = !instance.disposed;
        const settled = active
            && instance.lockHeld
            && instance.startupSettled
            && instance.pausedError === undefined
            && instance.activeReconcileCount === 0
            && instance.reconcileQueue.isIdle
            && instance.reconcileTimer === undefined
            && instance.pendingReasons.size === 0;
        return {
            active,
            lockHeld: instance.lockHeld,
            startupSettled: instance.startupSettled,
            paused: instance.pausedError !== undefined,
            initialized: instance.state?.initialized === true,
            pendingAction,
            conflictCount,
            settled,
            idle: settled
                && instance.status.status === 'idle'
                && !pendingAction
                && conflictCount === 0,
        };
    }

    public static showActiveReport(): void {
        const instance = LocalReplicaSCMProvider.activeWorkspaceInstance();
        if (!instance) {
            vscode.window.showWarningMessage('No active Hershel safe-sync replica in this workspace');
            return;
        }
        if (instance.lastScan) {
            instance.renderReport(instance.lastScan, 'manual report');
        }
        instance.output.clear();
        instance.output.appendLine(instance.lastReport);
        instance.output.show(true);
    }

    public static async copyActiveAiGatewayCommand(): Promise<void> {
        const instance = LocalReplicaSCMProvider.activeWorkspaceInstance();
        if (!instance) {
            vscode.window.showWarningMessage('No active Hershel safe-sync replica in this workspace');
            return;
        }
        const metadataPath = path.join(
            instance.baseUri.fsPath,
            '.overleaf',
            'safe-sync-gateway.json',
        );
        const metadata = JSON.parse(await readFile(metadataPath, 'utf8')) as {cliPath?: string};
        if (!metadata.cliPath) {
            throw new Error('AI gateway CLI path is unavailable');
        }
        const command = `node ${shellQuote(metadata.cliPath)} ${shellQuote(metadataPath)}`
            + ' begin <relative-path> codex';
        await vscode.env.clipboard.writeText(command);
        vscode.window.showInformationMessage('Copied the Hershel AI gateway command');
    }

    private static activeWorkspaceInstance(): LocalReplicaSCMProvider | undefined {
        const workspace = vscode.workspace.workspaceFolders?.[0]?.uri;
        return workspace?.scheme === 'file'
            ? LocalReplicaSCMProvider.activeInstances.get(path.resolve(workspace.fsPath))
            : undefined;
    }

    public static cleanupNamespace(
        basePath: string,
        namespace: string,
    ): Promise<LocalReplicaNamespaceCleanup> {
        if (!/^hershel-e2e-[a-zA-Z0-9_-]+$/.test(namespace)) {
            throw new Error(`Refusing to clean non-E2E namespace: ${namespace}`);
        }
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            throw new Error(`No active Local Replica runtime for ${path.resolve(basePath)}`);
        }
        return instance.reconcileQueue.run(async () => {
            instance.activeReconcileCount++;
            try {
                return await instance.cleanupReservedNamespace(namespace);
            } finally {
                instance.activeReconcileCount--;
            }
        });
    }

    public static resolveE2EConflictUsingRemote(
        basePath: string,
        filePath: string,
    ): Promise<string> {
        if (!/^hershel-e2e-[a-zA-Z0-9_-]+\/.+/.test(filePath)
            || filePath.split('/').some(part => part === '' || part === '.' || part === '..')) {
            throw new Error(`Refusing to resolve non-E2E conflict: ${filePath}`);
        }
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            throw new Error(`No active Local Replica runtime for ${path.resolve(basePath)}`);
        }
        return instance.enqueueConflictResolution(filePath, 'remote');
    }

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
        this.startupSettled = false;
        this.observationReady = false;
        this.recoveryObservations.clear();
        if (this.baseUri.scheme !== 'file') {
            throw new Error('Safe local replicas require a file:// directory');
        }
        await ensureLocalReplicaBinding(
            this.baseUri.fsPath,
            createLocalReplicaBinding(
                this.vfs.origin.toString(),
                this.vfs.serverName,
                this.vfs.projectName,
            ),
            this.vfs.projectId,
        );
        this.lock = await ReplicaLock.acquire(
            this.context.globalStorageUri.fsPath,
            this.baseUri.fsPath,
            {
                onLost: error => {
                    this.lockHeld = false;
                    this.failProject(error);
                },
            },
        );
        this.lockHeld = true;
        await this.vfs.init();
        try {
            this.state = await this.stateStore.load();
            if (this.state.aiLeaseState) {
                const recovered = this.state.aiLeaseState;
                this.aiLeases.restore({
                    nextFencingToken: recovered.nextFencingToken,
                    leases: [],
                });
                this.state.aiLeaseState = this.aiLeases.serialize();
                await this.stateStore.save(this.state);
            }
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
            this.vfsWatcher.onDidChange(uri => this.onRemoteFileEvent(uri, 'change')),
            this.vfsWatcher.onDidCreate(uri => this.onRemoteFileEvent(uri, 'create')),
            this.vfsWatcher.onDidDelete(uri => this.onRemoteFileEvent(uri, 'delete')),
            this.localWatcher.onDidChange(uri => this.onLocalFileEvent(uri, 'change')),
            this.localWatcher.onDidCreate(uri => this.onLocalFileEvent(uri, 'create')),
            this.localWatcher.onDidDelete(uri => this.onLocalFileEvent(uri, 'delete')),
            vscode.workspace.onDidChangeTextDocument(event => {
                const relative = this.relativeLocalPath(event.document.uri);
                if (relative !== undefined && !this.isIgnored(relative)) {
                    if (!this.observationReady) {
                        this.recoveryObservations.add(`editor:${relative}`);
                    } else {
                        void this.onLocalTextDocumentChange(relative, event.document);
                    }
                }
            }),
            vscode.workspace.onWillSaveTextDocument(event => {
                const relative = this.relativeLocalPath(event.document.uri);
                const machine = relative === undefined ? undefined : this.textReplicas.get(relative);
                if (this.observationReady && relative !== undefined && machine && !this.isIgnored(relative)) {
                    event.waitUntil(this.textTask(
                        relative,
                        'editor-save',
                        () => machine.beforeEditorSave(event.document.getText()),
                    ));
                }
            }),
            vscode.workspace.onDidSaveTextDocument(document => {
                const relative = this.relativeLocalPath(document.uri);
                const machine = relative === undefined ? undefined : this.textReplicas.get(relative);
                if (!this.observationReady && relative !== undefined) {
                    this.recoveryObservations.add(`editor:${relative}`);
                    this.recoveryObservations.add(`local:${relative}`);
                } else if (relative !== undefined && machine && !this.isIgnored(relative)) {
                    void this.textTask(
                        relative,
                        'editor-save',
                        () => machine.onEditorSaved(document.getText()),
                    ).catch(error => {
                        this.handleTextReplicaError(relative, error);
                    });
                }
            }),
        ];

        LocalReplicaSCMProvider.activeInstances.set(path.resolve(this.baseUri.fsPath), this);

        try {
            if (!this.pausedError) {
                await this.enqueueReconcile('startup');
                await this.startAiGateway();
                if (!this.pausedError) {
                    this.observationReady = true;
                    await this.replayRecoveryObservations();
                }
            }
        } finally {
            this.startupSettled = true;
        }

        return [
            this.vfsWatcher,
            this.localWatcher,
            ...listeners,
            new vscode.Disposable(() => {
                this.disposed = true;
                this.observationReady = false;
                for (const machine of this.textReplicas.values()) {
                    machine.dispose();
                }
                this.textReplicas.clear();
                for (const scheduler of this.textSchedulers.values()) {
                    scheduler.dispose();
                }
                this.textSchedulers.clear();
                void this.stopAiGateway();
                void this.stateStore.close();
                if (this.reconcileTimer) {
                    clearTimeout(this.reconcileTimer);
                    this.reconcileTimer = undefined;
                }
                try {
                    this.lock?.releaseSync();
                    this.lockHeld = false;
                } catch (error) {
                    console.error('Failed to release safe-sync replica lock', error);
                }
                this.unregisterRuntime();
            }),
        ];
    }

    public dispose(): void {
        this.disposed = true;
        this.observationReady = false;
        for (const machine of this.textReplicas.values()) {
            machine.dispose();
        }
        this.textReplicas.clear();
        for (const scheduler of this.textSchedulers.values()) {
            scheduler.dispose();
        }
        this.textSchedulers.clear();
        void this.stopAiGateway();
        void this.stateStore.close();
        if (this.reconcileTimer) {
            clearTimeout(this.reconcileTimer);
            this.reconcileTimer = undefined;
        }
        try {
            this.lock?.releaseSync();
            this.lockHeld = false;
        } catch (error) {
            console.error('Failed to release safe-sync replica lock', error);
        }
        this.unregisterRuntime();
        this.output.dispose();
    }

    private unregisterRuntime(): void {
        const key = path.resolve(this.baseUri.fsPath);
        if (LocalReplicaSCMProvider.activeInstances.get(key) === this) {
            LocalReplicaSCMProvider.activeInstances.delete(key);
        }
    }

    private onLocalFileEvent(uri: vscode.Uri, kind: string): void {
        const relative = this.relativeLocalPath(uri);
        if (relative === undefined || this.isIgnored(relative)) {
            return;
        }
        if (!this.observationReady) {
            this.recoveryObservations.add(`local:${relative}`);
            return;
        }
        const machine = this.textReplicas.get(relative);
        if (machine) {
            if (kind === 'delete') {
                this.dropTextReplica(relative);
                this.scheduleReconcile(`local delete: ${relative}`, 0);
                return;
            }
            // Read inside the document queue. Reading before enqueueing lets a
            // concurrent remote projection overwrite the file first, causing
            // the watcher to observe the plugin's bytes instead of the user's
            // write.
            void this.textTask(relative, 'filesystem', async () => {
                await machine.onDiskObserved(await this.readLocalText(relative));
            }).catch(error => {
                this.handleTextReplicaError(relative, error);
            });
            return;
        }
        this.scheduleReconcile(`local ${kind}: ${relative}`, TEXT_DEBOUNCE_MS);
    }

    private onRemoteFileEvent(uri: vscode.Uri, kind: string): void {
        const relative = this.relativeRemotePath(uri);
        if (relative === undefined || this.isIgnored(relative)) {
            return;
        }
        if (!this.observationReady) {
            this.recoveryObservations.add(`remote:${relative}`);
            return;
        }
        const machine = this.textReplicas.get(relative);
        if (machine) {
            if (kind === 'delete') {
                this.dropTextReplica(relative);
                this.scheduleReconcile(`remote delete: ${relative}`, 0);
                return;
            }
            void this.textTask(relative, 'remote', async () => {
                await machine.onCanonicalChanged(await this.readRemoteText(relative));
            }).catch(error => {
                this.handleTextReplicaError(relative, error);
            });
            return;
        }
        this.scheduleReconcile(`remote ${kind}: ${relative}`, 100);
    }

    private async onLocalTextDocumentChange(
        filePath: string,
        document: vscode.TextDocument,
    ): Promise<void> {
        if (!this.observationReady) {
            this.recoveryObservations.add(`editor:${filePath}`);
            return;
        }
        const machine = this.textReplicas.get(filePath);
        if (!machine) {
            this.scheduleReconcile(`text edit before replica attach: ${filePath}`, TEXT_DEBOUNCE_MS);
            return;
        }
        try {
            await this.textTask(
                filePath,
                'editor',
                () => machine.onEditorChanged(document.getText()),
            );
            this.status = { status: 'push', message: `Journalizing text: ${filePath}` };
        } catch (error) {
            this.handleTextReplicaError(filePath, error);
        }
    }

    private async replayRecoveryObservations(): Promise<void> {
        const observations = [...this.recoveryObservations];
        this.recoveryObservations.clear();
        for (const encoded of observations) {
            const separator = encoded.indexOf(':');
            const source = encoded.slice(0, separator);
            const filePath = encoded.slice(separator + 1);
            if (source === 'editor') {
                const document = vscode.workspace.textDocuments.find(
                    candidate => candidate.uri.fsPath === this.localUri(filePath).fsPath,
                );
                if (document) {
                    await this.onLocalTextDocumentChange(filePath, document);
                }
            } else if (source === 'local') {
                this.onLocalFileEvent(this.localUri(filePath), 'recovery replay');
            } else if (source === 'remote') {
                this.onRemoteFileEvent(this.remoteUri(filePath), 'recovery replay');
            }
        }
    }

    private async ensureTextReplicas(scan: ScanPair): Promise<void> {
        // Path-level deletes and type changes belong to the resource planner.
        // Drop a stale OT machine before it can recreate a deleted path.
        for (const filePath of [...this.textReplicas.keys()]) {
            const local = scan.local.get(filePath);
            const remote = scan.remote.get(filePath);
            if (local?.fileType !== 'doc' || remote?.fileType !== 'doc') {
                this.dropTextReplica(filePath);
            }
        }
        for (const [filePath, remote] of scan.remote) {
            if (remote.fileType !== 'doc' || remote.text === undefined || isPathConflicted(filePath, this.requireState())) {
                continue;
            }
            const local = scan.local.get(filePath);
            if (local && local.fileType !== 'doc') {
                // Let the conservative path/type planner create an explicit
                // conflict instead of overwriting a binary at this path.
                continue;
            }
            if (local?.localVariants) {
                // The resource planner owns the proved editor/disk fork. It
                // archives and merges both local branches before the OT
                // document attaches and rebases that result onto Overleaf.
                continue;
            }
            if (!local && this.state?.entries[filePath]) {
                // A missing local path may be an intentional local delete (or
                // a delete/edit overlap); preserve that evidence for the
                // planner rather than projecting the remote document.
                continue;
            }
            const previousActionPath = this.currentActionPath;
            this.currentActionPath = filePath;
            let completed = false;
            try {
                // The directory scan exposes the last server-acknowledged
                // snapshot. The VFS projection additionally contains any
                // local operation that is already durable in the document
                // journal but has not been acknowledged yet. Never reset a
                // replica from the older scan snapshot: doing so can roll back
                // or later duplicate a journaled operation during recovery.
                const remoteUri = this.remoteUri(filePath);
                const projection = await this.vfs.readDocumentProjection(remoteUri);
                if (projection.entityId !== remote.entityId) {
                    throw new ConcurrentRemoteResourceError(remoteUri.path);
                }
                const canonicalContent = projection.content;
                const existing = this.textReplicas.get(filePath);
                if (existing) {
                    if (!existing.isIsolated
                        && existing.canonical !== canonicalContent
                        && local?.fileType === 'doc') {
                        await this.textTask(
                            filePath,
                            'remote-scan',
                            () => existing.onCanonicalChanged(canonicalContent),
                        );
                    }
                } else {
                    await this.createTextReplica(filePath, canonicalContent, local);
                }
                completed = true;
            } finally {
                if (completed) {
                    this.currentActionPath = previousActionPath;
                }
            }
        }
    }

    private dropTextReplica(filePath: string): void {
        this.textReplicas.get(filePath)?.dispose();
        this.textReplicas.delete(filePath);
        this.textSchedulers.get(filePath)?.dispose();
        this.textSchedulers.delete(filePath);
    }

    private async createTextReplica(
        filePath: string,
        canonicalContent: string,
        local: ReplicaSnapshot | undefined,
    ): Promise<void> {
        if (this.textReplicas.has(filePath)) {
            return;
        }
        const openDocument = vscode.workspace.textDocuments.find(
            document => document.uri.fsPath === this.localUri(filePath).fsPath,
        );
        const diskContent = await this.readLocalText(filePath);
        const editorContent = openDocument?.getText();
        const baseline = this.state?.entries[filePath];
        const baselineText = baseline?.fileType === 'doc' ? baseline.text : undefined;
        const editorIsDirty = openDocument?.isDirty === true;
        const editorAndDiskDiffer = editorContent !== undefined
            && diskContent !== undefined
            && editorContent !== diskContent;
        const dirtyEditorHasProvableBase = editorIsDirty
            && editorContent !== undefined
            && baselineText !== undefined
            && (diskContent === baselineText || editorContent === diskContent);

        if (editorIsDirty && editorContent !== undefined && editorAndDiskDiffer && !dirtyEditorHasProvableBase) {
            await this.createTextConflict({
                path: filePath,
                reason: 'Dirty editor and disk snapshots do not share a provable common base',
                canonicalContent,
                editorContent,
                diskContent,
            });
            return;
        }

        const machine = new TextReplicaStateMachine({
            path: filePath,
            canonicalContent,
            // Seed a dirty editor with its durable base. The first observed
            // value below is then journaled as an OT operation.
            editorContent: dirtyEditorHasProvableBase ? baselineText : editorContent ?? diskContent,
            editorDirty: editorIsDirty,
            // Keep the baseline as the previous disk observation so an
            // uncoordinated filesystem write is classified by evidence.
            diskContent: baselineText ?? diskContent,
            projectionReceipts: this.state?.projectionReceipts?.filter(receipt => (
                receipt.path === `editor:${filePath}`
                || receipt.path === `disk:${filePath}`
            )),
        }, {
            commit: (ancestor, target, barrier) => this.vfs.acceptDocumentSnapshotAgainstAncestor(
                this.remoteUri(filePath),
                ancestor,
                target,
                barrier,
            ),
            projectEditor: async content => {
                const document = vscode.workspace.textDocuments.find(
                    candidate => candidate.uri.fsPath === this.localUri(filePath).fsPath,
                );
                if (!document || document.getText() === content) {
                    return;
                }
                const edit = new vscode.WorkspaceEdit();
                const end = document.positionAt(document.getText().length);
                edit.replace(document.uri, new vscode.Range(
                    new vscode.Position(0, 0),
                    end,
                ), content);
                if (!await vscode.workspace.applyEdit(edit)) {
                    throw new Error(`VS Code rejected internal projection for ${filePath}`);
                }
            },
            projectDisk: async content => {
                await this.ensureLocalParent(filePath);
                await vscode.workspace.fs.writeFile(
                    this.localUri(filePath),
                    new TextEncoder().encode(content),
                );
            },
            readDisk: () => this.readLocalText(filePath),
            onConverged: content => this.advanceTextBaselineIfAuthoritative(
                filePath,
                content,
            ),
            persistProjectionReceipts: async receipts => {
                if (this.state) {
                    const ownedPaths = new Set([
                        `editor:${filePath}`,
                        `disk:${filePath}`,
                    ]);
                    this.state.projectionReceipts = [
                        ...(this.state.projectionReceipts ?? [])
                            .filter(receipt => !ownedPaths.has(receipt.path)),
                        ...receipts,
                    ];
                    await this.stateStore.save(this.state);
                }
            },
            isolate: record => this.createTextConflict(record),
        });
        this.textReplicas.set(filePath, machine);
        this.textSchedulers.set(filePath, new FairTaskScheduler());

        if (!local || local.fileType !== 'doc') {
            // A remote-only document with no local baseline is a new remote
            // entity. Project it after recovery, never by calling save().
            await machine.onCanonicalChanged(canonicalContent);
            return;
        }
        const localText = local.text;
        if (localText === canonicalContent) {
            return;
        }
        if (dirtyEditorHasProvableBase) {
            if (editorContent !== undefined) {
                await machine.onEditorChanged(editorContent);
                await machine.flushInput(true);
            }
            return;
        }
        if (baselineText === canonicalContent && !editorIsDirty) {
            await machine.onDiskObserved(localText);
            return;
        }
        if (baselineText === localText && canonicalContent !== localText) {
            await machine.onCanonicalChanged(canonicalContent);
            return;
        }
        await this.createTextConflict({
            path: filePath,
            reason: 'Text content differed before a v2 operation ancestry could be established',
            canonicalContent,
            editorContent,
            diskContent,
        });
    }

    /**
     * Advance the resource baseline only when the OT session says the server
     * acknowledged these exact bytes and the disk still projects them. This
     * closes the projection-to-delete window without treating an unacked
     * journal operation as a shared ancestor.
     */
    private async advanceTextBaselineIfAuthoritative(
        filePath: string,
        content: string,
    ): Promise<void> {
        const state = this.requireState();
        if (
            !state.initialized
            || isPathConflicted(filePath, state)
            || state.pendingAction?.path === filePath
        ) {
            return;
        }
        let remote;
        try {
            remote = await this.vfs.readAuthoritativeFile(this.remoteUri(filePath));
        } catch (error) {
            if (this.isFileNotFound(error)) {
                return;
            }
            throw error;
        }
        if (remote.fileType !== 'doc') {
            return;
        }
        const bytes = new TextEncoder().encode(content);
        const hash = contentHash(bytes);
        if (contentHash(remote.content) !== hash) {
            // The operation is journaled locally but not server-acknowledged.
            return;
        }
        if (await this.readLocalText(filePath) !== content) {
            // A raw writer already produced the next local operation.
            return;
        }
        let localIdentity: string;
        try {
            localIdentity = await this.localIdentity(this.localUri(filePath));
        } catch (error) {
            if (this.isFileNotFound(error)) {
                return;
            }
            throw error;
        }
        const storedHash = await this.stateStore.saveBaseline(bytes);
        if (storedHash !== hash) {
            throw new Error(`Common-base snapshot hash changed for ${filePath}`);
        }
        // Re-check conflict/pending state after the asynchronous reads. A
        // parallel resource action must retain ownership of its ancestry.
        if (
            isPathConflicted(filePath, state)
            || state.pendingAction?.path === filePath
        ) {
            return;
        }
        state.entries[filePath] = {
            entityId: remote.entityId,
            fileType: 'doc',
            hash,
            text: content,
            localIdentity,
        };
        await this.stateStore.save(state);
    }

    private async createTextConflict(record: TextReplicaIsolation): Promise<void> {
        const state = this.requireState();
        if (state.conflicts[record.path]) {
            return;
        }
        const artifactDirectory = `.overleaf/conflicts/unresolved/${this.timestamp()}-${randomUUID()}`;
        const baseline = state.entries[record.path];
        const baseContent = await this.readCommonBase(baseline);
        const baseFile = baseContent === undefined
            ? undefined
            : `${artifactDirectory}/base/${record.path}`;
        if (baseContent !== undefined && baseFile) {
            await this.writeArtifact(baseFile, baseContent);
        }
        await this.writeArtifact(
            `${artifactDirectory}/remote/${record.path}`,
            new TextEncoder().encode(record.canonicalContent),
        );
        if (record.editorContent !== undefined) {
            await this.writeArtifact(
                `${artifactDirectory}/local-editor/${record.path}`,
                new TextEncoder().encode(record.editorContent),
            );
        }
        if (record.diskContent !== undefined && record.diskContent !== record.editorContent) {
            await this.writeArtifact(
                `${artifactDirectory}/local-disk/${record.path}`,
                new TextEncoder().encode(record.diskContent),
            );
        }
        const remote = this.lastScan?.remote.get(record.path);
        let remoteRevision: number | undefined;
        try {
            remoteRevision = (await this.vfs.readDocumentProjection(
                this.remoteUri(record.path),
            )).revision;
        } catch {
            // The document may be isolated precisely because its live session
            // cannot be recovered. Entity/hash evidence remains available.
        }
        const conflict: ConflictRecord = {
            path: record.path,
            artifactDirectory,
            reason: record.reason,
            createdAt: new Date().toISOString(),
            baseFile,
            baseHash: baseline?.hash,
            baselineEntityId: baseline?.entityId,
            remoteFile: `${artifactDirectory}/remote/${record.path}`,
            remoteEntityId: remote?.entityId,
            remoteRevision,
            localFile: record.editorContent === undefined
                ? record.diskContent === undefined ? undefined : `${artifactDirectory}/local-disk/${record.path}`
                : `${artifactDirectory}/local-editor/${record.path}`,
            remoteHash: contentHash(new TextEncoder().encode(record.canonicalContent)),
            localHash: record.editorContent === undefined
                ? record.diskContent === undefined ? undefined : contentHash(new TextEncoder().encode(record.diskContent))
                : contentHash(new TextEncoder().encode(record.editorContent)),
        };
        await this.writeArtifact(
            `${artifactDirectory}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                schemaVersion: 3,
                ...conflict,
                editorHash: record.editorContent === undefined
                    ? undefined
                    : contentHash(new TextEncoder().encode(record.editorContent)),
                diskHash: record.diskContent === undefined
                    ? undefined
                    : contentHash(new TextEncoder().encode(record.diskContent)),
                baseAvailable: baseFile !== undefined,
            }, null, 2)),
        );
        state.conflicts[record.path] = conflict;
        await this.stateStore.save(state);
        this.status = { status: 'need-attention', message: `Isolated text path: ${record.path}` };
        this.renderReport(this.lastScan ?? {local: new Map(), remote: new Map()}, `isolated ${record.path}`);
    }

    private handleTextReplicaError(filePath: string, error: unknown): void {
        const normalized = this.asError(error);
        void this.createTextConflict({
            path: filePath,
            reason: normalized.message,
            canonicalContent: this.textReplicas.get(filePath)?.canonical ?? '',
        }).catch(conflictError => this.failProject(this.asError(conflictError)));
    }

    private async readRemoteText(filePath: string): Promise<string> {
        const content = await this.vfs.openFile(this.remoteUri(filePath));
        return new TextDecoder('utf-8', { fatal: true }).decode(content);
    }

    private async readLocalText(filePath: string): Promise<string | undefined> {
        try {
            const uri = this.localUri(filePath);
            return new TextDecoder('utf-8', { fatal: true }).decode(
                await vscode.workspace.fs.readFile(uri),
            );
        } catch (error) {
            if (this.isFileNotFound(error)) {
                return undefined;
            }
            throw error;
        }
    }

    private relativeRemotePath(uri: vscode.Uri): string | undefined {
        if (uri.scheme !== this.vfs.origin.scheme) {
            return undefined;
        }
        const prefix = this.vfs.origin.path.replace(/\/$/, '');
        const relative = uri.path.startsWith(prefix) ? uri.path.slice(prefix.length) : undefined;
        return relative === undefined ? undefined : normalizeRelativePath(relative);
    }

    private async startAiGateway(): Promise<void> {
        if (this.aiGateway || this.disposed) {
            return;
        }
        const metadataPath = path.join(this.baseUri.fsPath, '.overleaf', 'safe-sync-gateway.json');
        this.aiGateway = new AiGatewayServer(metadataPath, {
            handle: request => this.handleAiGatewayRequest(request),
        }, undefined, this.context.asAbsolutePath('out/sync/aiGatewayCli.js'));
        try {
            const metadata = await this.aiGateway.start();
            this.output.appendLine(`AI gateway ready: ${metadataPath}`);
            this.output.appendLine(
                `AI gateway CLI: node ${metadata.cliPath} ${metadataPath} begin <path> <holder>`,
            );
        } catch (error) {
            this.aiGateway = undefined;
            this.output.appendLine(`[ai-gateway] unavailable: ${this.asError(error).message}`);
            vscode.window.showWarningMessage(
                `Safe-sync AI gateway unavailable: ${this.asError(error).message}`,
            );
        }
    }

    private async stopAiGateway(): Promise<void> {
        const gateway = this.aiGateway;
        this.aiGateway = undefined;
        if (gateway) {
            await gateway.stop().catch(error => {
                this.output.appendLine(`[ai-gateway] stop failed: ${this.asError(error).message}`);
            });
        }
    }

    private async handleAiGatewayRequest(request: AiGatewayRequest): Promise<unknown> {
        const filePath = typeof request.path === 'string'
            ? normalizeRelativePath(request.path)
            : undefined;
        const scheduler = filePath === undefined ? undefined : this.textSchedulers.get(filePath);
        if (!scheduler) {
            return this.handleAiGatewayRequestInternal(request);
        }
        return scheduler.run('ai', () => this.handleAiGatewayRequestInternal(request));
    }

    private async handleAiGatewayRequestInternal(request: AiGatewayRequest): Promise<unknown> {
        if (!this.observationReady) {
            throw new Error('Safe-sync recovery is still in progress; retry after READY');
        }
        const filePath = typeof request.path === 'string'
            ? normalizeRelativePath(request.path)
            : undefined;
        if (!filePath || filePath.split('/').some(part => part === '' || part === '.' || part === '..')) {
            throw new Error('AI gateway requires a safe relative document path');
        }
        if (this.isIgnored(filePath)) {
            throw new Error(`AI gateway refuses ignored path ${filePath}`);
        }
        if (!this.state?.initialized) {
            throw new Error('Safe-sync baseline is not initialized');
        }
        await this.ensureTextMachineForGateway(filePath);
        const machine = this.textReplicas.get(filePath);
        if (!machine || machine.isIsolated) {
            throw new Error(`Document is isolated or unavailable: ${filePath}`);
        }
        const projection = await this.vfs.readDocumentProjection(this.remoteUri(filePath));
        switch (request.method) {
            case 'begin': {
                const holder = typeof request.holder === 'string' && request.holder.length > 0
                    ? request.holder
                    : 'unknown-ai';
                await machine.flushInput(true);
                const current = await this.vfs.readDocumentProjection(this.remoteUri(filePath));
                const lease = this.aiLeases.acquire(
                    current.entityId,
                    holder,
                    current.revision,
                );
                this.aiTransactions.set(lease.leaseId, {
                    lease,
                    path: filePath,
                    baseContent: current.content,
                    entityId: current.entityId,
                });
                await this.persistAiLeases();
                return {
                    leaseId: lease.leaseId,
                    fencingToken: lease.fencingToken,
                    baseRevision: lease.baseRevision,
                    entityId: current.entityId,
                    path: filePath,
                    content: current.content,
                    revision: current.revision,
                };
            }
            case 'heartbeat': {
                const lease = this.requireAiLease(request, projection.entityId);
                const renewed = this.aiLeases.heartbeat(
                    projection.entityId,
                    lease.leaseId,
                    lease.fencingToken,
                );
                const transaction = this.aiTransactions.get(lease.leaseId);
                if (transaction) {
                    transaction.lease = renewed;
                }
                await this.persistAiLeases();
                return renewed;
            }
            case 'commit': {
                const lease = this.requireAiLease(request, projection.entityId);
                const transaction = this.aiTransactions.get(lease.leaseId);
                if (!transaction || transaction.path !== filePath) {
                    throw new Error(`AI transaction is not active for ${filePath}`);
                }
                if (typeof request.content !== 'string') {
                    throw new Error('AI commit requires target content');
                }
                this.aiLeases.assertCanCommit(
                    projection.entityId,
                    lease.leaseId,
                    lease.fencingToken,
                );
                const accepted = await this.vfs.acceptDocumentSnapshotAgainstAncestor(
                    this.remoteUri(filePath),
                    transaction.baseContent,
                    request.content,
                    true,
                );
                await machine.onCanonicalChanged(accepted.content);
                // Keep the AI lease through the remote-ack boundary. The
                // operation is already journaled, so the request can return
                // promptly; the asynchronous finalizer releases the lease
                // only after the authoritative update (or a fail-closed
                // acknowledgement error).
                accepted.acknowledgement.then(
                    () => void this.finishAiTransaction(lease.leaseId),
                    error => void this.finishAiTransaction(lease.leaseId, error),
                );
                return {
                    opId: accepted.opId,
                    path: filePath,
                    content: accepted.content,
                    journaled: true,
                    remoteAcknowledged: false,
                };
            }
            case 'abort': {
                const lease = this.requireAiLease(request, projection.entityId);
                this.aiTransactions.delete(lease.leaseId);
                this.aiLeases.release(
                    projection.entityId,
                    lease.leaseId,
                    lease.fencingToken,
                );
                await this.persistAiLeases();
                return { aborted: true };
            }
            default:
                throw new Error(`Unsupported AI gateway method: ${String(request.method)}`);
        }
    }

    private async finishAiTransaction(leaseId: string, error?: unknown): Promise<void> {
        const transaction = this.aiTransactions.get(leaseId);
        if (!transaction) {
            return;
        }
        this.aiTransactions.delete(leaseId);
        try {
            this.aiLeases.release(
                transaction.entityId,
                transaction.lease.leaseId,
                transaction.lease.fencingToken,
            );
        } catch {
            // The lease may have expired and been fenced by a later writer.
        }
        try {
            await this.persistAiLeases();
        } catch (persistenceError) {
            this.failProject(this.asError(persistenceError));
        }
        if (error) {
            this.handleTextReplicaError(transaction.path, error);
        }
    }

    private async ensureTextMachineForGateway(filePath: string): Promise<void> {
        if (this.textReplicas.has(filePath)) {
            return;
        }
        const scan = this.lastScan ?? await this.scanBoth();
        await this.ensureTextReplicas(scan);
        if (!this.textReplicas.has(filePath)) {
            throw new Error(`No editable Overleaf document exists at ${filePath}`);
        }
    }

    private textTask<T>(
        filePath: string,
        source: string,
        task: () => Promise<T>,
    ): Promise<T> {
        const scheduler = this.textSchedulers.get(filePath);
        return scheduler
            ? scheduler.run(source, task)
            : task();
    }

    private requireAiLease(request: AiGatewayRequest, entityId: string): AiWriterLease {
        if (typeof request.leaseId !== 'string' || !Number.isInteger(request.fencingToken)) {
            throw new Error('AI gateway request lacks lease credentials');
        }
        const transaction = this.aiTransactions.get(request.leaseId);
        if (
            !transaction
            || transaction.entityId !== entityId
            || transaction.lease.fencingToken !== request.fencingToken
        ) {
            throw new Error('AI gateway lease does not match the document');
        }
        return transaction.lease;
    }

    private async persistAiLeases(): Promise<void> {
        if (!this.state) {
            return;
        }
        this.state.aiLeaseState = this.aiLeases.serialize();
        await this.stateStore.save(this.state);
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
            void this.enqueueReconcile(reasons);
        }, delayMs);
    }

    private enqueueReconcile(reason: string): Promise<void> {
        return this.reconcileQueue
            .run(async () => {
                this.activeReconcileCount++;
                try {
                    await this.reconcileUntilStable(reason);
                } finally {
                    this.activeReconcileCount--;
                }
            })
            .catch(error => this.handleReconcileError(error));
    }

    private enqueueConflictResolution(
        filePath: string,
        resolution: ConflictResolution,
    ): Promise<string> {
        return this.reconcileQueue.run(async () => {
            this.activeReconcileCount++;
            try {
                return await this.resolveConflictRecord(filePath, resolution);
            } finally {
                this.activeReconcileCount--;
            }
        });
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
                const pending = this.state.pendingAction;
                let recovery;
                try {
                    recovery = validatePendingRecovery(
                        pending,
                        scan.local,
                        scan.remote,
                        this.state.entries[pending.path],
                    );
                } catch (error) {
                    await this.createConflict(
                        pending.path,
                        `Pending ${pending.kind} ancestry is not provable: ${this.asError(error).message}`,
                        scan,
                    );
                    // The recoverable snapshots must be durable before the
                    // pending action is cleared. A crash between these saves
                    // therefore re-enters recovery instead of losing ancestry.
                    if (this.state.pendingAction?.id === pending.id) {
                        this.state.pendingAction = undefined;
                        await this.stateStore.save(this.state);
                    }
                    continue;
                }
                if (recovery.kind === 'recovered-pull' || recovery.kind === 'recovered-push') {
                    this.state.entries[pending.path] = recovery.baseline;
                    this.state.pendingAction = undefined;
                    await this.stateStore.save(this.state);
                    continue;
                }
                if (this.pendingActionCompleted(pending, scan)) {
                    await this.refreshConvergedState(this.state, scan);
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

            await this.ensureTextReplicas(scan);

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
                await this.refreshConvergedState(this.state, scan);
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

    private async cleanupReservedNamespace(
        namespace: string,
    ): Promise<LocalReplicaNamespaceCleanup> {
        if (this.disposed || !this.lockHeld || !this.state) {
            throw new Error('Local Replica runtime is not ready for E2E cleanup');
        }
        if (this.pausedError) {
            throw new Error(`Local Replica is paused: ${this.pausedError.message}`);
        }
        if (!this.state.initialized) {
            throw new Error('Local Replica baseline is not initialized');
        }
        if (this.state.pendingAction) {
            throw new Error(
                `Cannot clean E2E namespace with pending ${this.state.pendingAction.kind}`,
            );
        }
        const namespaceConflict = Object.keys(this.state.conflicts).find(filePath => (
            filePath === namespace || filePath.startsWith(`${namespace}/`)
        ));
        if (namespaceConflict) {
            throw new Error(
                `Cannot clean E2E namespace while ${namespaceConflict} is conflicted`,
            );
        }

        const before = await this.scanBoth();
        const local = before.local.get(namespace);
        const remote = before.remote.get(namespace);
        if (remote) {
            await this.vfs.remove(this.remoteUri(namespace), true);
        }
        if (local) {
            await this.deleteLocalSnapshot(namespace, local);
        }

        const after = await this.scanBoth();
        const remainsIn = (entries: Map<string, ReplicaSnapshot>) => (
            [...entries.keys()].some(candidate => (
                candidate === namespace || candidate.startsWith(`${namespace}/`)
            ))
        );
        if (remainsIn(after.remote) || remainsIn(after.local)) {
            throw new Error(`E2E namespace cleanup did not converge: ${namespace}`);
        }

        let removedBaselineEntries = 0;
        for (const filePath of Object.keys(this.state.entries)) {
            if (filePath === namespace || filePath.startsWith(`${namespace}/`)) {
                delete this.state.entries[filePath];
                removedBaselineEntries++;
            }
        }
        for (const filePath of [...this.binaryStability.keys()]) {
            if (filePath === namespace || filePath.startsWith(`${namespace}/`)) {
                this.binaryStability.delete(filePath);
            }
        }
        await this.stateStore.save(this.state);
        this.lastScan = after;
        this.renderReport(after, `exclusive E2E cleanup: ${namespace}`);
        this.status = {status: 'idle', message: 'Safe sync converged'};
        return {
            localRemoved: local !== undefined,
            remoteRemoved: remote !== undefined,
            removedBaselineEntries,
        };
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
                this.textReplicas?.has(filePath)
                && remote?.fileType === 'doc'
                && (local?.fileType === 'doc' || baseline === undefined)
            ) {
                // An attached OT machine owns an existing document (or a
                // genuinely new remote document). Creation/deletion/type
                // changes stay in the conservative resource planner.
                continue;
            }
            if (
                baseline
                && remote
                && remote.entityId !== baseline.entityId
                && local?.hash !== baseline.hash
                && (
                    !local
                    || local.fileType !== remote.fileType
                    || local.hash !== remote.hash
                )
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
            if (this.textReplicas.has(filePath)) {
                continue;
            }
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
                () => this.writeMergedLocalVariants(
                    filePath,
                    variants.mergedContent!,
                    variants,
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
                    async () => {
                        if (
                            remote.fileType === 'doc'
                            && local?.fileType === 'doc'
                            && remote.text !== undefined
                            && local.text !== undefined
                        ) {
                            const converged = await this.convergeTextToLocal(
                                filePath,
                                remote.text,
                                local.text,
                                local.hash,
                            );
                            await this.commitConvergedTextAction(
                                filePath,
                                remote.entityId,
                                converged,
                                local.localIdentity,
                            );
                            return;
                        }
                        await this.writeLocalSnapshot(remote, local?.hash);
                    },
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
                        const converged = await this.convergeTextToLocal(
                            filePath,
                            actual,
                            local.text!,
                            local.hash,
                        );
                        await this.commitConvergedTextAction(
                            filePath,
                            remote.entityId,
                            converged,
                            local.localIdentity,
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
                        const converged = await this.convergeTextToLocal(
                            filePath,
                            new TextDecoder().decode(created.content),
                            local.text,
                            local.hash,
                        );
                        await this.commitConvergedTextAction(
                            filePath,
                            created.entityId,
                            converged,
                            local.localIdentity,
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
                    const converged = await this.convergeTextToLocal(
                        filePath,
                        actual,
                        local.text,
                        local.hash,
                    );
                    await this.commitConvergedTextAction(
                        filePath,
                        remote.entityId,
                        converged,
                        local.localIdentity,
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
            entityId: entityId ?? remote?.entityId,
        };
        await this.stateStore.save(state);
        this.status = {
            status: kind.includes('local') || kind === 'pull' ? 'pull' : 'push',
            message: `${kind}: ${filePath}${targetPath ? ` -> ${targetPath}` : ''}`,
        };
        const previousActionPath = this.currentActionPath;
        this.currentActionPath = filePath;
        let completed = false;
        try {
            await action();
            completed = true;
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
        } finally {
            if (completed) {
                this.currentActionPath = previousActionPath;
            }
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

    private async refreshConvergedState(state: ReplicaState, scan: ScanPair): Promise<void> {
        refreshConvergedBaseline(state, scan.local, scan.remote);
        for (const [filePath, baseline] of Object.entries(state.entries)) {
            if (baseline.fileType === 'folder') {
                continue;
            }
            const remote = scan.remote.get(filePath);
            const local = scan.local.get(filePath);
            if (
                !remote?.content
                || !local
                || remote.fileType !== baseline.fileType
                || local.fileType !== baseline.fileType
                || remote.hash !== baseline.hash
                || local.hash !== baseline.hash
            ) {
                continue;
            }
            const storedHash = await this.stateStore.saveBaseline(remote.content);
            if (storedHash !== baseline.hash) {
                throw new Error(`Common-base snapshot hash changed for ${filePath}`);
            }
        }
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
                    if (this.textReplicas.has(filePath)) {
                        // The text state machine owns editor/disk ancestry. Do
                        // not synthesize a snapshot merge in the resource scan.
                        content = editorContent;
                    } else if (diskHash === editorHash) {
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
            if (!await vscode.workspace.applyEdit(edit)) {
                throw new Error(`VS Code could not project synchronized content for ${filePath}`);
            }
            // Projection is not mutation: materialize the same proven bytes
            // without making TextDocument.save() part of the remote transaction.
            await vscode.workspace.fs.writeFile(uri, content);
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

    private async writeMergedLocalVariants(
        filePath: string,
        content: Uint8Array,
        variants: NonNullable<ReplicaSnapshot['localVariants']>,
    ): Promise<void> {
        const uri = this.localUri(filePath);
        const openDocument = vscode.workspace.textDocuments
            .find(document => document.uri.fsPath === uri.fsPath);
        if (!openDocument || !openDocument.isDirty) {
            throw new StaleLocalSnapshotError(`Dirty editor closed during merge: ${filePath}`);
        }
        const diskBefore = await vscode.workspace.fs.readFile(uri);
        const editorBefore = new TextEncoder().encode(openDocument.getText());
        if (
            contentHash(diskBefore) !== variants.diskHash
            || contentHash(editorBefore) !== variants.editorHash
        ) {
            throw new StaleLocalSnapshotError(
                `Editor or disk content changed during merge: ${filePath}`,
                editorBefore,
            );
        }

        const edit = new vscode.WorkspaceEdit();
        const end = openDocument.positionAt(openDocument.getText().length);
        edit.replace(
            uri,
            new vscode.Range(new vscode.Position(0, 0), end),
            new TextDecoder().decode(content),
        );
        if (!await vscode.workspace.applyEdit(edit)) {
            throw new Error(`VS Code could not update the merged editor buffer for ${filePath}`);
        }
        const diskAtWrite = await vscode.workspace.fs.readFile(uri);
        if (contentHash(diskAtWrite) !== variants.diskHash) {
            throw new StaleLocalSnapshotError(
                `Disk content changed again during merge: ${filePath}`,
                new TextEncoder().encode(openDocument.getText()),
            );
        }

        // TextDocument.save() intentionally rejects this case because the disk
        // changed behind a dirty editor. Both variants were archived above, so
        // materialize the proven OT result to disk after the double hash check.
        await vscode.workspace.fs.writeFile(uri, content);
        const diskAfter = await vscode.workspace.fs.readFile(uri);
        const visibleAfter = new TextEncoder().encode(openDocument.getText());
        if (
            contentHash(diskAfter) !== contentHash(content)
            || contentHash(visibleAfter) !== contentHash(content)
        ) {
            throw new StaleLocalSnapshotError(
                `Merged editor and disk snapshots did not converge: ${filePath}`,
                visibleAfter,
            );
        }
    }

    private async convergeTextToLocal(
        filePath: string,
        authoritativeText: string,
        expectedLocalText: string,
        expectedLocalHash: string,
    ): Promise<string> {
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
                return remoteText;
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

    private async commitConvergedTextAction(
        filePath: string,
        entityId: string | undefined,
        authoritativeText: string,
        localIdentity: string | undefined,
    ): Promise<void> {
        const state = this.requireState();
        if (
            !state.pendingAction
            || (
                state.pendingAction.kind !== 'push'
                && state.pendingAction.kind !== 'pull'
            )
            || state.pendingAction.path !== filePath
            || !entityId
        ) {
            throw new Error(`Text sync completed without a matching journal for ${filePath}`);
        }
        state.entries[filePath] = {
            entityId,
            fileType: 'doc',
            hash: contentHash(new TextEncoder().encode(authoritativeText)),
            text: authoritativeText,
            localIdentity,
        };
        await this.stateStore.saveBaseline(new TextEncoder().encode(authoritativeText));
        state.pendingAction = undefined;
        await this.stateStore.save(state);
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
        await this.stateStore.saveBaseline(authoritativeContent);
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
        const baseline = state.entries[filePath];
        const baseContent = await this.readCommonBase(baseline);
        let baseFile = baseContent === undefined
            ? undefined
            : `${artifactDirectory}/base/${filePath}`;
        if (baseContent !== undefined && baseFile) {
            await this.writeArtifact(baseFile, baseContent);
        } else if (baseline?.fileType === 'folder') {
            baseFile = `${artifactDirectory}/base/manifest.json`;
            const subtreeBaseline = Object.entries(state.entries)
                .filter(([candidate]) => candidate === filePath || candidate.startsWith(`${filePath}/`))
                .map(([candidate, entry]) => ({path: candidate, ...entry}));
            await this.writeArtifact(
                baseFile,
                new TextEncoder().encode(JSON.stringify({schemaVersion: 1, entries: subtreeBaseline}, null, 2)),
            );
        }
        const remote = scan.remote.get(filePath);
        let remoteRevision: number | undefined;
        if (remote?.fileType === 'doc') {
            try {
                remoteRevision = (await this.vfs.readDocumentProjection(
                    this.remoteUri(filePath),
                )).revision;
            } catch {
                // Conflict evidence is still complete enough to recover the
                // three snapshots even if the live OT session is unavailable.
            }
        }
        const record: ConflictRecord = {
            path: filePath,
            artifactDirectory,
            reason,
            createdAt: new Date().toISOString(),
            baseFile,
            baseHash: baseline?.hash,
            baselineEntityId: baseline?.entityId,
            localFile,
            remoteFile,
            localHash: scan.local.get(filePath)?.hash,
            remoteHash: remote?.hash,
            remoteEntityId: remote?.entityId,
            remoteRevision,
        };
        state.conflicts[filePath] = record;
        await this.writeArtifact(
            `${artifactDirectory}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                schemaVersion: 3,
                ...record,
                baseAvailable: baseFile !== undefined,
            }, null, 2)),
        );
        await this.stateStore.save(state);
        this.status = { status: 'need-attention', message: `Conflict paused: ${filePath}` };
    }

    private async readCommonBase(
        baseline: BaselineEntry | undefined,
    ): Promise<Uint8Array | undefined> {
        if (!baseline || baseline.fileType === 'folder') {
            return undefined;
        }
        if (baseline.fileType === 'doc' && baseline.text !== undefined) {
            const content = new TextEncoder().encode(baseline.text);
            if (contentHash(content) !== baseline.hash) {
                throw new Error(`Stored text baseline is corrupt: ${baseline.entityId}`);
            }
            return content;
        }
        return this.stateStore.readBaseline(baseline.hash);
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
            `Recovery/observation: ${this.observationReady ? 'READY' : 'RECOVERING'}`,
            `OT documents: ${this.textReplicas.size}; buffered: ${[...this.textReplicas.values()].filter(machine => machine.hasBufferedInput).length}; isolated: ${[...this.textReplicas.values()].filter(machine => machine.isIsolated).length}`,
            `AI writer leases: ${this.aiTransactions.size}; projection receipts: ${this.state?.projectionReceipts?.length ?? 0}`,
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
        await this.refreshConvergedState(state, finalScan);
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
        await this.refreshConvergedState(state, finalScan);
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
        const record = selected.record;
        const choice: ConflictResolution = resolution === useRemote
            ? 'remote'
            : resolution === useLocal ? 'local' : 'both';
        try {
            await this.enqueueConflictResolution(record.path, choice);
        } catch (error) {
            this.handleReconcileError(error);
        }
    }

    private async resolveConflictRecord(
        filePath: string,
        resolution: ConflictResolution,
    ): Promise<string> {
        const state = this.requireState();
        if (this.disposed || !this.lockHeld || this.pausedError || !state.initialized) {
            throw new Error('Local Replica runtime is not ready for conflict resolution');
        }
        if (state.pendingAction) {
            throw new Error(`Cannot resolve a conflict with pending ${state.pendingAction.kind}`);
        }
        const record = state.conflicts[filePath];
        if (!record) {
            throw new Error(`No active conflict exists for ${filePath}`);
        }

        const scan = await this.scanBoth();
        const local = scan.local.get(filePath);
        const remote = scan.remote.get(filePath);
        if (remote) {
            await this.journalAction(
                'resolve-conflict',
                filePath,
                local,
                remote,
                remote.hash,
                () => this.restoreRemotePath(filePath, scan),
                undefined,
                remote.entityId,
            );
        } else if (local) {
            await this.journalAction(
                'delete-local',
                filePath,
                local,
                undefined,
                undefined,
                () => this.restoreRemotePath(filePath, scan),
            );
        }

        const canonicalScan = await this.scanBoth();
        if (!this.isSubtreeConverged(filePath, canonicalScan)) {
            throw new Error(`Conflict resolution did not converge ${filePath}`);
        }
        if (state.pendingAction) {
            if (!this.pendingActionCompleted(state.pendingAction, canonicalScan)) {
                throw new Error(`Conflict resolution journal did not complete for ${filePath}`);
            }
            state.pendingAction = undefined;
        }
        delete state.conflicts[filePath];
        await this.refreshConvergedState(state, canonicalScan);
        state.conflicts[filePath] = record;
        await this.stateStore.save(state);

        if (resolution !== 'remote') {
            if (record.localFile) {
                const target = resolution === 'both'
                    ? this.conflictSibling(record.path)
                    : record.path;
                await this.copyLocalTree(record.localFile, target);
            } else if (resolution === 'local') {
                const canonicalLocal = canonicalScan.local.get(record.path);
                if (canonicalLocal) {
                    await this.deleteLocalSnapshot(record.path, canonicalLocal);
                }
            }
        }

        const resolvedArtifactDirectory = await this.moveConflictToResolved(record);
        delete state.conflicts[record.path];
        await this.stateStore.save(state);
        this.scheduleReconcile(`conflict resolved: ${record.path}`, 0);
        return resolvedArtifactDirectory;
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

    private async moveConflictToResolved(record: ConflictRecord): Promise<string> {
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
        return target;
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

    private handleReconcileError(error: unknown): void | Promise<void> {
        const normalized = this.asError(error);
        const actionPath = this.currentActionPath;
        if (normalized instanceof StaleLocalSnapshotError
            || normalized instanceof ConcurrentRemoteResourceError) {
            this.currentActionPath = undefined;
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, 100);
            return;
        }
        if (this.isGlobalFailure(normalized, actionPath)) {
            this.currentActionPath = undefined;
            this.failProject(normalized);
            return;
        }
        if (this.isTransientFailure(normalized)) {
            this.currentActionPath = undefined;
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, 1000);
            return;
        }
        const filePath = actionPath ?? this.pathFromError(normalized);
        if (filePath && this.lastScan) {
            return this.isolatePathFailure(filePath, normalized, this.lastScan);
        }
        // An unscoped document/network failure must not stop unrelated paths.
        // Keep the queue live and require an explicit report for diagnosis.
        this.currentActionPath = undefined;
        this.output.appendLine(`[retry] unscoped safe-sync error: ${normalized.message}`);
        this.scheduleReconcile(normalized.message, 1000);
    }

    private async isolatePathFailure(
        filePath: string,
        error: Error,
        scan: ScanPair,
    ): Promise<void> {
        const state = this.requireState();
        try {
            await this.createConflict(
                filePath,
                `Document/path operation was isolated: ${error.message}`,
                scan,
            );
            if (state.pendingAction?.path === filePath) {
                state.pendingAction = undefined;
                await this.stateStore.save(state);
            }
        } catch (conflictError) {
            this.failProject(this.asError(conflictError));
        } finally {
            this.currentActionPath = undefined;
        }
    }

    private isGlobalFailure(error: Error, actionPath?: string): boolean {
        if (error instanceof ReplicaLockedError || /lock ownership was lost/i.test(error.message)) {
            return true;
        }
        return actionPath === undefined
            && (
                error instanceof WalCorruptionError
                || /WAL|checkpoint|binding/i.test(error.message)
            );
    }

    private isTransientFailure(error: Error): boolean {
        const code = (error as NodeJS.ErrnoException).code;
        return [
            'ECONNRESET',
            'ECONNREFUSED',
            'ETIMEDOUT',
            'EAI_AGAIN',
            'ENETUNREACH',
            'EPIPE',
        ].includes(code ?? '')
            || /timed out|network|socket|disconnected|temporarily unavailable|HTTP [45]\d\d/i.test(
                error.message,
            );
    }

    private pathFromError(error: Error): string | undefined {
        const match = error.message.match(/(?:for|at|: )([A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)*)/);
        const candidate = match?.[1];
        if (!candidate || candidate.startsWith('.')) {
            return undefined;
        }
        return this.lastScan?.local.has(candidate) || this.lastScan?.remote.has(candidate)
            ? candidate
            : undefined;
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
            await this.enqueueReconcile('manual retry');
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
                    await this.enqueueReconcile('manual synchronization');
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

function shellQuote(value: string): string {
    return `'${value.replace(/'/g, `'"'"'`)}'`;
}
