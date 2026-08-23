import { randomUUID } from 'crypto';
import { lstat, readFile, rename } from 'fs/promises';
import * as path from 'path';
import * as vscode from 'vscode';
import { BaseSCM, CommitItem, SettingItem } from '.';
import {
    ConcurrentRemoteResourceError,
    DocumentProjectionChange,
    DocumentRecoveryIsolationError,
    EMPTY_REMOTE_FOLDER_TREE_HASH,
    IsolatedDocumentRecovery,
    PendingDocumentRecovery,
    PreparedDocumentRecovery,
    REMOTE_SAFE_SYNC_DIRECTORY,
    RetainedRemoteCandidateError,
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
    AiCommitIntent,
    AiCommitReceipt,
    BaselineEntry,
    BaselineFileType,
    CompletedAiCommit,
    ConflictRecord,
    DetachedDocumentRecovery,
    PendingReplicaAction,
    ReplicaActionKind,
    ReplicaState,
    ReplicaStateStore,
    recordCompletedAiCommit,
} from '../sync/replicaStateStore';
import {
    contentHash,
    DEFAULT_IGNORE_PATTERNS,
    isEditableTextCandidate,
    isHardIgnored,
    normalizeRelativePath,
    readPastTruncationWindow,
    shouldIgnore,
} from '../sync/syncPolicy';
import { createLocalReplicaBinding, ensureLocalReplicaBinding } from '../sync/localBinding';
import {
    TextReplicaDiagnosticEvent,
    TextReplicaIsolation,
    TextReplicaStateMachine,
    UnattributedDiskSnapshotError,
} from '../sync/textReplicaStateMachine';
import {
    AiGatewayRequest,
    AiGatewayServer,
    sendAiGatewayRequest,
} from '../sync/aiGateway';
import {
    AiWriterLease,
    AiWriterLeaseManager,
} from '../sync/aiWriterLease';
import {
    DocumentSchedulerRetiredError,
    FairTaskScheduler,
} from '../sync/fairTaskScheduler';
import { WalCorruptionError } from '../sync/durableWal';
import { DocumentSessionRetiredError } from '../sync/durableDocumentSession';
import { TextOperation } from '../sync/textOperation';
import { GlobalStateManager } from '../utils/globalStateManager';
import {
    LocalProjectionCas,
    LocalProjectionRecoveryCandidate,
} from '../sync/localProjectionCas';
import {
    createDirectoryDurably,
    syncDirectoryDurably,
    writeFileDurably,
} from '../sync/durableFile';

const IGNORE_SETTING_KEY = 'ignore-patterns';
const TEXT_DEBOUNCE_MS = 350;
const PROJECTION_OBSERVATION_RETRY_MS = 25;
const BINARY_STABILITY_MS = 500;
const LOCAL_DELETE_STABILITY_MS = 500;
const REMOTE_DELETE_STABILITY_MS = 500;
const REMOTE_CAPACITY_RETRY_MS = 30000;
const MAX_RECONCILE_ACTIONS = 100;

type ReconcileStep = 'acted' | 'waiting' | 'none';

class StaleLocalSnapshotError extends Error {
    constructor(
        message: string,
        readonly actualContent?: Uint8Array,
        readonly endpoint?: 'editor' | 'disk',
        readonly diskProjectionBase?: Uint8Array,
    ) {
        super(message);
    }
}

class BaselineSelectionChangedError extends Error {}

class DetachedRecoveryPathAppearedError extends Error {}

class LocalProjectionConflictError extends Error {
    constructor(readonly filePath: string) {
        super(`A displaced local writer was retained for ${filePath}`);
    }
}

interface ScanPair {
    local: Map<string, ReplicaSnapshot>;
    remote: Map<string, ReplicaSnapshot>;
    /** Projection generation captured before either endpoint was observed. */
    projectionEpoch?: number;
    /** Per-path baseline heads captured before the endpoint scan began. */
    baselineAtStart?: Map<string, BaselineEntry>;
}

function sameBaselineEntry(
    left: BaselineEntry | undefined,
    right: BaselineEntry | undefined,
): boolean {
    return left === right || (
        left !== undefined
        && right !== undefined
        && left.entityId === right.entityId
        && left.fileType === right.fileType
        && left.hash === right.hash
        && left.text === right.text
        && left.treeHash === right.treeHash
        && left.localIdentity === right.localIdentity
    );
}

interface EditorObservationHead {
    document: vscode.TextDocument;
    version: number;
    content: string;
}

/** A reconcile that has not started yet, and everything asking for it. */
interface QueuedReconcile {
    reasons: Set<string>;
    done: Promise<void>;
}

interface PendingEditorObservation {
    content: string;
    observedAncestor?: string;
    editorDirty: boolean;
    projectionBoundary: boolean;
}

interface EditorReplacement {
    startOffset: number;
    endOffset: number;
    text: string;
}

export interface LocalReplicaRuntimeStatus {
    active: boolean;
    lockHeld: boolean;
    startupSettled: boolean;
    paused: boolean;
    initialized: boolean;
    pendingAction: boolean;
    conflictCount: number;
    textIsolationEvents: number;
    projectPauseEvents: number;
    settled: boolean;
    /** Names of the settle conditions still unsatisfied, empty when settled. */
    settledBlockers: string[];
    /** The reconcile currently running, when one is blocking settlement. */
    activeReconcile?: {
        reason: string;
        durationMs: number;
        /** Where inside the reconcile the time is going. */
        phase: string;
        iteration: number;
    };
    /** Reconciles started since activation; a churning loop is not a stall. */
    reconcileStarts: number;
    idle: boolean;
    activeDocumentTasks: Array<{
        path: string;
        source: string;
        durationMs: number;
        pendingCount: number;
    }>;
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
    private readonly localProjectionCas: LocalProjectionCas;
    private readonly output: vscode.OutputChannel;
    private readonly binaryStability = new Map<string, { hash: string; since: number }>();
    private localDeletionCandidates = new Map<string, {
        remoteEntityId: string;
        remoteHash: string;
        remoteTreeHash?: string;
        since: number;
    }>();
    /** When each currently absent local pathname was first observed missing. */
    private localAbsences?: Map<string, number>;
    /** When each currently absent remote pathname was first observed missing. */
    private remoteAbsences?: Map<string, number>;
    private readonly pendingReasons = new Set<string>();
    private state?: ReplicaState;
    private lock?: ReplicaLock;
    private vfsWatcher?: vscode.FileSystemWatcher;
    private localWatcher?: vscode.FileSystemWatcher;
    private lifecycleDisposables: vscode.Disposable[] = [];
    private reconcileTimer?: NodeJS.Timeout;
    private readonly reconcileQueue = new SerialTaskQueue();
    /** The reconcile that is waiting to start, which later requests join. */
    private queuedReconcile?: QueuedReconcile;
    private readonly textReplicas = new Map<string, TextReplicaStateMachine>();
    private readonly textSchedulers = new Map<string, FairTaskScheduler>();
    private readonly editorObservationHeads = new Map<string, EditorObservationHead>();
    private readonly pendingEditorObservations = new Map<string, PendingEditorObservation[]>();
    private readonly editorObservationDrains = new Map<string, Promise<void>>();
    private readonly textConflictTasks = new Map<string, Promise<void>>();
    private isolatedRevisionTasks = new Map<string, Promise<void>>();
    private textRetirements = new Map<string, Promise<void>>();
    private readonly resolvingPaths = new Set<string>();
    /** Which variant each in-flight resolution is converging on. */
    private resolvingResolutions = new Map<string, ConflictResolution>();
    private resolutionTypeOverrides = new Map<string, BaselineFileType>();
    private readonly aiLeases = new AiWriterLeaseManager();
    private readonly aiTransactions = new Map<string, {
        lease: AiWriterLease;
        path: string;
        baseContent: string;
        entityId: string;
        commit?: {
            targetContent: string;
            receipt: AiCommitReceipt;
        };
    }>();
    private aiGateway?: AiGatewayServer;
    private observationReady = false;
    private readonly recoveryObservations = new Set<string>();
    private pausedError?: Error;
    private disposed = false;
    private lockHeld = false;
    private startupSettled = false;
    private activeReconcileCount = 0;
    /** Reason, start time, and progress of the running reconcile, for traces. */
    private activeReconcile?: {
        reason: string;
        startedAt: number;
        phase: string;
        iteration: number;
    };
    private reconcileStarts = 0;
    /** Consecutive transient-failure retries, for the retry backoff. */
    private transientRetryStreak = 0;
    private textIsolationEvents = 0;
    private projectPauseEvents = 0;
    private handoffInProgress = false;
    private disposeRequestedDuringHandoff = false;
    private handoffMutationObserved = false;
    private handoffPromise?: Promise<unknown>;
    private readonly activeAiGatewayRequests = new Set<Promise<unknown>>();
    private retirementPromise?: Promise<void>;
    private lockOwnershipLost = false;
    private readonly handoffDeferredEditorObservations = new Map<string, {
        content: string;
        observedAncestor?: string;
        editorDirty: boolean;
    }>();
    private readonly pendingEditorSaveBoundaries = new Map<string, Promise<void>>();
    private readonly deferredLocalFileObservations = new Map<string, {
        uri: vscode.Uri;
        kind: string;
    }>();
    private readonly deferredLocalFileBoundaryWaits = new Map<string, Promise<void>>();
    private lastScan?: ScanPair;
    private lastReport = 'Safe sync has not scanned this replica yet.';
    private currentActionPath?: string;

    public static getRuntimeStatus(basePath: string): LocalReplicaRuntimeStatus | undefined {
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            return undefined;
        }
        const pendingAction = instance.state?.pendingAction !== undefined;
        const conflictCount = Object.keys(instance.state?.conflicts ?? {}).length
            + Object.keys(instance.state?.detachedDocumentRecoveries ?? {}).length;
        const active = !instance.disposed;
        const activeDocumentTasks = [...instance.textSchedulers.entries()]
            .flatMap(([filePath, scheduler]) => {
                const task = scheduler.activeTask;
                return task === undefined ? [] : [{
                    path: filePath,
                    source: task.source,
                    durationMs: task.durationMs,
                    pendingCount: scheduler.pendingCount,
                }];
            });
        // Naming each condition keeps "not settled yet" diagnosable: an
        // operator, and the release gate, can see which endpoint is behind
        // instead of only that convergence has not been announced.
        const settledConditions: Array<[string, boolean]> = [
            ['active', active],
            ['lock', instance.lockHeld],
            ['observation-ready', instance.observationReady],
            ['handoff', !instance.handoffInProgress],
            ['startup', instance.startupSettled],
            ['paused', instance.pausedError === undefined],
            ['reconcile-active', instance.activeReconcileCount === 0],
            ['reconcile-queue', instance.reconcileQueue.isIdle],
            ['reconcile-timer', instance.reconcileTimer === undefined],
            ['pending-reasons', instance.pendingReasons.size === 0],
            ['ai-requests', instance.activeAiGatewayRequests.size === 0],
            ['ai-transactions', instance.aiTransactions.size === 0],
            ['editor-observations', instance.pendingEditorObservations.size === 0],
            ['observation-drains', instance.editorObservationDrains.size === 0],
            ['text-conflicts', instance.textConflictTasks.size === 0],
            ['isolated-revisions', instance.isolatedRevisionTasks.size === 0],
            ['text-retirements', instance.textRetirements.size === 0],
            ['connection', instance.vfs.isSafeSyncConnectionReady],
            ['document-operations', !instance.vfs.hasPendingDocumentOperations],
            [
                'schedulers',
                [...instance.textSchedulers.values()].every(scheduler => scheduler.isIdle),
            ],
            [
                'replicas',
                [...instance.textReplicas.values()].every(machine => machine.isSettled),
            ],
        ];
        const settledBlockers = settledConditions
            .filter(([, satisfied]) => !satisfied)
            .map(([name]) => name);
        const settled = settledBlockers.length === 0;
        return {
            active,
            lockHeld: instance.lockHeld,
            startupSettled: instance.startupSettled,
            paused: instance.pausedError !== undefined,
            initialized: instance.state?.initialized === true,
            pendingAction,
            conflictCount,
            textIsolationEvents: instance.textIsolationEvents,
            projectPauseEvents: instance.projectPauseEvents,
            settled,
            settledBlockers,
            activeReconcile: instance.activeReconcile === undefined ? undefined : {
                reason: instance.activeReconcile.reason,
                durationMs: Date.now() - instance.activeReconcile.startedAt,
                phase: instance.activeReconcile.phase,
                iteration: instance.activeReconcile.iteration,
            },
            reconcileStarts: instance.reconcileStarts,
            idle: settled
                && instance.status.status === 'idle'
                && !pendingAction
                && conflictCount === 0,
            activeDocumentTasks,
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

    public static reclaimE2ERetainedRemoteTransactions(
        basePath: string,
        expectedProjectId: string,
    ): Promise<boolean> {
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            return Promise.reject(
                new Error(`No active Local Replica runtime for ${path.resolve(basePath)}`),
            );
        }
        if (
            instance.vfs.projectId !== expectedProjectId
            || instance.vfs.projectName !== 'hershel-safe-sync-e2e'
        ) {
            return Promise.reject(
                new Error('Refusing retained-transaction reclamation outside the approved E2E project'),
            );
        }
        return instance.reconcileQueue.run(async () => {
            instance.activeReconcileCount++;
            try {
                return await instance.vfs.reclaimRetainedRemoteTransactionsUnderPressure();
            } finally {
                instance.activeReconcileCount--;
            }
        });
    }

    public static resolveE2EConflictUsingRemote(
        basePath: string,
        filePath: string,
    ): Promise<string> {
        if (!/^hershel-e2e-[a-zA-Z0-9_-]+(?:\/.+)?$/.test(filePath)
            || filePath.split('/').some(part => part === '' || part === '.' || part === '..')) {
            throw new Error(`Refusing to resolve non-E2E conflict: ${filePath}`);
        }
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            throw new Error(`No active Local Replica runtime for ${path.resolve(basePath)}`);
        }
        return instance.enqueueConflictResolution(filePath, 'remote');
    }

    /** Test-only fault boundary for proving late text attachment. */
    public static detachE2ETextReplica(
        basePath: string,
        filePath: string,
    ): Promise<void> {
        if (!/^hershel-e2e-[a-zA-Z0-9_-]+\/.+$/.test(filePath)
            || filePath.split('/').some(part => part === '' || part === '.' || part === '..')) {
            throw new Error(`Refusing to detach non-E2E document: ${filePath}`);
        }
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            throw new Error(`No active Local Replica runtime for ${path.resolve(basePath)}`);
        }
        return instance.retireTextReplicasUnder(filePath);
    }

    public static suspendE2EDocumentTransport(
        basePath: string,
        filePath: string,
    ): Promise<void> {
        const instance = LocalReplicaSCMProvider.requireE2EInstance(basePath, filePath);
        return instance.vfs.suspendDocumentTransportForE2E(instance.remoteUri(filePath));
    }

    public static hasPendingE2EDocumentRecovery(
        basePath: string,
        filePath: string,
    ): Promise<boolean> {
        const instance = LocalReplicaSCMProvider.requireE2EInstance(basePath, filePath);
        return instance.vfs.hasPendingDocumentRecoveryForE2E(instance.remoteUri(filePath));
    }

    public static inspectE2EDocumentRecovery(
        basePath: string,
        filePath: string,
        operationId?: string,
    ): ReturnType<VirtualFileSystem['inspectDocumentRecoveryForE2E']> {
        const instance = LocalReplicaSCMProvider.requireE2EInstance(basePath, filePath);
        return instance.vfs.inspectDocumentRecoveryForE2E(
            instance.remoteUri(filePath),
            operationId,
        );
    }

    public static inspectE2ETextDiagnostics(
        basePath: string,
        filePath: string,
    ): TextReplicaDiagnosticEvent[] {
        if (!/^hershel-e2e-[a-zA-Z0-9_-]+\/.+$/.test(filePath)) {
            throw new Error(`Refusing to inspect non-E2E text diagnostics: ${filePath}`);
        }
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        return instance?.textReplicas.get(filePath)?.inspectDiagnostics() ?? [];
    }

    public static cleanupE2EDetachedRecovery(
        basePath: string,
        documentId: string,
    ): Promise<void> {
        if (!/^hershel-e2e-detached-[a-zA-Z0-9_-]+$/.test(documentId)) {
            throw new Error(`Refusing to clean non-E2E detached recovery: ${documentId}`);
        }
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            throw new Error(`No active Local Replica runtime for ${path.resolve(basePath)}`);
        }
        return instance.reconcileQueue.run(async () => {
            const state = instance.requireState();
            const record = state.detachedDocumentRecoveries?.[documentId];
            if (!record) {
                return;
            }
            if (!record.walRetired) {
                throw new Error(`Detached E2E WAL is not retired: ${documentId}`);
            }
            if (!record.artifactDirectory.startsWith(
                '.overleaf/conflicts/unresolved/detached-wal-',
            )) {
                throw new Error(`Detached E2E artifact path is invalid: ${documentId}`);
            }
            delete state.detachedDocumentRecoveries![documentId];
            await instance.stateStore.save(state);
            try {
                await vscode.workspace.fs.delete(
                    instance.localUri(record.artifactDirectory),
                    {recursive: true, useTrash: false},
                );
            } catch (error) {
                if (!instance.isFileNotFound(error)) {
                    throw error;
                }
            }
        });
    }

    private static requireE2EInstance(
        basePath: string,
        filePath: string,
    ): LocalReplicaSCMProvider {
        if (!/^hershel-e2e-[a-zA-Z0-9_-]+\/.+$/.test(filePath)
            || filePath.split('/').some(part => part === '' || part === '.' || part === '..')) {
            throw new Error(`Refusing E2E transport access outside its namespace: ${filePath}`);
        }
        const instance = LocalReplicaSCMProvider.activeInstances.get(path.resolve(basePath));
        if (!instance) {
            throw new Error(`No active Local Replica runtime for ${path.resolve(basePath)}`);
        }
        return instance;
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
        this.localProjectionCas = new LocalProjectionCas(baseUri.fsPath);
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
        await this.assertNoLocalReservedNamespace();
        await ensureLocalReplicaBinding(
            this.baseUri.fsPath,
            createLocalReplicaBinding(
                this.vfs.origin.toString(),
                this.vfs.serverName,
                this.vfs.projectName,
                GlobalStateManager.getServerUrl(this.context, this.vfs.serverName),
            ),
            this.vfs.projectId,
        );
        this.lock = await this.acquireReplicaLock();
        this.lockHeld = true;
        const recoveredProjectionCandidates = await this.localProjectionCas.recover();
        await this.vfs.init();
        try {
            this.state = await this.stateStore.load();
            await this.recoverDetachedRecoveryArtifactLocations();
            const migratedAiReceipts = await this.stateStore
                .normalizeCompletedAiCommits(this.state);
            if (migratedAiReceipts > 0) {
                this.output.appendLine(
                    `[recovery] migrated ${migratedAiReceipts} AI receipts to baseline storage`,
                );
            }
            const removedBaselines = await this.stateStore
                .garbageCollectBaselinesAtStartup(this.state);
            if (removedBaselines > 0) {
                this.output.appendLine(
                    `[recovery] removed ${removedBaselines} unreferenced baseline snapshots`,
                );
            }
            // Recovery before observation: verify the private remote namespace
            // before watchers or the first semantic scan can hide its paths.
            // An initialized legacy replica may explicitly adopt only the
            // exact history/trash layout created by pre-marker safe-sync.
            await this.vfs.ensureSafeSyncRemoteNamespace(
                this.state.initialized && this.vfs.safeSyncCapability.writable
                    ? this.state.replicaId
                    : undefined,
            );
            await this.registerOrphanDocumentRecoveries(
                await this.vfs.inspectPendingDocumentRecoveries(),
            );
            if (this.state.aiLeaseState) {
                const recovered = this.state.aiLeaseState;
                this.aiLeases.restore({
                    nextFencingToken: recovered.nextFencingToken,
                    leases: [],
                });
                this.state.aiLeaseState = this.aiLeases.serialize();
                await this.stateStore.save(this.state);
            }
            // A commit was already authorized before its intent became
            // durable. Finish every such fixed-ID transition before exposing
            // filesystem observations or accepting a new AI lease.
            await this.recoverPendingAiCommitIntents();
            await this.registerProjectionRecoveryConflicts(recoveredProjectionCandidates);
        } catch (error) {
            const normalized = this.asError(error);
            this.failProject(normalized);
            throw normalized;
        }

        // Publish the handoff endpoint before the potentially lengthy initial
        // scan/recovery. A second VS Code window must never see a live lock
        // with no way to request an orderly transfer.
        await this.startAiGateway();
        if (!this.aiGateway) {
            throw new Error('Safe-sync ownership gateway did not start');
        }

        this.vfsWatcher = vscode.workspace.createFileSystemWatcher(
            new vscode.RelativePattern(this.vfs.origin, '**/*'),
        );
        this.localWatcher = vscode.workspace.createFileSystemWatcher(
            new vscode.RelativePattern(this.baseUri.fsPath, '**/*'),
        );
        this.editorObservationHeads.clear();
        for (const document of vscode.workspace.textDocuments) {
            this.rememberEditorObservationHead(document);
        }
        const listeners: vscode.Disposable[] = [
            this.vfs.onDidChangeDocumentProjection(
                change => this.onRemoteDocumentProjection(change),
            ),
            this.vfsWatcher.onDidChange(uri => this.onRemoteFileEvent(uri, 'change')),
            this.vfsWatcher.onDidCreate(uri => this.onRemoteFileEvent(uri, 'create')),
            this.vfsWatcher.onDidDelete(uri => this.onRemoteFileEvent(uri, 'delete')),
            this.localWatcher.onDidChange(uri => this.onLocalFileEvent(uri, 'change')),
            this.localWatcher.onDidCreate(uri => this.onLocalFileEvent(uri, 'create')),
            this.localWatcher.onDidDelete(uri => this.onLocalFileEvent(uri, 'delete')),
            vscode.workspace.onDidOpenTextDocument(document => {
                this.rememberEditorObservationHead(document);
            }),
            vscode.workspace.onDidCloseTextDocument(document => {
                const relative = this.relativeLocalPath(document.uri);
                if (
                    relative !== undefined
                    && this.editorObservationHeads.get(relative)?.document === document
                ) {
                    this.editorObservationHeads.delete(relative);
                }
            }),
            vscode.workspace.onDidChangeTextDocument(event => {
                const relative = this.relativeLocalPath(event.document.uri);
                if (relative !== undefined && !this.isIgnored(relative)) {
                    if (this.handoffInProgress) {
                        this.handoffMutationObserved = true;
                        const observation = this.captureEditorObservation(relative, event.document);
                        const deferred = this.handoffDeferredEditorObservations.get(relative);
                        this.handoffDeferredEditorObservations.set(relative, {
                            content: observation.content,
                            observedAncestor: deferred !== undefined
                                ? deferred.observedAncestor
                                : observation.observedAncestor,
                            editorDirty: observation.editorDirty,
                        });
                        return;
                    }
                    const {content, observedAncestor, editorDirty} = this.captureEditorObservation(
                        relative,
                        event.document,
                    );
                    if (!this.observationReady) {
                        this.recoveryObservations.add(`editor:${relative}`);
                    } else {
                        // Freeze both sides of the event-time transition. A
                        // remote task may run before this queued observation,
                        // so reading the machine's later editor head as its
                        // ancestor would invent a destructive snapshot diff.
                        this.admitEditorObservation(
                            relative,
                            content,
                            observedAncestor,
                            editorDirty,
                        );
                    }
                }
            }),
            vscode.workspace.onWillSaveTextDocument(event => {
                const relative = this.relativeLocalPath(event.document.uri);
                const machine = relative === undefined ? undefined : this.textReplicas.get(relative);
                if (this.observationReady && relative !== undefined && machine && !this.isIgnored(relative)) {
                    const savingContent = event.document.getText();
                    machine.noteSaveReason(event.reason);
                    // A user save is an external filesystem mutation, not a
                    // plugin projection. Publish a causal fence immediately;
                    // watcher invalidations coalesce behind it until the frozen
                    // save snapshot crosses the journal boundary. The save
                    // remains its own fair source so a burst of watcher echoes
                    // cannot starve remote or editor tasks.
                    const boundary = this.textTask(
                        relative,
                        'editor-save',
                        () => machine.beforeEditorSave(savingContent),
                    );
                    this.trackEditorSaveBoundary(relative, boundary);
                    void boundary.catch(error => {
                        if (this.textReplicas.get(relative) === machine) {
                            this.handleTextReplicaError(relative, error);
                        }
                    });
                }
            }),
            vscode.workspace.onDidSaveTextDocument(document => {
                const relative = this.relativeLocalPath(document.uri);
                const machine = relative === undefined ? undefined : this.textReplicas.get(relative);
                if (!this.observationReady && relative !== undefined) {
                    this.recoveryObservations.add(`editor:${relative}`);
                    this.recoveryObservations.add(`local:${relative}`);
                } else if (relative !== undefined && machine && !this.isIgnored(relative)) {
                    const savedEditorContent = document.getText();
                    const editorDirtyAfterSave = document.isDirty;
                    void this.textTask(
                        relative,
                        'editor-save',
                        () => machine.onEditorSaved(
                            savedEditorContent,
                            editorDirtyAfterSave,
                        ),
                    ).catch(error => {
                        if (this.textReplicas.get(relative) === machine) {
                            this.handleTextReplicaError(relative, error);
                        }
                    });
                }
            }),
        ];
        this.lifecycleDisposables = [this.vfsWatcher, this.localWatcher, ...listeners];

        LocalReplicaSCMProvider.activeInstances.set(path.resolve(this.baseUri.fsPath), this);

        try {
            if (!this.pausedError) {
                await this.enqueueReconcile('startup');
                if (!this.pausedError) {
                    this.observationReady = true;
                    await this.replayRecoveryObservations();
                }
            }
        } finally {
            this.startupSettled = true;
        }

        return [new vscode.Disposable(() => this.dispose())];
    }

    public dispose(): void {
        if (this.handoffInProgress) {
            this.disposeRequestedDuringHandoff = true;
            return;
        }
        if (this.disposed || this.retirementPromise) {
            return;
        }
        this.disposed = true;
        this.observationReady = false;
        if (this.reconcileTimer) {
            clearTimeout(this.reconcileTimer);
            this.reconcileTimer = undefined;
        }
        this.pendingReasons.clear();
        const lock = this.lock;
        this.retirementPromise = (async () => {
            let retiredCleanly = true;
            const attempt = async (label: string, task: () => Promise<void>): Promise<void> => {
                try {
                    await task();
                } catch (error) {
                    retiredCleanly = false;
                    console.error(`Failed to retire safe-sync ${label}`, error);
                }
            };
            try {
                const listenerError = this.disposeLifecycleObservation();
                if (listenerError) {
                    retiredCleanly = false;
                    console.error('Failed to retire safe-sync lifecycle listeners', listenerError);
                }
                // Existing resource reconciliation can hold a durable pending
                // action across remote I/O. It must finish before any WAL,
                // socket, or writer lease is closed.
                await attempt('AI gateway requests', () => this.drainActiveAiGatewayRequests());
                await attempt('reconcile queue', () => this.reconcileQueue.run(async () => {}));
                await attempt('editor observations', () => this.drainQueuedEditorObservations());
                await attempt('retiring text owners', () => this.drainTextRetirements());
                // An already-running reconcile may attach a document owner
                // before reaching the barrier. Snapshot owners only after it
                // drains so every newly attached machine is retired as well.
                const machines = [...this.textReplicas.entries()];
                const schedulers = [...this.textSchedulers.values()];
                if (!this.lockOwnershipLost) {
                    for (const [filePath, machine] of machines) {
                        await attempt(`editor ${filePath}`, () => (
                            this.textTask(filePath, 'retire', () => machine.prepareForHandoff())
                        ));
                    }
                }
                await attempt('document schedulers', async () => {
                    for (const scheduler of schedulers) {
                        await scheduler.disposeAndWait();
                    }
                });
                await attempt('text state machines', async () => {
                    for (const [, machine] of machines) {
                        await machine.disposeAndWait();
                    }
                });
                await attempt('late text retirements', () => this.drainTextRetirements());
                await attempt('text conflict artifacts', () => this.drainTextConflictTasks());
                await attempt('isolated text revisions', () => this.drainIsolatedRevisionTasks());
                await attempt('gateway', () => this.stopAiGateway());
                await attempt('remote sessions', () => this.vfs.retireForReplicaHandoff());
                await attempt('state WAL', () => this.stateStore.close());
            } finally {
                try {
                    if (retiredCleanly) {
                        await lock?.release();
                    } else {
                        // Keep renewing the old lease while this process is
                        // alive. A successor must not enter after TTL when an
                        // old writer could still own a socket or WAL handle.
                        this.output.appendLine(
                            '[retire] ownership retained because shutdown was incomplete',
                        );
                    }
                } catch (error) {
                    lock?.abandon();
                    console.error('Failed to release safe-sync replica lock', error);
                }
                this.lockHeld = !retiredCleanly;
                if (retiredCleanly) {
                    this.lock = undefined;
                }
                this.textReplicas.clear();
                this.textSchedulers.clear();
                this.editorObservationHeads.clear();
                this.pendingEditorObservations?.clear();
                this.editorObservationDrains?.clear();
                this.unregisterRuntime();
                this.output.dispose();
            }
        })();
    }

    /** Stop every event source, even when one Disposable is faulty. */
    private disposeLifecycleObservation(): Error | undefined {
        let firstError: Error | undefined;
        for (const disposable of this.lifecycleDisposables.splice(0)) {
            try {
                disposable.dispose();
            } catch (error) {
                firstError ??= this.asError(error);
            }
        }
        return firstError;
    }

    private async drainActiveAiGatewayRequests(): Promise<void> {
        while (this.activeAiGatewayRequests.size > 0) {
            await Promise.allSettled([...this.activeAiGatewayRequests]);
        }
    }

    private async drainTextConflictTasks(): Promise<void> {
        while (this.textConflictTasks.size > 0) {
            const results = await Promise.allSettled([...this.textConflictTasks.values()]);
            const failure = results.find(
                (result): result is PromiseRejectedResult => result.status === 'rejected',
            );
            if (failure) {
                throw this.asError(failure.reason);
            }
        }
    }

    private async drainTextRetirements(): Promise<void> {
        this.textRetirements ??= new Map<string, Promise<void>>();
        while (this.textRetirements.size > 0) {
            const results = await Promise.allSettled([...this.textRetirements.values()]);
            const failure = results.find(
                (result): result is PromiseRejectedResult => result.status === 'rejected',
            );
            if (failure) {
                throw this.asError(failure.reason);
            }
        }
    }

    private async drainIsolatedRevisionTasks(filePath?: string): Promise<void> {
        this.isolatedRevisionTasks ??= new Map<string, Promise<void>>();
        while (true) {
            const tasks = [...this.isolatedRevisionTasks.entries()]
                .filter(([candidate]) => filePath === undefined
                    || candidate === filePath
                    || candidate.startsWith(`${filePath}/`))
                .map(([, task]) => task);
            if (tasks.length === 0) {
                return;
            }
            const results = await Promise.allSettled(tasks);
            const failure = results.find(
                (result): result is PromiseRejectedResult => result.status === 'rejected',
            );
            if (failure) {
                throw this.asError(failure.reason);
            }
        }
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
        const saveBoundary = this.pendingEditorSaveBoundaries?.get(relative);
        if (saveBoundary) {
            this.deferLocalFileObservation(relative, uri, kind, saveBoundary);
            return;
        }
        const projectionEpochAtEvent = this.localProjectionCas?.observationEpoch;
        const projectionActiveAtEvent = this.localProjectionCas?.hasActiveTransition ?? false;
        const machine = this.textReplicas.get(relative);
        if (machine) {
            if (machine.isIsolated) {
                void this.readLocalText(relative).then(content => (
                    content === undefined
                        ? undefined
                        : this.persistIsolatedTextRevision(relative, content, 'disk')
                )).catch(error => this.failProject(this.asError(error)));
                return;
            }
            // Read inside the document queue. Reading before enqueueing lets a
            // concurrent remote projection overwrite the file first, causing
            // the watcher to observe the plugin's bytes instead of the user's
            // write. A delete is also only an invalidation hint: the atomic
            // disk CAS temporarily displaces the pathname before installing
            // the new projection.
            void this.textTask(relative, 'filesystem', async () => {
                if (this.textReplicas.get(relative) !== machine) {
                    return;
                }
                const content = await this.readSettledLocalText(relative, machine);
                if (this.textReplicas.get(relative) !== machine) {
                    return;
                }
                if (content === undefined) {
                    const projectionCrossedObservation = projectionActiveAtEvent
                        || this.localProjectionCas?.hasActiveTransition === true
                        || (
                            projectionEpochAtEvent !== undefined
                            && projectionEpochAtEvent !== this.localProjectionCas?.observationEpoch
                        );
                    if (projectionCrossedObservation) {
                        // Projection is not mutation. The local CAS atomically
                        // displaces the pathname while preserving its inode, so
                        // a watcher can legitimately read ENOENT inside that
                        // window. Re-observe after the epoch settles; a genuine
                        // delete will still be absent on the stable retry.
                        this.retryLocalFileObservation(uri, kind);
                        return;
                    }
                    // Watcher events are invalidation hints, not immutable
                    // snapshots. A queued change/create event can be read only
                    // after a later delete has landed. Reclassify the observed
                    // absence as a resource delete instead of isolating the
                    // document for an imaginary text ancestry failure.
                    const retirement = this.dropTextReplica(
                        relative,
                        'resource-change',
                        'The local document path disappeared while its text actor was active',
                    );
                    void retirement.then(() => this.scheduleReconcile(
                        `local delete observed after ${kind}: ${relative}`,
                        0,
                    )).catch(retirementError => this.failProject(
                        this.asError(retirementError),
                    ));
                    return;
                }
                await machine.onDiskObserved(content);
            }).catch(error => {
                if (this.textReplicas.get(relative) === machine) {
                    this.handleTextReplicaError(relative, error);
                }
            });
            return;
        }
        this.scheduleReconcile(`local ${kind}: ${relative}`, TEXT_DEBOUNCE_MS);
    }

    private trackEditorSaveBoundary(filePath: string, boundary: Promise<void>): void {
        this.pendingEditorSaveBoundaries.set(filePath, boundary);
        const settled = (): void => {
            if (this.pendingEditorSaveBoundaries.get(filePath) === boundary) {
                this.pendingEditorSaveBoundaries.delete(filePath);
            }
        };
        void boundary.then(settled, settled);
    }

    private deferLocalFileObservation(
        filePath: string,
        uri: vscode.Uri,
        kind: string,
        boundary: Promise<void>,
    ): void {
        // Watcher notifications are invalidation hints. While one frozen save
        // is becoming durable, only the latest hint matters because the drain
        // reads the current filesystem snapshot after the fence.
        this.deferredLocalFileObservations.set(filePath, {uri, kind});
        if (this.deferredLocalFileBoundaryWaits.get(filePath) === boundary) {
            return;
        }
        this.deferredLocalFileBoundaryWaits.set(filePath, boundary);
        const release = (): void => {
            if (this.deferredLocalFileBoundaryWaits.get(filePath) !== boundary) {
                return;
            }
            this.deferredLocalFileBoundaryWaits.delete(filePath);
            const deferred = this.deferredLocalFileObservations.get(filePath);
            this.deferredLocalFileObservations.delete(filePath);
            if (deferred && !this.disposed) {
                this.onLocalFileEvent(deferred.uri, deferred.kind);
            }
        };
        void boundary.then(release, release);
    }

    /** Read a local document without believing a rewrite-in-place window. */
    private readSettledLocalText(
        relative: string,
        machine: TextReplicaStateMachine,
    ): Promise<string | undefined> {
        return readPastTruncationWindow({
            read: () => this.readLocalText(relative),
            previousContent: machine.diskSnapshot,
            isCurrent: () => this.textReplicas.get(relative) === machine,
            wait: milliseconds => new Promise<void>(resolve => {
                setTimeout(resolve, milliseconds).unref();
            }),
        });
    }

    private retryLocalFileObservation(uri: vscode.Uri, kind: string): void {
        const timer = setTimeout(() => {
            if (!this.disposed) {
                this.onLocalFileEvent(uri, `${kind} after projection`);
            }
        }, PROJECTION_OBSERVATION_RETRY_MS);
        timer.unref();
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
            if (
                kind === 'change'
                && this.vfs.hasLiveDocumentProjection(machine.remoteEntityId)
            ) {
                // VFS publishes every journal-visible OT revision directly.
                // A FileSystemWatcher may merge several Changed events into
                // one later snapshot; replaying that snapshot in this queue
                // could overtake and then regress the ordered projections.
                return;
            }
            void this.textTask(relative, 'remote', async () => {
                if (this.textReplicas.get(relative) !== machine) {
                    return;
                }
                let content: string;
                try {
                    content = await this.readRemoteText(relative, machine.remoteEntityId);
                } catch (error) {
                    if (!this.isFileNotFound(error)) {
                        throw error;
                    }
                    if (this.textReplicas.get(relative) === machine) {
                        const retirement = this.dropTextReplica(
                            relative,
                            'resource-change',
                            'The remote document identity disappeared while local input was active',
                        );
                        void retirement.then(() => this.scheduleReconcile(
                            `remote delete observed after ${kind}: ${relative}`,
                            0,
                        )).catch(retirementError => this.failProject(
                            this.asError(retirementError),
                        ));
                    }
                    return;
                }
                if (this.textReplicas.get(relative) !== machine) {
                    return;
                }
                await machine.onCanonicalChanged(content);
            }).catch(error => {
                if (this.textReplicas.get(relative) === machine) {
                    this.handleTextReplicaError(relative, error);
                }
            });
            return;
        }
        this.scheduleReconcile(`remote ${kind}: ${relative}`, 100);
    }

    private onRemoteDocumentProjection(change: DocumentProjectionChange): void {
        const relative = normalizeRelativePath(change.path);
        if (relative === '' || this.isIgnored(relative)) {
            return;
        }
        if (!this.observationReady) {
            this.recoveryObservations.add(`remote:${relative}`);
            return;
        }
        const machine = this.textReplicas.get(relative);
        if (!machine || machine.remoteEntityId !== change.documentId) {
            this.scheduleReconcile(
                `remote document projection ${change.revision}: ${relative}`,
                100,
            );
            return;
        }
        void this.textTask(relative, 'remote-projection', async () => {
            if (
                this.textReplicas.get(relative) !== machine
                || machine.remoteEntityId !== change.documentId
            ) {
                return;
            }
            await machine.onCanonicalChanged(
                change.content,
                change.revision,
                change.sequence,
            );
        }).catch(error => {
            if (this.textReplicas.get(relative) === machine) {
                this.handleTextReplicaError(relative, error);
            }
        });
    }

    private rememberEditorObservationHead(document: vscode.TextDocument): void {
        const relative = this.relativeLocalPath(document.uri);
        if (relative === undefined || this.isIgnored(relative)) {
            return;
        }
        this.captureEditorObservation(relative, document);
    }

    /** Freeze the aggregate transition delivered for this document instance. */
    private captureEditorObservation(
        filePath: string,
        document: vscode.TextDocument,
    ): {content: string; observedAncestor?: string; editorDirty: boolean} {
        const content = document.getText();
        const previous = this.editorObservationHeads.get(filePath);
        // The last content observed from the same TextDocument object is the
        // causal ancestor of the next notification. VS Code versions are
        // diagnostic metadata, not a causal clock: external reloads can keep,
        // reset, or coalesce them. Object identity still prevents a closed
        // document's head from being reused after the path is reopened.
        const observedAncestor = previous?.document === document
            ? previous.content
            : undefined;
        this.editorObservationHeads.set(filePath, {
            document,
            version: document.version,
            content,
        });
        return {content, observedAncestor, editorDirty: document.isDirty};
    }

    private async onLocalTextDocumentChange(
        filePath: string,
        content: string,
        observedAncestor?: string,
        requireObservedAncestor = false,
        editorDirty = true,
    ): Promise<void> {
        if (!this.observationReady) {
            this.recoveryObservations.add(`editor:${filePath}`);
            return;
        }
        const machine = this.textReplicas?.get(filePath);
        if (!machine) {
            if (
                this.state?.conflicts[filePath]
                && this.state.entries[filePath]?.fileType === 'doc'
            ) {
                await this.persistIsolatedTextRevision(filePath, content, 'editor');
                return;
            }
            this.scheduleReconcile(`text edit before replica attach: ${filePath}`, TEXT_DEBOUNCE_MS);
            return;
        }
        if (machine.isIsolated) {
            await this.persistIsolatedTextRevision(filePath, content, 'editor');
            return;
        }
        try {
            await this.textTask(
                filePath,
                'editor',
                () => machine.onEditorChanged(
                    content,
                    observedAncestor,
                    requireObservedAncestor,
                    editorDirty,
                ),
            );
            this.status = { status: 'push', message: `Journalizing text: ${filePath}` };
        } catch (error) {
            if (this.textReplicas.get(filePath) === machine) {
                this.handleTextReplicaError(filePath, error);
            }
        }
    }

    private admitEditorObservation(
        filePath: string,
        content: string,
        observedAncestor: string | undefined,
        editorDirty: boolean,
    ): void {
        const earlierObservationPending = this.editorObservationDrains.has(filePath)
            || (this.pendingEditorObservations.get(filePath)?.length ?? 0) > 0;
        const machine = this.textReplicas.get(filePath);
        if (
            !earlierObservationPending
            && machine?.tryBufferEditorChange(content, observedAncestor, editorDirty)
        ) {
            this.status = {
                status: 'push',
                message: `Buffering text: ${filePath}`,
            };
            return;
        }
        // Once one event is queued, every later snapshot stays behind it. A
        // synchronous fast-path here would invert projection/user event order
        // and replay already-canonical bytes as a second local operation.
        this.enqueueLocalTextDocumentChange(
            filePath,
            content,
            observedAncestor,
            editorDirty,
        );
    }

    /**
     * Coalesce only contiguous human snapshots before they enter the document
     * scheduler. Plugin projection snapshots remain explicit boundaries so a
     * following keystroke cannot be mistaken for a replay of remote content.
     */
    private enqueueLocalTextDocumentChange(
        filePath: string,
        content: string,
        observedAncestor: string | undefined,
        editorDirty: boolean,
    ): void {
        const machine = this.textReplicas.get(filePath);
        const projectionBoundary = machine?.isExpectedEditorProjection(
            content,
            observedAncestor,
        ) ?? false;
        const pending = this.pendingEditorObservations.get(filePath) ?? [];
        const last = pending.at(-1);
        if (
            last
            && !last.projectionBoundary
            && !projectionBoundary
            && last.content === observedAncestor
        ) {
            last.content = content;
            last.editorDirty = editorDirty;
        } else {
            pending.push({
                content,
                observedAncestor,
                editorDirty,
                projectionBoundary,
            });
        }
        this.pendingEditorObservations.set(filePath, pending);
        this.scheduleEditorObservationDrain(filePath);
    }

    private scheduleEditorObservationDrain(filePath: string): void {
        if (this.editorObservationDrains.has(filePath)) {
            return;
        }
        const drain = Promise.resolve().then(async () => {
            const pending = this.pendingEditorObservations.get(filePath);
            const observation = pending?.shift();
            if (!observation) {
                this.pendingEditorObservations.delete(filePath);
                return;
            }
            if (!pending || pending.length === 0) {
                this.pendingEditorObservations.delete(filePath);
            }
            await this.onLocalTextDocumentChange(
                filePath,
                observation.content,
                observation.observedAncestor,
                true,
                observation.editorDirty,
            );
        }).catch(error => {
            this.handleTextReplicaError(filePath, error);
        }).finally(() => {
            if (this.editorObservationDrains.get(filePath) === drain) {
                this.editorObservationDrains.delete(filePath);
            }
            if ((this.pendingEditorObservations.get(filePath)?.length ?? 0) > 0) {
                this.scheduleEditorObservationDrain(filePath);
                return;
            }
            this.resumeDeferredProjection(filePath);
        });
        this.editorObservationDrains.set(filePath, drain);
    }

    /**
     * A remote projection deferred while editor observations were queued has
     * no other trigger: those observations carry no buffered input, so no idle
     * commit timer fires. Re-enter it once the queue for this document is empty.
     */
    private resumeDeferredProjection(filePath: string): void {
        const machine = this.textReplicas.get(filePath);
        if (!machine || machine.isSettled) {
            return;
        }
        void this.textTask(
            filePath,
            'projection-resume',
            () => machine.resumeProjection(),
        ).catch(error => {
            if (this.textReplicas.get(filePath) === machine) {
                this.handleTextReplicaError(filePath, error);
            }
        });
    }

    private async drainQueuedEditorObservations(): Promise<void> {
        // Some recovery-focused tests construct a provider prototype with no
        // live event ingress. Production instances always initialize both maps.
        if (!this.editorObservationDrains || !this.pendingEditorObservations) {
            return;
        }
        while (
            this.editorObservationDrains.size > 0
            || this.pendingEditorObservations.size > 0
        ) {
            for (const filePath of this.pendingEditorObservations.keys()) {
                this.scheduleEditorObservationDrain(filePath);
            }
            await Promise.all([...this.editorObservationDrains.values()]);
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
                    await this.onLocalTextDocumentChange(
                        filePath,
                        document.getText(),
                        undefined,
                        false,
                        document.isDirty,
                    );
                }
            } else if (source === 'local') {
                this.onLocalFileEvent(this.localUri(filePath), 'recovery replay');
            } else if (source === 'remote') {
                this.onRemoteFileEvent(this.remoteUri(filePath), 'recovery replay');
            }
        }
    }

    /**
     * Ensure OT ownership for the supplied scan. A true result means an older
     * owner retired, so the caller must scan again before attaching a successor.
     */
    private async ensureTextReplicas(scan: ScanPair): Promise<boolean> {
        this.textRetirements ??= new Map<string, Promise<void>>();
        if (
            scan.projectionEpoch !== undefined
            && (
                scan.projectionEpoch !== this.localProjectionCas.observationEpoch
                || this.localProjectionCas.hasActiveTransition
            )
        ) {
            // The local scan crossed a plugin-originated atomic displacement.
            // It has no user-filesystem semantics and must be discarded as a
            // whole before a missing path can retire an OT owner.
            return true;
        }
        if (this.textRetirements.size > 0) {
            await this.drainTextRetirements();
            return true;
        }
        // Path-level deletes and type changes belong to the resource planner.
        // Drop a stale OT machine before it can recreate a deleted path.
        let retiredOwner = false;
        for (const [filePath, machine] of [...this.textReplicas]) {
            const local = scan.local.get(filePath);
            const remote = scan.remote.get(filePath);
            const baseline = this.state?.entries[filePath];
            const ownedEntityId = machine.remoteEntityId ?? baseline?.entityId;
            const movedRemote = ownedEntityId === undefined
                ? undefined
                : [...scan.remote.values()].find(candidate => (
                    candidate.path !== filePath
                    && candidate.fileType === 'doc'
                    && candidate.entityId === ownedEntityId
                ));
            if (
                local?.fileType !== 'doc'
                || remote?.fileType !== 'doc'
                || isPathConflicted(filePath, this.requireState())
                || (
                    (machine.remoteEntityId ?? baseline?.entityId) !== undefined
                    && (machine.remoteEntityId ?? baseline?.entityId) !== remote?.entityId
                )
            ) {
                await this.dropTextReplica(
                    filePath,
                    movedRemote ? 'handoff' : 'resource-change',
                    'The document path or immutable Overleaf entity changed while local input was active',
                );
                retiredOwner = true;
            }
        }
        if (retiredOwner) {
            return true;
        }
        const baselinePathByEntity = new Map(
            Object.entries(this.requireState().entries)
                .map(([entryPath, entry]) => [entry.entityId, entryPath]),
        );
        for (const [filePath, remote] of scan.remote) {
            if (
                remote.fileType !== 'doc'
                || remote.text === undefined
                || isPathConflicted(filePath, this.requireState())
                || this.isPathResolving(filePath)
            ) {
                continue;
            }
            const retirement = this.textRetirements.get(filePath);
            if (retirement) {
                await retirement;
                return true;
            }
            const local = scan.local.get(filePath);
            const baseline = this.state?.entries[filePath];
            const priorEntityPath = remote.entityId === undefined
                ? undefined
                : baselinePathByEntity.get(remote.entityId);
            if (priorEntityPath !== undefined && priorEntityPath !== filePath) {
                // The immutable entity moved. Let findReplicaMove relocate the
                // existing local path before a text actor can project a second
                // copy at the destination.
                continue;
            }
            if (baseline !== undefined && baseline.entityId !== remote.entityId) {
                // A replacement entity is a resource decision, not an OT
                // continuation of the old document identity.
                continue;
            }
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
                const existing = this.textReplicas.get(filePath);
                if (existing) {
                    await this.textTask(filePath, 'remote-scan', async () => {
                        if (
                            this.isPathResolving(filePath)
                            || this.textReplicas.get(filePath) !== existing
                        ) {
                            return;
                        }
                        if (this.vfs.hasLiveDocumentProjection(existing.remoteEntityId)) {
                            // The live session publishes every canonical OT
                            // transition in revision order. A scan is only a
                            // recovery trigger; reading a later aggregate here
                            // would create a second, reorderable text source.
                            return;
                        }
                        const remoteUri = this.remoteUri(filePath);
                        const projection = await this.vfs.readDocumentProjection(
                            remoteUri,
                            existing.remoteEntityId,
                        );
                        if (
                            this.isPathResolving(filePath)
                            || this.textReplicas.get(filePath) !== existing
                        ) {
                            return;
                        }
                        if (projection.entityId !== remote.entityId) {
                            throw new ConcurrentRemoteResourceError(remoteUri.path);
                        }
                        if (this.vfs.hasLiveDocumentProjection(existing.remoteEntityId)) {
                            // Opening the recovery session synchronously
                            // published its journal-visible projection. Let the
                            // queued direct event own the canonical transition.
                            return;
                        }
                        if (!existing.isIsolated
                            && existing.canonical !== projection.content
                            && local?.fileType === 'doc') {
                            await existing.onCanonicalChanged(
                                projection.content,
                                projection.revision,
                                projection.sequence,
                            );
                        }
                    });
                } else {
                    // Own the entire attach operation through the document
                    // scheduler. A resolver retires this scheduler before it
                    // changes the path, so a stale scan cannot attach after
                    // the conflict fence becomes active.
                    const scheduler = this.ensureTextScheduler(filePath);
                    try {
                        await scheduler.run('attach', async () => {
                            if (
                                this.isPathResolving(filePath)
                                || this.textRetirements.has(filePath)
                                || this.textReplicas.has(filePath)
                                || this.textSchedulers.get(filePath) !== scheduler
                            ) {
                                return;
                            }
                            const remoteUri = this.remoteUri(filePath);
                            const projection = await this.vfs.readDocumentProjection(
                                remoteUri,
                                remote.entityId,
                            );
                            if (
                                this.isPathResolving(filePath)
                                || this.textRetirements.has(filePath)
                                || this.textReplicas.has(filePath)
                                || this.textSchedulers.get(filePath) !== scheduler
                            ) {
                                return;
                            }
                            if (projection.entityId !== remote.entityId) {
                                throw new ConcurrentRemoteResourceError(remoteUri.path);
                            }
                            await this.createTextReplica(
                                filePath,
                                projection.content,
                                local,
                                scheduler,
                                projection.entityId,
                                projection.sequence,
                            );
                        });
                    } catch (error) {
                        if (
                            this.isPathResolving(filePath)
                            || this.textSchedulers.get(filePath) !== scheduler
                        ) {
                            completed = true;
                            continue;
                        }
                        throw error;
                    }
                }
                completed = true;
            } finally {
                if (completed) {
                    this.currentActionPath = previousActionPath;
                }
            }
        }
        return false;
    }

    private dropTextReplica(
        filePath: string,
        mode: 'handoff' | 'resource-change' = 'handoff',
        reason = 'The document resource changed while local input was active',
    ): Promise<void> {
        this.textRetirements ??= new Map<string, Promise<void>>();
        const active = this.textRetirements.get(filePath);
        if (active) {
            return active;
        }
        const machine = this.textReplicas.get(filePath);
        const scheduler = this.textSchedulers.get(filePath);
        this.textReplicas.delete(filePath);
        this.textSchedulers.delete(filePath);
        const retirement = (async () => {
            try {
                if (machine && !machine.isIsolated) {
                    if (mode === 'resource-change') {
                        const isolation = scheduler
                            ? await scheduler.run(
                                'retire',
                                () => machine.freezeForConflict(reason),
                            )
                            : await machine.freezeForConflict(reason);
                        const baseline = this.state?.entries[filePath];
                        const baselineText = baseline?.fileType === 'doc'
                            ? baseline.text
                            : undefined;
                        // Text that still equals the durable common base is not
                        // a local branch: it is the file waiting for a remote
                        // projection that had not landed yet. Only content that
                        // differs from the remote head *and* from that base is
                        // evidence somebody wrote here, and archiving anything
                        // less records a conflict whose local side is the base.
                        const divergesFromBoth = (content?: string): boolean => (
                            content !== undefined
                            && content !== isolation.canonicalContent
                            && content !== baselineText
                        );
                        const hasDivergentLocalProjection = divergesFromBoth(isolation.editorContent)
                            || divergesFromBoth(isolation.diskContent);
                        // Divergent bytes are the whole basis for a conflict
                        // here. Pending journal work is not: an in-memory batch
                        // has already advanced the editor head, so it shows up
                        // as divergence above, and an unacknowledged operation
                        // is durable in the document WAL, which outlives this
                        // actor and replays when the resource scan reattaches
                        // the document at its new path. Conflicting on it as
                        // well archived the baseline as both sides of a
                        // conflict, at a path the move had already removed,
                        // leaving the user to resolve by hand a fork that
                        // never happened.
                        if (hasDivergentLocalProjection) {
                            await this.createTextConflict(isolation);
                        }
                    } else {
                        if (scheduler) {
                            await scheduler.run(
                                'retire',
                                () => machine.prepareForHandoff(),
                            );
                        } else {
                            await machine.prepareForHandoff();
                        }
                    }
                }
            } finally {
                machine?.dispose();
                scheduler?.dispose();
                await Promise.all([
                    machine?.disposeAndWait(),
                    scheduler?.disposeAndWait(),
                ]);
                await this.textConflictTasks?.get(filePath);
            }
        })().finally(() => {
            if (this.textRetirements.get(filePath) === retirement) {
                this.textRetirements.delete(filePath);
            }
        });
        this.textRetirements.set(filePath, retirement);
        return retirement;
    }

    private ensureTextScheduler(filePath: string): FairTaskScheduler {
        let scheduler = this.textSchedulers.get(filePath);
        if (!scheduler) {
            scheduler = new FairTaskScheduler();
            this.textSchedulers.set(filePath, scheduler);
        }
        return scheduler;
    }

    /**
     * Fence the old document owner before a conflict choice mutates its path.
     * The current editor head crosses the journal boundary before queued stale
     * callbacks are discarded with the retired machine.
     */
    private async retireTextReplicasUnder(filePath: string): Promise<void> {
        this.textRetirements ??= new Map<string, Promise<void>>();
        const retirements: Promise<void>[] = [];
        for (const candidate of [...this.textReplicas.keys()]) {
            if (candidate !== filePath && !candidate.startsWith(`${filePath}/`)) {
                continue;
            }
            retirements.push(this.dropTextReplica(candidate));
        }
        for (const [candidate, retirement] of this.textRetirements) {
            if (candidate === filePath || candidate.startsWith(`${filePath}/`)) {
                retirements.push(retirement);
            }
        }
        await Promise.all(retirements);
    }

    private clearProjectionReceiptsUnder(filePath: string): void {
        const state = this.requireState();
        state.projectionReceipts = (state.projectionReceipts ?? []).filter(receipt => {
            const separator = receipt.path.indexOf(':');
            const projectedPath = separator < 0
                ? receipt.path
                : receipt.path.slice(separator + 1);
            return projectedPath !== filePath
                && !projectedPath.startsWith(`${filePath}/`);
        });
    }

    private isPathResolving(filePath: string): boolean {
        return [...(this.resolvingPaths ?? [])].some(candidate => (
            filePath === candidate || filePath.startsWith(`${candidate}/`)
        ));
    }

    /** Apply a minimal editor projection only to the version we inspected. */
    private async projectEditorCas(
        filePath: string,
        expectedContent: string,
        targetContent: string,
    ): Promise<boolean> {
        const document = vscode.workspace.textDocuments.find(
            candidate => candidate.uri.fsPath === this.localUri(filePath).fsPath,
        );
        if (!document || document.getText() === targetContent) {
            return true;
        }
        if (document.getText() !== expectedContent) {
            return false;
        }
        const replacements = planEditorReplacements(expectedContent, targetContent);
        const visibleEditor = vscode.window.visibleTextEditors.find(
            editor => editor.document.uri.toString() === document.uri.toString(),
        );
        let applied: boolean;
        if (visibleEditor) {
            // TextEditor.edit sends the captured document version to the
            // workbench. A keystroke racing this call makes it return false
            // instead of allowing a stale range to replace that input.
            applied = await visibleEditor.edit(builder => {
                for (const replacement of replacements) {
                    const start = document.positionAt(replacement.startOffset);
                    const end = document.positionAt(replacement.endOffset);
                    if (replacement.startOffset === replacement.endOffset) {
                        builder.insert(start, replacement.text);
                    } else {
                        builder.replace(new vscode.Range(start, end), replacement.text);
                    }
                }
            }, {
                undoStopBefore: false,
                undoStopAfter: false,
            });
        } else {
            // WorkspaceEdit captures the current document version while it is
            // converted for IPC. Build and submit it without an await between
            // the expected-content check and applyEdit.
            const edit = new vscode.WorkspaceEdit();
            for (const replacement of replacements) {
                edit.replace(document.uri, new vscode.Range(
                    document.positionAt(replacement.startOffset),
                    document.positionAt(replacement.endOffset),
                ), replacement.text);
            }
            applied = await vscode.workspace.applyEdit(edit);
        }
        // A successful versioned edit proves that targetContent was installed.
        // A keystroke can advance the document again before this Promise
        // resumes; that is a later editor transition, not a failed projection.
        // Keep the receipt so the queued target event remains non-mutating.
        return applied;
    }

    private async createTextReplica(
        filePath: string,
        canonicalContent: string,
        local: ReplicaSnapshot | undefined,
        scheduler?: FairTaskScheduler,
        remoteEntityId?: string,
        canonicalSequence?: number,
    ): Promise<void> {
        if (
            this.isPathResolving(filePath)
            || this.textRetirements?.has(filePath)
            || this.textReplicas.has(filePath)
            || (scheduler !== undefined && this.textSchedulers.get(filePath) !== scheduler)
        ) {
            return;
        }
        const diskContent = await this.readLocalText(filePath);
        if (
            this.isPathResolving(filePath)
            || this.textRetirements?.has(filePath)
            || this.textReplicas.has(filePath)
            || (scheduler !== undefined && this.textSchedulers.get(filePath) !== scheduler)
        ) {
            return;
        }
        const openDocument = vscode.workspace.textDocuments.find(
            document => document.uri.fsPath === this.localUri(filePath).fsPath,
        );
        if (openDocument) {
            this.rememberEditorObservationHead(openDocument);
        }
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

        if (
            local?.fileType === 'doc'
            && (
                diskContent === undefined
                || (!dirtyEditorHasProvableBase && diskContent !== local.text)
            )
        ) {
            throw new StaleLocalSnapshotError(
                `Local file changed while attaching ${filePath}`,
                diskContent === undefined ? undefined : new TextEncoder().encode(diskContent),
            );
        }

        if (editorIsDirty && editorContent !== undefined && editorAndDiskDiffer && !dirtyEditorHasProvableBase) {
            // The scan that selected this attach point did not contain this
            // fork. Do not turn a scan-to-attach race into a conflict: retry
            // from a fresh scan, where the resource planner can prove and
            // merge both branches or archive a real unprovable fork.
            throw new StaleLocalSnapshotError(
                `Editor or disk changed while attaching ${filePath}`,
                new TextEncoder().encode(editorContent),
            );
        }

        const documentScheduler = scheduler ?? new FairTaskScheduler();
        // Annotated because the port below reads back through this binding.
        const machine: TextReplicaStateMachine = new TextReplicaStateMachine({
            path: filePath,
            diagnosticsEnabled: /^hershel-e2e-/.test(filePath),
            remoteEntityId,
            canonicalContent,
            canonicalSequence,
            // Seed a dirty editor with its durable base. The first observed
            // value below is then journaled as an OT operation.
            editorContent: editorContent === canonicalContent
                ? editorContent
                : dirtyEditorHasProvableBase ? baselineText : editorContent ?? diskContent,
            editorDirty: editorIsDirty,
            // Keep the baseline as the previous disk observation so an
            // uncoordinated filesystem write is classified by evidence.
            // A journal-recovered canonical snapshot already present on disk
            // is our converged projection, not a new filesystem mutation.
            diskContent: diskContent === canonicalContent
                ? diskContent
                : baselineText ?? diskContent,
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
                remoteEntityId,
            ),
            projectEditor: (content, expectedContent) => this.projectEditorCas(
                filePath,
                expectedContent,
                content,
            ),
            readEditor: async () => vscode.workspace.textDocuments.find(
                candidate => candidate.uri.fsPath === this.localUri(filePath).fsPath,
            )?.getText(),
            restoreSavedEditor: async expectedContent => {
                const document = vscode.workspace.textDocuments.find(
                    candidate => candidate.uri.fsPath === this.localUri(filePath).fsPath,
                );
                if (!document || document.getText() !== expectedContent) {
                    // The user is now editing these bytes. Only the projection
                    // that dirtied a saved document may save it again.
                    return false;
                }
                if (!document.isDirty) {
                    // Already saved, by this host or by auto-save. The write
                    // that cleared the marker carries the same bytes as the
                    // receipt recorded for this restore.
                    return true;
                }
                // This save installs the projection: it writes the bytes the
                // machine holds as canonical, and its watcher echo is
                // classified by the receipt recorded before it started.
                return await document.save();
            },
            projectDisk: async (content, expectedContent) => {
                await this.ensureLocalParent(filePath);
                const result = await this.localProjectionCas.replace(
                    filePath,
                    expectedContent === undefined
                        ? undefined
                        : contentHash(new TextEncoder().encode(expectedContent)),
                    new TextEncoder().encode(content),
                );
                if (result.recoveryCandidate) {
                    await this.registerProjectionRecoveryConflicts([
                        result.recoveryCandidate,
                    ]);
                    await machine.freezeForConflict(
                        'A local writer changed a displaced projection inode',
                    );
                    return false;
                }
                if (
                    !result.applied
                    && result.actualWasReadFromInstalledTarget
                    && result.actual !== undefined
                ) {
                    return {
                        applied: false,
                        actualContent: new TextDecoder('utf-8', {fatal: true})
                            .decode(result.actual),
                        installedAncestor: content,
                    };
                }
                return result.applied;
            },
            // The machine re-reads disk after its own projections and after a
            // CAS it lost, and feeds what it finds straight back in as an
            // observation. Those reads race external writers exactly as the
            // watcher does, so they need the same truncation-window guard: an
            // empty read taken mid-rewrite is journaled as a deletion, and the
            // rewrite that follows only restores its own writer's buffer.
            readDisk: () => this.readSettledLocalText(filePath, machine),
            onConverged: content => this.advanceTextBaselineIfAuthoritative(
                filePath,
                content,
                remoteEntityId,
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
            scheduleInputBoundary: task => documentScheduler.run('input-boundary', task),
            scheduleProjectionRetry: (task, delayMs) => {
                const timer = setTimeout(() => {
                    if (this.textReplicas.get(filePath) !== machine) {
                        return;
                    }
                    void documentScheduler.run('projection-retry', task).catch(error => {
                        if (this.textReplicas.get(filePath) === machine) {
                            this.handleTextReplicaError(filePath, error);
                        }
                    });
                }, delayMs);
                timer.unref();
            },
            hasPendingEditorObservations: () => (
                documentScheduler.pendingCountFor('editor') > 0
                || this.editorObservationDrains?.has(filePath)
                || (this.pendingEditorObservations?.get(filePath)?.length ?? 0) > 0
            ),
            isolate: record => this.createTextConflict(record),
        });
        if (
            this.isPathResolving(filePath)
            || this.textRetirements?.has(filePath)
            || this.textReplicas.has(filePath)
            || (scheduler !== undefined && this.textSchedulers.get(filePath) !== scheduler)
        ) {
            return;
        }
        this.textReplicas.set(filePath, machine);
        this.textSchedulers.set(filePath, documentScheduler);

        if (!local || local.fileType !== 'doc') {
            // A remote-only document with no local baseline is a new remote
            // entity. Project it after recovery, never by calling save().
            await machine.onCanonicalChanged(canonicalContent, undefined, canonicalSequence);
            return;
        }
        const localText = local.text;
        if (localText === canonicalContent) {
            // Startup may recover the WAL and disk projection before this
            // actor attaches. Publish that already-converged state so the
            // resource baseline advances without waiting for a watcher event
            // that happened in the previous extension host.
            await machine.onCanonicalChanged(canonicalContent, undefined, canonicalSequence);
            return;
        }
        if (dirtyEditorHasProvableBase) {
            if (editorContent !== undefined) {
                await machine.onEditorChanged(editorContent);
                await machine.flushInput(true);
            }
            return;
        }
        if (baselineText !== undefined) {
            if (localText !== baselineText) {
                // A clean external writer and Overleaf may both have advanced
                // since the durable common base. Submit the local delta from
                // that base; the document session OT-rebases it over the
                // current remote projection instead of manufacturing a
                // conflict merely because this machine attached late.
                await machine.onDiskObserved(localText);
            } else {
                await machine.onCanonicalChanged(canonicalContent, undefined, canonicalSequence);
            }
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
        expectedRemoteEntityId?: string,
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
        if (
            remote.fileType !== 'doc'
            || (
                expectedRemoteEntityId !== undefined
                && remote.entityId !== expectedRemoteEntityId
            )
        ) {
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
        this.traceBaselineWrite('advance-authoritative', filePath, state.entries[filePath]);
        await this.stateStore.save(state);
    }

    private createTextConflict(record: TextReplicaIsolation): Promise<void> {
        const existing = this.textConflictTasks.get(record.path);
        if (existing) {
            return existing;
        }
        const task = this.persistTextConflict(record);
        this.textConflictTasks.set(record.path, task);
        const clear = () => {
            if (this.textConflictTasks.get(record.path) === task) {
                this.textConflictTasks.delete(record.path);
            }
        };
        task.then(clear, clear);
        return task;
    }

    /**
     * Whether an observation of an isolated path is the user's text or our own.
     *
     * Resolving a conflict writes the chosen content into the very path the
     * conflict is about, and the editor and watcher report those bytes back
     * like any other change. Recording them would archive the resolution's own
     * output as the local side of the conflict — and after the artifacts are
     * renamed into the resolved tree, a straggler rewrites the manifest there,
     * overwriting the evidence the archive exists to preserve. A real edit
     * made during that window is not lost: the path re-attaches as soon as the
     * resolution finishes and journals the editor's content as a local edit.
     */
    private acceptsIsolatedTextRevisions(filePath: string): boolean {
        return !this.isPathResolving(filePath);
    }

    private persistIsolatedTextRevision(
        filePath: string,
        content: string,
        source: 'editor' | 'disk',
    ): Promise<void> {
        if (!this.acceptsIsolatedTextRevisions(filePath)) {
            return Promise.resolve();
        }
        this.isolatedRevisionTasks ??= new Map<string, Promise<void>>();
        const previous = this.isolatedRevisionTasks.get(filePath) ?? Promise.resolve();
        const task = previous.catch(() => {}).then(async () => {
            await this.textConflictTasks.get(filePath)?.catch(() => {});
            const state = this.requireState();
            const record = state.conflicts[filePath];
            if (!record) {
                throw new Error(`Isolated text revision has no conflict record: ${filePath}`);
            }
            if (record.artifactDirectory.startsWith('.overleaf/conflicts/resolved/')) {
                // The artifacts were renamed into the resolved tree while this
                // observation was in flight. That archive is closed evidence of
                // what the two sides held; nothing may write into it again.
                return;
            }
            const bytes = new TextEncoder().encode(content);
            const revisionFile = `${record.artifactDirectory}/local-revisions/`
                + `${this.timestamp()}-${randomUUID()}/${source}/${filePath}`;
            await this.writeArtifact(revisionFile, bytes);
            record.localFile = revisionFile;
            record.localHash = contentHash(bytes);
            await this.writeConflictManifest(record, {
                latestLocalSource: source,
                latestLocalRevisionAt: new Date().toISOString(),
            });
            await this.stateStore.save(state);
            this.output.appendLine(
                `[conflict-revision] preserved ${source} revision for ${filePath}`,
            );
        }).finally(() => {
            if (this.isolatedRevisionTasks.get(filePath) === task) {
                this.isolatedRevisionTasks.delete(filePath);
            }
        });
        this.isolatedRevisionTasks.set(filePath, task);
        return task;
    }

    private async persistTextConflict(record: TextReplicaIsolation): Promise<void> {
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
                record.remoteEntityId,
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
            baseType: baseline?.fileType,
            baselineEntityId: baseline?.entityId,
            remoteFile: `${artifactDirectory}/remote/${record.path}`,
            remoteEntityId: record.remoteEntityId ?? remote?.entityId,
            remoteRevision,
            localFile: record.editorContent === undefined
                ? record.diskContent === undefined ? undefined : `${artifactDirectory}/local-disk/${record.path}`
                : `${artifactDirectory}/local-editor/${record.path}`,
            localType: record.editorContent === undefined && record.diskContent === undefined
                ? undefined
                : 'doc',
            remoteHash: contentHash(new TextEncoder().encode(record.canonicalContent)),
            remoteType: 'doc',
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
        this.textIsolationEvents++;
        this.status = { status: 'need-attention', message: `Isolated text path: ${record.path}` };
        this.renderReport(this.lastScan ?? {local: new Map(), remote: new Map()}, `isolated ${record.path}`);
        // Re-enter the serialized planner after the durable fence is written.
        // This retires metadata-only false observations immediately; genuine
        // divergent candidates remain paused with their complete artifacts.
        if (this.observationReady) {
            this.scheduleReconcile(`text isolation persisted: ${record.path}`, 0);
        }
    }

    /**
     * Retire a false observation conflict that preserved no divergent state.
     * Later endpoint changes are reconciled afresh from the retained baseline;
     * the live VFS journal is deliberately kept so remote OT history survives.
     */
    private async recoverNonDivergentEditorObservationConflict(): Promise<boolean> {
        const state = this.requireState();
        const record = Object.values(state.conflicts).find(candidate => (
            candidate.reason === 'Cannot prove the event-time ancestor of a VS Code edit'
            && candidate.baseHash !== undefined
            && candidate.baseHash === candidate.localHash
            && candidate.baseHash === candidate.remoteHash
            && candidate.baselineEntityId !== undefined
            && candidate.baselineEntityId === candidate.remoteEntityId
        ));
        if (!record) {
            return false;
        }

        await this.recoverConflictArtifactLocation(record);
        await this.retireTextReplicasUnder(record.path);
        await this.drainIsolatedRevisionTasks(record.path);
        if (
            state.conflicts[record.path] !== record
            || record.baseHash === undefined
            || record.baseHash !== record.localHash
            || record.baseHash !== record.remoteHash
            || record.baselineEntityId === undefined
            || record.baselineEntityId !== record.remoteEntityId
        ) {
            return false;
        }
        this.clearProjectionReceiptsUnder(record.path);
        await this.moveConflictToResolved(record);
        delete state.conflicts[record.path];
        try {
            await this.stateStore.save(state);
        } catch (error) {
            state.conflicts[record.path] = record;
            throw error;
        }
        this.output.appendLine(
            `[auto-recovered] ${record.path}: the isolated base, local, and remote snapshots were identical`,
        );
        return true;
    }

    private handleTextReplicaError(filePath: string, error: unknown): void {
        if (this.disposed || this.handoffInProgress) {
            return;
        }
        const normalized = this.asError(error);
        if (normalized instanceof DocumentSchedulerRetiredError) {
            return;
        }
        if (normalized instanceof DocumentRecoveryIsolationError) {
            this.output.appendLine(`[recovery] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, 0);
            return;
        }
        if (normalized instanceof UnattributedDiskSnapshotError) {
            // A resource action materialized the file underneath an actor that
            // attached for a remote-only document. Retire the actor and rescan:
            // the next attach sees the local text and its baseline, so it can
            // either project the remote change or submit a provable local
            // delta. Keeping the actor attached would only repeat the failure,
            // and archiving a conflict here would record a local side the
            // actor never held.
            this.output.appendLine(`[resource-change] ${normalized.message}`);
            void this.dropTextReplica(filePath, 'resource-change', normalized.message)
                .then(() => this.scheduleReconcile(normalized.message, 0))
                .catch(dropError => this.failProject(this.asError(dropError)));
            return;
        }
        // A watcher/editor callback can finish after the path has already
        // changed again. These errors describe an obsolete observation, not a
        // loss of OT ancestry; let the serialized resource scan re-read both
        // endpoints before deciding whether a conflict exists.
        if (
            normalized instanceof StaleLocalSnapshotError
            || normalized instanceof ConcurrentRemoteResourceError
            || this.isMissingPathFailure(normalized)
            || this.isTransientFailure(normalized)
        ) {
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, this.nextTransientRetryDelay());
            return;
        }
        if (this.isRemoteCapacityFailure(normalized)) {
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, REMOTE_CAPACITY_RETRY_MS);
            return;
        }
        void this.createTextConflict({
            path: filePath,
            reason: normalized.message,
            canonicalContent: this.textReplicas.get(filePath)?.canonical ?? '',
        }).catch(conflictError => this.failProject(this.asError(conflictError)));
    }

    private async readRemoteText(
        filePath: string,
        expectedRemoteEntityId?: string,
    ): Promise<string> {
        return (await this.vfs.readDocumentProjection(
            this.remoteUri(filePath),
            expectedRemoteEntityId,
        )).content;
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
            await gateway.stop();
        }
    }

    private async handleAiGatewayRequest(request: AiGatewayRequest): Promise<unknown> {
        if (request.method === 'handoff') {
            if (this.handoffPromise) {
                return this.handoffPromise;
            }
            const handoff = this.handoffReplicaOwnership(request);
            this.handoffPromise = handoff;
            void handoff.then(
                () => {
                    if (this.handoffPromise === handoff && !this.disposed) {
                        this.handoffPromise = undefined;
                    }
                },
                () => {
                    if (this.handoffPromise === handoff && !this.disposed) {
                        this.handoffPromise = undefined;
                    }
                },
            );
            return handoff;
        }
        if (this.disposed || this.handoffInProgress || !this.observationReady) {
            throw new Error('Safe-sync recovery or ownership handoff is in progress; retry later');
        }
        const active = this.handleNonHandoffAiGatewayRequest(request);
        this.activeAiGatewayRequests.add(active);
        try {
            return await active;
        } finally {
            this.activeAiGatewayRequests.delete(active);
        }
    }

    private async handleNonHandoffAiGatewayRequest(request: AiGatewayRequest): Promise<unknown> {
        const filePath = typeof request.path === 'string'
            ? normalizeRelativePath(request.path)
            : undefined;
        let scheduler = filePath === undefined ? undefined : this.textSchedulers.get(filePath);
        if (
            !scheduler
            && this.observationReady
            && this.state?.initialized
            && filePath
            && !filePath.split('/').some(part => part === '' || part === '.' || part === '..')
            && !this.isIgnored(filePath)
        ) {
            await this.ensureTextMachineForGateway(filePath);
            scheduler = this.textSchedulers.get(filePath);
        }
        if (!scheduler) {
            return this.handleAiGatewayRequestInternal(request);
        }
        return scheduler.run('ai', () => this.handleAiGatewayRequestInternal(request));
    }

    /** Flush the durable boundary before a newer VS Code window takes ownership. */
    private async handoffReplicaOwnership(request: AiGatewayRequest): Promise<unknown> {
        if (
            request.replicaPath !== path.resolve(this.baseUri.fsPath)
            || !Number.isSafeInteger(request.requesterPid)
            || Number(request.requesterPid) <= 0
        ) {
            throw new Error('Safe-sync handoff request does not match this replica');
        }
        if ((!this.observationReady && !this.startupSettled) || this.disposed) {
            throw new Error('Safe-sync owner is not ready to hand off this replica');
        }
        this.handoffInProgress = true;
        this.handoffMutationObserved = false;
        this.handoffDeferredEditorObservations.clear();
        this.observationReady = false;
        const unmanagedDirtyDocuments = vscode.workspace.textDocuments.filter(document => {
            const relative = this.relativeLocalPath(document.uri);
            const machine = relative === undefined ? undefined : this.textReplicas.get(relative);
            return relative !== undefined
                && !this.isIgnored(relative)
                && document.isDirty
                && (!machine || machine.isIsolated);
        });
        if (unmanagedDirtyDocuments.length > 0) {
            this.handoffInProgress = false;
            this.observationReady = true;
            throw new Error('Safe-sync cannot hand off an untracked or isolated dirty document');
        }
        if (this.reconcileTimer) {
            clearTimeout(this.reconcileTimer);
            this.reconcileTimer = undefined;
        }
        this.pendingReasons.clear();
        this.output.appendLine(
            `[handoff] flushing owner ${process.pid} for extension host ${String(request.requesterPid)}`,
        );
        try {
            await this.drainActiveAiGatewayRequests();
            await this.reconcileQueue.run(async () => {});
            await this.drainQueuedEditorObservations();
            await this.drainTextRetirements();
            await this.drainTextConflictTasks();
            await this.drainIsolatedRevisionTasks();
            if (this.pausedError) {
                throw this.pausedError;
            }
            for (const [filePath, machine] of this.textReplicas) {
                await this.textTask(filePath, 'handoff', () => machine.prepareForHandoff());
            }
            await this.drainTextRetirements();
            await this.drainTextConflictTasks();
            await this.drainIsolatedRevisionTasks();
            if (this.handoffMutationObserved) {
                throw new Error('Safe-sync owner observed a new editor change during handoff; retry');
            }
            await new Promise<void>(resolve => setImmediate(resolve));
            if (this.handoffMutationObserved) {
                throw new Error('Safe-sync owner observed a queued editor change during handoff; retry');
            }
            this.handoffInProgress = false;
            this.observationReady = true;
            this.handoffMutationObserved = false;
            if (this.disposeRequestedDuringHandoff) {
                this.disposeRequestedDuringHandoff = false;
                this.dispose();
                return {closing: true, previousOwnerPid: process.pid};
            }
            // The old workbench remains the sole live owner until VS Code
            // actually closes it. Normal dispose then drains every source and
            // releases the lease. If close is cancelled, this owner stays
            // fully observable and the successor cannot enter.
            setTimeout(() => {
                void vscode.commands.executeCommand('workbench.action.closeWindow');
            }, 0);
            return {closing: true, previousOwnerPid: process.pid};
        } catch (error) {
            this.handoffInProgress = false;
            this.observationReady = true;
            const deferred = [...this.handoffDeferredEditorObservations.entries()];
            this.handoffDeferredEditorObservations.clear();
            for (const [filePath, observation] of deferred) {
                void this.onLocalTextDocumentChange(
                    filePath,
                    observation.content,
                    observation.observedAncestor,
                    true,
                    observation.editorDirty,
                );
            }
            this.handoffMutationObserved = false;
            if (this.disposeRequestedDuringHandoff) {
                this.disposeRequestedDuringHandoff = false;
                this.dispose();
            }
            this.scheduleReconcile('handoff aborted', 0);
            throw error;
        }
    }

    /**
     * A replica directory has one extension-host writer. If a newer VS Code
     * window opens it, the existing host is asked to durably drain and release
     * the lease before this host retries. This prevents an editable window
     * from silently running without synchronization.
     */
    private async acquireReplicaLock(): Promise<ReplicaLock> {
        const options = {
            onLost: (error: Error) => {
                this.lockOwnershipLost = true;
                this.lockHeld = false;
                this.observationReady = false;
                for (const machine of this.textReplicas.values()) {
                    machine.dispose();
                }
                for (const scheduler of this.textSchedulers.values()) {
                    scheduler.dispose();
                }
                void this.stopAiGateway().catch(() => {});
                void this.vfs.retireForReplicaHandoff().catch(() => {});
                this.failProject(error);
                this.dispose();
            },
        };
        const metadataPath = path.join(
            this.baseUri.fsPath,
            '.overleaf',
            'safe-sync-gateway.json',
        );
        const handoffDeadline = Date.now() + 30000;
        let latestError: unknown;
        let nextHandoffAttempt = 0;
        while (Date.now() < handoffDeadline) {
            try {
                return await ReplicaLock.acquire(
                    this.context.globalStorageUri.fsPath,
                    this.baseUri.fsPath,
                    options,
                );
            } catch (error) {
                latestError = error;
                if (!(error instanceof ReplicaLockedError)) {
                    throw error;
                }
                if (Date.now() >= nextHandoffAttempt) {
                    nextHandoffAttempt = Date.now() + 500;
                    try {
                        await sendAiGatewayRequest(metadataPath, {
                            method: 'handoff',
                            replicaPath: path.resolve(this.baseUri.fsPath),
                            requesterPid: process.pid,
                        }, Math.max(1, handoffDeadline - Date.now()));
                    } catch (handoffError) {
                        latestError = handoffError;
                        const message = handoffError instanceof Error
                            ? handoffError.message
                            : String(handoffError);
                        if (!/not ready|recovery is still in progress|ENOENT|connect|socket|timed out|ETIMEDOUT/i.test(message)) {
                            // An old owner may not understand handoff yet. Keep
                            // waiting for its lease to expire, but never delete
                            // a live lock pre-emptively.
                        }
                    }
                }
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        throw latestError instanceof Error
            ? latestError
            : new ReplicaLockedError();
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
        if (!this.state?.initialized) {
            throw new Error('Safe-sync baseline is not initialized');
        }
        const completedCommit = request.method === 'commit'
            ? this.completedAiCommitForRequest(request, filePath)
            : undefined;
        // A completed receipt is immutable historical proof. A later move,
        // replacement, deletion, or ignore-setting change cannot turn the
        // already journaled request back into a failure.
        if (completedCommit) {
            return this.hydrateCompletedAiCommit(completedCommit);
        }
        const pendingCommit = request.method === 'commit'
            ? this.pendingAiCommitForRequest(request, filePath)
            : undefined;
        if (pendingCommit) {
            const machine = [...this.textReplicas.values()].find(candidate => (
                candidate.remoteEntityId === pendingCommit.entityId
            ));
            try {
                return await this.recoverAiCommitIntent(pendingCommit, machine);
            } catch (error) {
                const normalized = this.asError(error);
                if (!this.shouldIsolateAiCommitRecovery(normalized)) {
                    throw normalized;
                }
                await this.isolateAiCommitIntent(pendingCommit, normalized);
                throw new Error(
                    `AI commit recovery was isolated for ${pendingCommit.path}: ${normalized.message}`,
                );
            }
        }
        if (this.isIgnored(filePath)) {
            throw new Error(`AI gateway refuses ignored path ${filePath}`);
        }
        await this.ensureTextMachineForGateway(filePath);
        const machine = this.textReplicas.get(filePath);
        if (!machine || machine.isIsolated) {
            throw new Error(`Document is isolated or unavailable: ${filePath}`);
        }
        const ownedEntityId = machine.remoteEntityId;
        if (!ownedEntityId) {
            throw new Error(`Document identity is unavailable: ${filePath}`);
        }
        const projection = await this.vfs.readDocumentProjection(
            this.remoteUri(filePath),
            ownedEntityId,
        );
        if (this.textReplicas.get(filePath) !== machine) {
            throw new ConcurrentRemoteResourceError(this.remoteUri(filePath).path);
        }
        switch (request.method) {
            case 'begin': {
                const holder = typeof request.holder === 'string' && request.holder.length > 0
                    ? request.holder
                    : 'unknown-ai';
                if (this.pendingAiCommitForEntity(ownedEntityId)) {
                    throw new Error(`A pending AI commit already owns document ${ownedEntityId}`);
                }
                await machine.flushInput(true);
                const current = await this.vfs.readDocumentProjection(
                    this.remoteUri(filePath),
                    ownedEntityId,
                );
                if (this.textReplicas.get(filePath) !== machine) {
                    throw new ConcurrentRemoteResourceError(this.remoteUri(filePath).path);
                }
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
                if (transaction.commit) {
                    if (transaction.commit.targetContent !== request.content) {
                        throw new Error('AI transaction was already committed with different content');
                    }
                    return transaction.commit.receipt;
                }
                this.aiLeases.assertCanCommit(
                    projection.entityId,
                    lease.leaseId,
                    lease.fencingToken,
                );
                const intent = await this.persistAiCommitIntent(
                    lease,
                    filePath,
                    transaction.entityId,
                    transaction.baseContent,
                    request.content,
                );
                return this.recoverAiCommitIntent(intent, machine);
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
        if (this.disposed || this.handoffInProgress) {
            return;
        }
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
        if (error && !(error instanceof DocumentSessionRetiredError)) {
            this.handleTextReplicaError(transaction.path, error);
        }
    }

    private async ensureTextMachineForGateway(filePath: string): Promise<void> {
        if (this.isPathResolving(filePath)) {
            throw new Error(`Conflict resolution is in progress for ${filePath}`);
        }
        if (this.textReplicas.has(filePath)) {
            return;
        }
        let scan = this.lastScan ?? await this.scanBoth();
        if (await this.ensureTextReplicas(scan)) {
            scan = await this.scanBoth();
            this.lastScan = scan;
            await this.ensureTextReplicas(scan);
        }
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

    private completedAiCommitForRequest(
        request: AiGatewayRequest,
        filePath: string,
    ): CompletedAiCommit | undefined {
        if (typeof request.leaseId !== 'string') {
            return undefined;
        }
        const completed = this.state?.aiCompletedCommits?.[request.leaseId];
        if (!completed) {
            return undefined;
        }
        if ('targetContent' in completed) {
            throw new Error('Legacy AI commit receipt was not migrated before observation');
        }
        if (
            !Number.isInteger(request.fencingToken)
            || completed.fencingToken !== request.fencingToken
            || completed.path !== filePath
            || typeof request.content !== 'string'
            || completed.targetHash !== contentHash(new TextEncoder().encode(request.content))
        ) {
            throw new Error('AI commit retry does not match its durable transaction');
        }
        return completed;
    }

    private async hydrateCompletedAiCommit(
        completed: CompletedAiCommit,
    ): Promise<AiCommitReceipt> {
        const content = await this.stateStore.readBaseline(completed.receiptContentHash);
        if (!content) {
            throw new Error(`Completed AI commit ${completed.leaseId} is missing durable content`);
        }
        return {
            ...completed.receipt,
            content: new TextDecoder('utf-8', {fatal: true}).decode(content),
        };
    }

    private pendingAiCommitForRequest(
        request: AiGatewayRequest,
        filePath: string,
    ): AiCommitIntent | undefined {
        if (typeof request.leaseId !== 'string') {
            return undefined;
        }
        const pending = this.state?.aiCommitIntents?.[request.leaseId];
        if (!pending) {
            return undefined;
        }
        if (
            !Number.isInteger(request.fencingToken)
            || pending.fencingToken !== request.fencingToken
            || pending.path !== filePath
            || typeof request.content !== 'string'
            || pending.targetHash !== contentHash(new TextEncoder().encode(request.content))
            || (
                request.entityId !== undefined
                && request.entityId !== pending.entityId
            )
        ) {
            throw new Error('AI commit retry does not match its durable intent');
        }
        return pending;
    }

    private pendingAiCommitForEntity(entityId: string): AiCommitIntent | undefined {
        return Object.values(this.state?.aiCommitIntents ?? {})
            .find(intent => intent.entityId === entityId);
    }

    private async persistAiCommitIntent(
        lease: AiWriterLease,
        filePath: string,
        entityId: string,
        baseContent: string,
        targetContent: string,
    ): Promise<AiCommitIntent> {
        const state = this.requireState();
        if (this.pendingAiCommitForEntity(entityId)) {
            throw new Error(`A pending AI commit already owns document ${entityId}`);
        }
        const [baseHash, targetHash] = await Promise.all([
            this.stateStore.saveBaseline(new TextEncoder().encode(baseContent)),
            this.stateStore.saveBaseline(new TextEncoder().encode(targetContent)),
        ]);
        const intent: AiCommitIntent = {
            leaseId: lease.leaseId,
            fencingToken: lease.fencingToken,
            path: filePath,
            entityId,
            baseHash,
            targetHash,
            operationId: randomUUID(),
            createdAt: new Date().toISOString(),
        };
        const previous = state.aiCommitIntents;
        state.aiCommitIntents = {
            ...(state.aiCommitIntents ?? {}),
            [intent.leaseId]: intent,
        };
        try {
            await this.stateStore.save(state);
        } catch (error) {
            state.aiCommitIntents = previous;
            throw error;
        }
        return intent;
    }

    private async recoverAiCommitIntent(
        intent: AiCommitIntent,
        machine?: TextReplicaStateMachine,
    ): Promise<AiCommitReceipt> {
        const [baseBytes, targetBytes] = await Promise.all([
            this.stateStore.readBaseline(intent.baseHash),
            this.stateStore.readBaseline(intent.targetHash),
        ]);
        if (!baseBytes || !targetBytes) {
            throw new Error(`AI commit intent ${intent.leaseId} is missing durable content`);
        }
        const decoder = new TextDecoder('utf-8', {fatal: true});
        const baseContent = decoder.decode(baseBytes);
        const targetContent = decoder.decode(targetBytes);
        const inspection = await this.vfs.inspectDocumentOperation(
            this.remoteUri(intent.path),
            intent.entityId,
            intent.operationId,
        );
        let content = inspection.content;
        // Which session state `content` was read from. Everything below this
        // read is durable bookkeeping the session does not wait for, so a
        // newer state can be published while it runs; projecting without
        // saying which state this is would let the machine step backwards.
        let contentSequence = inspection.sequence;
        let acknowledgement: Promise<string> | undefined;
        if (inspection.disposition !== 'settled') {
            const accepted = await this.vfs.acceptDocumentSnapshotAgainstAncestor(
                this.remoteUri(intent.path),
                baseContent,
                targetContent,
                true,
                intent.entityId,
                intent.operationId,
            );
            if (accepted.opId !== undefined && accepted.opId !== intent.operationId) {
                throw new Error(`AI commit intent ${intent.leaseId} changed operation identity`);
            }
            content = accepted.content;
            contentSequence = accepted.sequence;
            acknowledgement = accepted.acknowledgement;
        }
        const receipt: AiCommitReceipt = {
            opId: intent.operationId,
            path: intent.path,
            content,
            journaled: true,
            remoteAcknowledged: false,
        };
        try {
            const receiptContentHash = await this.stateStore.saveBaseline(
                new TextEncoder().encode(content),
            );
            await this.persistCompletedAiCommit({
                leaseId: intent.leaseId,
                fencingToken: intent.fencingToken,
                path: intent.path,
                entityId: intent.entityId,
                targetHash: intent.targetHash,
                receiptContentHash,
                receipt: {
                    opId: intent.operationId,
                    path: intent.path,
                    journaled: true,
                    remoteAcknowledged: false,
                },
                completedAt: new Date().toISOString(),
            }, intent);
        } catch (error) {
            void acknowledgement?.catch(() => {});
            const normalized = this.asError(error);
            this.failProject(normalized);
            throw normalized;
        }
        const transaction = this.aiTransactions.get(intent.leaseId);
        if (transaction) {
            transaction.commit = {targetContent, receipt};
        }
        if (acknowledgement) {
            acknowledgement.then(
                () => void this.finishAiTransaction(intent.leaseId),
                error => void this.finishAiTransaction(intent.leaseId, error),
            );
        } else {
            void this.finishAiTransaction(intent.leaseId);
        }
        // Projection observes the durable journal/receipt transition; it does
        // not define whether retrying the logical operation is safe.
        await machine?.onCanonicalChanged(content, undefined, contentSequence);
        return receipt;
    }

    private async recoverPendingAiCommitIntents(): Promise<void> {
        const pending = Object.values(this.state?.aiCommitIntents ?? {})
            .sort((left, right) => (
                left.createdAt.localeCompare(right.createdAt)
                || left.leaseId.localeCompare(right.leaseId)
            ));
        for (const intent of pending) {
            try {
                await this.recoverAiCommitIntent(intent);
            } catch (error) {
                const normalized = this.asError(error);
                if (!this.shouldIsolateAiCommitRecovery(normalized)) {
                    throw normalized;
                }
                await this.isolateAiCommitIntent(intent, normalized);
            }
        }
    }

    private shouldIsolateAiCommitRecovery(error: Error): boolean {
        return error instanceof ConcurrentRemoteResourceError
            || error instanceof DocumentRecoveryIsolationError
            || error instanceof WalCorruptionError
            || this.isFileNotFound(error)
            || /cannot prove.*(?:operation|history|ancestry)|OT history|document identity/i.test(
                error.message,
            );
    }

    private async isolateAiCommitIntent(intent: AiCommitIntent, error: Error): Promise<void> {
        const state = this.requireState();
        const pending = state.aiCommitIntents?.[intent.leaseId];
        if (!pending || JSON.stringify(pending) !== JSON.stringify(intent)) {
            throw new Error(`AI commit intent changed before isolation: ${intent.leaseId}`);
        }
        const [baseContent, targetContent] = await Promise.all([
            this.stateStore.readBaseline(intent.baseHash),
            this.stateStore.readBaseline(intent.targetHash),
        ]);
        if (!baseContent || !targetContent) {
            throw new Error(`AI commit intent ${intent.leaseId} lost its recovery candidates`);
        }

        let remote;
        let remoteReadError: string | undefined;
        try {
            remote = await this.vfs.readAuthoritativeDocumentByIdWithoutRecovery(
                intent.entityId,
            );
        } catch (remoteError) {
            const normalized = this.asError(remoteError);
            if (this.isTransientFailure(normalized)) {
                throw normalized;
            }
            remoteReadError = normalized.message;
        }
        let remotePath = remote?.path;
        if (!remote) {
            try {
                const replacement = await this.vfs.readAuthoritativeFileWithoutRecovery(
                    this.remoteUri(intent.path),
                );
                remote = {...replacement, path: intent.path};
                remotePath = intent.path;
            } catch (remoteError) {
                const normalized = this.asError(remoteError);
                if (!this.isFileNotFound(normalized)) {
                    if (this.isTransientFailure(normalized)) {
                        throw normalized;
                    }
                    remoteReadError ??= normalized.message;
                }
            }
        }

        const artifactKey = contentHash(new TextEncoder().encode(
            `${intent.leaseId}\0${intent.operationId}`,
        )).slice(0, 20);
        const existing = state.conflicts[intent.path];
        const artifactDirectory = existing?.artifactDirectory
            ?? `.overleaf/conflicts/unresolved/ai-intent-${artifactKey}`;
        const evidenceRoot = `${artifactDirectory}/ai-intent/${artifactKey}`;
        const baseFile = `${evidenceRoot}/base/${intent.path}`;
        const targetFile = `${evidenceRoot}/local-candidate/${intent.path}`;
        await this.writeArtifact(baseFile, baseContent);
        await this.writeArtifact(targetFile, targetContent);
        const remoteFile = remote
            ? `${evidenceRoot}/remote/${normalizeRelativePath(remotePath ?? intent.path)}`
            : undefined;
        if (remote && remoteFile) {
            await this.writeArtifact(remoteFile, remote.content);
        }
        const reason = `Authorized AI commit could not prove recovery ancestry: ${error.message}`;
        const record: ConflictRecord = existing ?? {
            path: intent.path,
            artifactDirectory,
            reason,
            createdAt: new Date().toISOString(),
            baseFile,
            baseHash: intent.baseHash,
            baseType: 'doc',
            baselineEntityId: intent.entityId,
            localFile: targetFile,
            localHash: intent.targetHash,
            localType: 'doc',
            remoteFile,
            remoteHash: remote ? contentHash(remote.content) : undefined,
            remoteType: remote?.fileType,
            remoteEntityId: remote?.entityId,
            remoteRevision: remote?.revision,
        };
        const intentEvidence = {
            isolatedAiCommitIntent: true,
            leaseId: intent.leaseId,
            fencingToken: intent.fencingToken,
            operationId: intent.operationId,
            originalPath: intent.path,
            entityId: intent.entityId,
            baseFile,
            baseHash: intent.baseHash,
            targetFile,
            targetHash: intent.targetHash,
            currentRemoteFile: remoteFile,
            currentRemotePath: remotePath,
            currentRemoteHash: remote ? contentHash(remote.content) : undefined,
            currentRemoteType: remote?.fileType,
            currentRemoteEntityId: remote?.entityId,
            currentRemoteRevision: remote?.revision,
            currentRemoteReadError: remoteReadError,
            recoveryOperationId: intent.operationId,
            recoveryError: error.message,
            isolatedAt: new Date().toISOString(),
        };
        await this.writeArtifact(
            `${evidenceRoot}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                schemaVersion: 3,
                path: intent.path,
                reason,
                ...intentEvidence,
            }, null, 2)),
        );
        if (!existing) {
            await this.writeConflictManifest(record, intentEvidence);
        }

        const previousConflict = state.conflicts[intent.path];
        const previousIntents = state.aiCommitIntents;
        const retained = {...(state.aiCommitIntents ?? {})};
        delete retained[intent.leaseId];
        state.aiCommitIntents = Object.keys(retained).length > 0 ? retained : undefined;
        state.conflicts[intent.path] = record;
        try {
            await this.stateStore.save(state);
        } catch (saveError) {
            state.aiCommitIntents = previousIntents;
            if (previousConflict) {
                state.conflicts[intent.path] = previousConflict;
            } else {
                delete state.conflicts[intent.path];
            }
            throw saveError;
        }
        await this.retireIsolatedAiWriter(intent);
        this.textIsolationEvents++;
        this.output.appendLine(
            `[recovery] isolated AI commit ${intent.operationId} for ${intent.path}`,
        );
        this.status = {
            status: 'need-attention',
            message: `Recovered AI commit conflict: ${intent.path}`,
        };
    }

    private async retireIsolatedAiWriter(intent: AiCommitIntent): Promise<void> {
        const transaction = this.aiTransactions.get(intent.leaseId);
        if (
            transaction
            && (
                transaction.entityId !== intent.entityId
                || transaction.lease.leaseId !== intent.leaseId
                || transaction.lease.fencingToken !== intent.fencingToken
            )
        ) {
            const error = new Error(
                `AI transaction changed before isolated writer retirement: ${intent.leaseId}`,
            );
            this.failProject(error);
            throw error;
        }
        try {
            this.aiLeases.release(
                intent.entityId,
                intent.leaseId,
                intent.fencingToken,
            );
        } catch {
            // Startup clears active leases before intent recovery, and a live
            // lease may also have expired. Persist the manager's fenced state
            // either way so no durable ghost writer remains.
        }
        try {
            await this.persistAiLeases();
        } catch (persistenceError) {
            const normalized = this.asError(persistenceError);
            this.failProject(normalized);
            throw normalized;
        }
        if (transaction && this.aiTransactions.get(intent.leaseId) === transaction) {
            this.aiTransactions.delete(intent.leaseId);
        }
    }

    private async persistCompletedAiCommit(
        completed: CompletedAiCommit,
        intent: AiCommitIntent,
    ): Promise<void> {
        const state = this.requireState();
        const pending = state.aiCommitIntents?.[intent.leaseId];
        if (!pending || JSON.stringify(pending) !== JSON.stringify(intent)) {
            throw new Error(`AI commit intent changed before completion: ${intent.leaseId}`);
        }
        const previousCompleted = state.aiCompletedCommits;
        const previousPending = state.aiCommitIntents;
        recordCompletedAiCommit(state, completed);
        const retainedPending = {...(state.aiCommitIntents ?? {})};
        delete retainedPending[intent.leaseId];
        state.aiCommitIntents = Object.keys(retainedPending).length > 0
            ? retainedPending
            : undefined;
        try {
            await this.stateStore.save(state);
        } catch (error) {
            state.aiCompletedCommits = previousCompleted;
            state.aiCommitIntents = previousPending;
            throw error;
        }
    }

    private async persistAiLeases(): Promise<void> {
        if (!this.state) {
            return;
        }
        this.state.aiLeaseState = this.aiLeases.serialize();
        await this.stateStore.save(this.state);
    }

    /**
     * Delay before retrying a failure that only a later attempt can clear.
     *
     * A fixed short delay is right for the first few tries, when the cause is
     * usually a callback that raced a change. It is wrong once the cause is a
     * transport that cannot recover until something rebuilds it: the retry
     * then neither fixes nor reports anything, and at a fixed 100ms it spins
     * the reconcile queue hard enough to keep the replica from ever settling.
     * Back off geometrically to a ceiling so a stuck endpoint costs little,
     * while a genuinely transient failure still recovers in well under a
     * second.
     */
    private nextTransientRetryDelay(): number {
        const delay = Math.min(100 * 2 ** this.transientRetryStreak, 5000);
        this.transientRetryStreak += 1;
        return delay;
    }

    private scheduleReconcile(reason: string, delayMs = TEXT_DEBOUNCE_MS): void {
        if (this.disposed || this.handoffInProgress || this.pausedError) {
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

    /**
     * A reconcile scans the whole replica, so a second one waiting behind the
     * first only repeats that scan. Requests that arrive while a pass is still
     * waiting join it and contribute their reason: the state it will scan is
     * the state at the moment it starts, which already includes them. A request
     * that arrives while a pass is *running* still gets its own, because that
     * pass may have scanned before the change it is reporting landed.
     */
    private enqueueReconcile(reason: string): Promise<void> {
        const waiting = this.queuedReconcile;
        if (waiting) {
            waiting.reasons.add(reason);
            return waiting.done;
        }
        const reasons = new Set([reason]);
        const entry: QueuedReconcile = {
            reasons,
            done: this.reconcileQueue.run(async () => {
                if (this.queuedReconcile?.reasons === reasons) {
                    this.queuedReconcile = undefined;
                }
                const merged = [...reasons].join(', ');
                this.activeReconcileCount++;
                this.reconcileStarts++;
                this.activeReconcile = {
                    reason: merged,
                    startedAt: Date.now(),
                    phase: 'enter',
                    iteration: 0,
                };
                try {
                    try {
                        await this.reconcileUntilStable(merged);
                        // A pass that reached a stable state is proof the
                        // condition the last retry was backing off from is
                        // over, so the next one starts from the fast delay.
                        this.transientRetryStreak = 0;
                    } catch (error) {
                        await this.handleReconcileError(error);
                    }
                } finally {
                    this.activeReconcileCount--;
                    this.activeReconcile = undefined;
                }
            }),
        };
        this.queuedReconcile = entry;
        return entry.done;
    }

    private enqueueConflictResolution(
        filePath: string,
        resolution: ConflictResolution,
    ): Promise<string> {
        if (this.disposed || this.handoffInProgress) {
            return Promise.reject(new Error('Safe-sync ownership retirement is in progress'));
        }
        return this.reconcileQueue.run(async () => {
            this.activeReconcileCount++;
            try {
                try {
                    return await this.resolveConflictRecord(filePath, resolution);
                } catch (error) {
                    await this.handleReconcileError(error);
                    throw error;
                }
            } finally {
                this.activeReconcileCount--;
            }
        });
    }

    /**
     * Name where a reconcile currently is. "Not settled yet" is only
     * actionable when the phase holding the loop open is visible from outside.
     */
    private markReconcilePhase(phase: string, iteration: number): void {
        if (this.activeReconcile) {
            this.activeReconcile.phase = phase;
            this.activeReconcile.iteration = iteration;
        }
    }

    private async reconcileUntilStable(reason: string): Promise<void> {
        if (this.disposed || this.pausedError || !this.state) {
            return;
        }
        for (let actionCount = 0; actionCount < MAX_RECONCILE_ACTIONS; actionCount++) {
            this.markReconcilePhase('scan', actionCount);
            const scan = await this.scanBoth();
            this.lastScan = scan;
            this.clearLocalDeletionCandidates(scan);
            this.renderReport(scan, reason);

            this.markReconcilePhase('observation-recovery', actionCount);
            if (await this.recoverNonDivergentEditorObservationConflict()) {
                continue;
            }

            if (this.state.pendingAction) {
                this.markReconcilePhase('pending-action', actionCount);
                const pending = this.state.pendingAction;
                if (await this.isolateChangedRetainedPendingCandidate(pending, scan)) {
                    continue;
                }
                let documentRecovery: 'none' | 'advanced' | 'waiting';
                try {
                    documentRecovery = await this.recoverPendingDocumentOperation(pending);
                } catch (error) {
                    await this.createConflict(
                        pending.path,
                        `Pending document operation is not provable: ${this.asError(error).message}`,
                        scan,
                    );
                    if (this.state.pendingAction?.id === pending.id) {
                        this.state.pendingAction = undefined;
                        await this.stateStore.save(this.state);
                    }
                    continue;
                }
                if (documentRecovery === 'advanced') {
                    continue;
                }
                if (documentRecovery === 'waiting') {
                    this.status = {
                        status: 'push',
                        message: `Waiting for journaled document operation: ${pending.path}`,
                    };
                    this.scheduleReconcile(`document operation acknowledgement: ${pending.path}`, 250);
                    return;
                }
                // A local-choice type replacement is a durable compound
                // transaction: the old entity may already be retained while
                // the selected replacement has not yet been created. Resume
                // that exact journaled pre-state before generic ancestry
                // validation classifies the intentional vacancy as a delete.
                if (
                    pending.kind === 'resolve-conflict'
                    && await this.resumePendingAction(pending, scan)
                ) {
                    continue;
                }
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
                    this.traceBaselineWrite(recovery.kind, pending.path, recovery.baseline);
                    this.state.pendingAction = undefined;
                    await this.stateStore.save(this.state);
                    continue;
                }
                if (this.pendingActionCompleted(pending, scan)) {
                    if (
                        (pending.kind === 'move-local' || pending.kind === 'move-remote')
                        && pending.targetPath
                    ) {
                        this.relocateBaselineSubtree(
                            this.state,
                            pending.path,
                            pending.targetPath,
                        );
                    }
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

            this.markReconcilePhase('ensure-replicas', actionCount);
            if (await this.ensureTextReplicas(scan)) {
                continue;
            }

            this.markReconcilePhase('reconcile-one', actionCount);
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
                this.markReconcilePhase('refresh-converged', actionCount);
                await this.refreshConvergedState(this.state, scan);
                await this.stateStore.save(this.state);
                const pathConflictCount = Object.keys(this.state.conflicts).length;
                const detachedRecoveryCount = Object.keys(
                    this.state.detachedDocumentRecoveries ?? {},
                ).length;
                const conflictCount = pathConflictCount + detachedRecoveryCount;
                this.status = conflictCount > 0
                    ? {
                        status: 'need-attention',
                        message: `${pathConflictCount} path conflict(s), `
                            + `${detachedRecoveryCount} detached recovery record(s)`,
                    }
                    : { status: 'idle', message: 'Safe sync converged' };
                return;
            }
        }
        throw new Error(`Safe sync exceeded ${MAX_RECONCILE_ACTIONS} actions without converging`);
    }

    private async isolateChangedRetainedPendingCandidate(
        pending: PendingReplicaAction,
        scan: ScanPair,
    ): Promise<boolean> {
        if (!pending.entityId) {
            return false;
        }
        const retained = await this.vfs.inspectRetainedRemoteCandidate(pending.entityId);
        if (!retained) {
            return false;
        }
        const expectedHash = retained.fileType === 'folder'
            ? pending.expectedRemoteStructureHash
            : pending.expectedRemoteHash;
        if (expectedHash === undefined || retained.hash === expectedHash) {
            return false;
        }
        await this.isolateRetainedRemoteCandidate(
            pending.path,
            new RetainedRemoteCandidateError(
                this.remoteUri(pending.path),
                retained.entityId,
                retained.hash,
                retained.fileType,
            ),
            scan,
        );
        return this.requireState().conflicts[pending.path] !== undefined;
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
        const activeAiPath = [
            ...[...this.aiTransactions.values()].map(transaction => transaction.path),
            ...Object.values(this.state.aiCommitIntents ?? {}).map(intent => intent.path),
        ].find(filePath => filePath === namespace || filePath.startsWith(`${namespace}/`));
        if (activeAiPath) {
            throw new Error(`Cannot clean E2E namespace with active AI writer ${activeAiPath}`);
        }

        this.resolvingPaths.add(namespace);
        try {
            await this.retireTextReplicasUnder(namespace);
            await this.drainTextConflictTasks();
            const livePaths = new Map(
                this.vfs.walk(entity => entity._type === 'doc').map(({entity, path: entityPath}) => (
                    [entity._id, normalizeRelativePath(entityPath)] as const
                )),
            );
            const pendingRecoveries = await this.vfs.inspectPendingDocumentRecoveries();
            for (const recovery of pendingRecoveries) {
                const baselinePath = Object.entries(this.state.entries).find(([, entry]) => (
                    entry.fileType === 'doc' && entry.entityId === recovery.documentId
                ))?.[0];
                const conflictPath = Object.entries(this.state.conflicts).find(([, record]) => (
                    record.baselineEntityId === recovery.documentId
                    || record.remoteEntityId === recovery.documentId
                    || record.documentRecoveries?.some(candidate => (
                        candidate.entityId === recovery.documentId
                    )) === true
                ))?.[0];
                const recoveryPath = baselinePath
                    ?? conflictPath
                    ?? recovery.lastKnownPath
                    ?? livePaths.get(recovery.documentId);
                if (!recoveryPath) {
                    throw new Error(
                        `Cannot clean E2E namespace while document WAL `
                        + `${recovery.documentId} has no provable path`,
                    );
                }
                const normalized = normalizeRelativePath(recoveryPath);
                if (normalized === namespace || normalized.startsWith(`${namespace}/`)) {
                    throw new Error(
                        `Cannot clean E2E namespace with pending document WAL at ${normalized}`,
                    );
                }
            }

            const before = await this.scanBoth();
            const local = before.local.get(namespace);
            const remote = before.remote.get(namespace);
            if (remote) {
                const initialRemoteCount = [...before.remote.keys()].filter(candidate => (
                    candidate === namespace || candidate.startsWith(`${namespace}/`)
                )).length;
                for (let attempt = 0; ; attempt++) {
                    try {
                        await this.vfs.remove(this.remoteUri(namespace), true);
                        break;
                    } catch (error) {
                        if (!(error instanceof RetainedRemoteCandidateError)) {
                            throw error;
                        }
                        // Recursive removal can safely retain one changed
                        // descendant and stop before visiting its siblings.
                        // Continue only after a fresh scan proves that exact
                        // path is now absent and the original namespace root
                        // still owns the visible subtree. A replacement at
                        // either path remains untouched and fails closed.
                        const retained = await this.scanBoth();
                        const visibleRoot = retained.remote.get(namespace);
                        if (!visibleRoot) {
                            break;
                        }
                        const retainedPath = this.relativeRemotePath(error.uri);
                        if (
                            visibleRoot.entityId !== remote.entityId
                            || retainedPath === undefined
                            || retained.remote.has(retainedPath)
                            || attempt >= initialRemoteCount
                        ) {
                            throw error;
                        }
                    }
                }
            }
            if (local) {
                await this.deleteLocalSnapshot(namespace, local, before.local);
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
        } finally {
            this.resolvingPaths.delete(namespace);
        }
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
                this.relocateBaselineSubtree(state, move.oldPath, move.newPath);
                await this.stateStore.save(state);
                return 'acted';
            }
            if (move.action === 'move-local') {
                await this.ensureLocalParent(move.newPath);
                const expectedLocal = scan.local.get(move.oldPath);
                await this.journalAction(
                    'move-local',
                    move.oldPath,
                    expectedLocal,
                    scan.remote.get(move.newPath),
                    expectedLocal?.hash,
                    () => this.moveLocalSnapshotAgainstBaseline(
                        move.oldPath,
                        move.newPath,
                        expectedLocal,
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
                () => this.moveRemoteSnapshotAgainstBaseline(
                    move.oldPath,
                    move.newPath,
                    scan.remote.get(move.oldPath),
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
            const decision: ReconcileDecision = (
                baseline?.fileType === 'folder'
                && local === undefined
                && remote?.fileType === 'folder'
                && remote.entityId === baseline.entityId
                && remote.remoteStructureHash === EMPTY_REMOTE_FOLDER_TREE_HASH
            )
                ? {action: 'delete-remote'}
                : decideReconciliation(baseline, local, remote);
            if (
                decision.action === 'delete-remote'
                && remote?.fileType === 'folder'
                && [...scan.remote.keys()].some(candidate => candidate.startsWith(`${filePath}/`))
            ) {
                // Retain children first. Moving a parent with live descendants
                // would hide a concurrent remote edit before it had its own
                // baseline/identity check.
                continue;
            }
            if (
                decision.action === 'delete-remote'
                && !this.isStableLocalDeletion(filePath, baseline, remote)
            ) {
                return 'waiting';
            }
            if (
                decision.action === 'conflict'
                && !local
                && remote
                && !this.isStableLocalAbsence(filePath)
            ) {
                // The same absence that would be made to wait before it could
                // delete the remote entity must not be allowed to raise an
                // unresolvable conflict just because the remote side moved on
                // inside the window.
                return 'waiting';
            }
            if (
                (
                    decision.action === 'delete-local'
                    || (decision.action === 'conflict' && local && !remote)
                )
                && !this.isStableRemoteAbsence(filePath)
            ) {
                // A remote entity replaced at its own path is deleted and
                // recreated as two updates, so the project tree genuinely has
                // no entity there in between. Believing that gap deletes a
                // local file nobody asked to remove, or freezes the path in a
                // conflict whose remote evidence is the absence itself.
                return 'waiting';
            }
            if (decision.action !== 'delete-remote') {
                this.deletionCandidates().delete(filePath);
            }
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
            const localIsPluginProjection = record.projectionTargetHash !== undefined
                && local.hash === record.projectionTargetHash
                && record.localHash !== local.hash;
            if (!localIsPluginProjection && !this.isBinaryStable(record.path, local.hash)) {
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
            if (!localIsPluginProjection && record.localHash !== local.hash && local.content) {
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
                record.remoteEntityId = remote.entityId;
                record.remoteType = remote.fileType;
            }
            // The next local bytes are another plugin preview. Persist its
            // provenance before the projection can change the filesystem.
            record.projectionTargetHash = remote.hash;
            await this.writeConflictManifest(record, {
                localProjectionReason: 'binary-conflict-remote-preview',
            });
            await this.stateStore.save(state);
            await this.journalAction(
                'pull', record.path, local, remote, remote.hash,
                () => this.writeLocalSnapshot(remote, local),
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
                                local.localVariants?.diskHash,
                                remote.entityId,
                            );
                            await this.commitConvergedTextAction(
                                filePath,
                                remote.entityId,
                                converged,
                                local.localIdentity,
                            );
                            return;
                        }
                        await this.writeLocalSnapshot(remote, local);
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
                    () => this.deleteLocalSnapshot(filePath, local, scan.local),
                );
                return 'acted';
            case 'delete-remote':
                if (!remote) {
                    throw new Error(`Remote delete decision has no snapshot for ${filePath}`);
                }
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
                    () => this.removeRemoteSnapshot(remote),
                );
                return 'acted';
            case 'merge-text': {
                if (
                    !local
                    || !remote
                    || !remote.entityId
                    || remote.text === undefined
                    || local.text === undefined
                ) {
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
                        const actual = await this.acceptPendingDocumentOperation(
                            filePath,
                            remote.text!,
                            decision.content,
                            remote.entityId!,
                        );
                        const converged = await this.convergeTextToLocal(
                            filePath,
                            actual,
                            local.text!,
                            local.hash,
                            local.localVariants?.diskHash,
                            remote.entityId,
                        );
                        await this.commitConvergedTextAction(
                            filePath,
                            remote.entityId,
                            converged,
                            local.localIdentity,
                        );
                    },
                    undefined,
                    remote.entityId,
                    {ancestor: remote.text, target: decision.content},
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
                    const record = this.requireState().conflicts[filePath];
                    if (record) {
                        // The pull below is a plugin projection, not a newer
                        // local choice. Preserve the already archived local
                        // candidate when Resolve later re-scans the path.
                        record.projectionTargetHash = remote.hash;
                        await this.writeConflictManifest(record, {
                            localProjectionReason: 'binary-conflict-remote-preview',
                        });
                        await this.stateStore.save(this.requireState());
                    }
                    await this.journalAction(
                        'pull', filePath, local, remote, remote.hash,
                        () => this.writeLocalSnapshot(remote, local),
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
                            local.localVariants?.diskHash,
                            created.entityId,
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
                    if (remote.text === undefined || local.text === undefined || !remote.entityId) {
                        throw new Error(`Document is not valid UTF-8: ${filePath}`);
                    }
                    const actual = await this.acceptPendingDocumentOperation(
                        filePath,
                        remote.text,
                        local.text,
                        remote.entityId,
                    );
                    const converged = await this.convergeTextToLocal(
                        filePath,
                        actual,
                        local.text,
                        local.hash,
                        local.localVariants?.diskHash,
                        remote.entityId,
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
            undefined,
            remote?.entityId,
            remote?.fileType === 'doc' && remote.text !== undefined && local.text !== undefined
                ? {ancestor: remote.text, target: local.text}
                : undefined,
        );
    }

    /** Retain the exact remote entity instead of destructively deleting it. */
    private async removeRemoteSnapshot(remote: ReplicaSnapshot): Promise<void> {
        if (!remote.entityId) {
            throw new Error(`Remote delete lacks an entity id for ${remote.path}`);
        }
        const uri = this.remoteUri(remote.path);
        switch (remote.fileType) {
            case 'doc':
                await this.vfs.removeDocumentAgainstBaseline(
                    uri,
                    remote.entityId,
                    remote.hash,
                );
                return;
            case 'file':
                await this.vfs.removeResourceAgainstBaseline(
                    uri,
                    remote.entityId,
                    remote.hash,
                );
                return;
            case 'folder':
                if (!remote.remoteStructureHash) {
                    throw new Error(`Remote folder delete lacks a structure hash for ${remote.path}`);
                }
                await this.vfs.removeFolderAgainstBaseline(
                    uri,
                    remote.entityId,
                    remote.remoteStructureHash,
                );
        }
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
        documentOperation?: {ancestor: string; target: string},
    ): Promise<void> {
        const state = this.requireState();
        const actionId = randomUUID();
        let durableDocumentOperation: Pick<
            PendingReplicaAction,
            | 'documentOperationId'
            | 'documentOperationPhase'
            | 'documentAncestorHash'
            | 'documentRequestedTargetHash'
        > = {};
        if (documentOperation) {
            const [ancestorHash, requestedTargetHash] = await Promise.all([
                this.stateStore.saveBaseline(new TextEncoder().encode(documentOperation.ancestor)),
                this.stateStore.saveBaseline(new TextEncoder().encode(documentOperation.target)),
            ]);
            durableDocumentOperation = {
                documentOperationId: `${state.replicaId}:${actionId}`,
                documentOperationPhase: 'prepared',
                documentAncestorHash: ancestorHash,
                documentRequestedTargetHash: requestedTargetHash,
            };
        }
        state.pendingAction = {
            id: actionId,
            kind,
            path: filePath,
            targetPath,
            startedAt: new Date().toISOString(),
            expectedLocalHash: local?.hash,
            expectedLocalTreeHash: local?.treeHash,
            expectedLocalIdentity: local?.localIdentity,
            expectedRemoteHash: remote?.hash,
            expectedRemoteTreeHash: remote?.treeHash,
            expectedRemoteStructureHash: remote?.remoteStructureHash,
            targetHash,
            entityId: entityId ?? remote?.entityId,
            ...durableDocumentOperation,
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
                && (
                    kind === 'pull'
                    || kind === 'converge-local'
                    || kind === 'resolve-conflict'
                )
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

    private async acceptPendingDocumentOperation(
        filePath: string,
        ancestor: string,
        target: string,
        expectedEntityId: string,
    ): Promise<string> {
        const state = this.requireState();
        const pending = state.pendingAction;
        if (
            !pending
            || !pending.documentOperationId
            || pending.documentOperationPhase !== 'prepared'
            || pending.entityId !== expectedEntityId
            || pending.documentAncestorHash !== contentHash(new TextEncoder().encode(ancestor))
            || pending.documentRequestedTargetHash !== contentHash(new TextEncoder().encode(target))
        ) {
            throw new Error(`Document operation was not durably prepared for ${filePath}`);
        }
        const accepted = await this.vfs.acceptDocumentSnapshotAgainstAncestor(
            this.remoteUri(filePath),
            ancestor,
            target,
            true,
            expectedEntityId,
            pending.documentOperationId,
        );
        void accepted.acknowledgement.catch(() => {});
        await this.recordPendingDocumentAcceptance(pending, accepted.opId, accepted.content);
        const acknowledged = await accepted.acknowledgement;
        pending.targetHash = contentHash(new TextEncoder().encode(acknowledged));
        if (accepted.opId !== undefined) {
            pending.documentOperationPhase = 'settled';
        }
        await this.stateStore.save(state);
        return acknowledged;
    }

    private async recordPendingDocumentAcceptance(
        pending: PendingReplicaAction,
        acceptedOperationId: string | undefined,
        visibleContent: string,
    ): Promise<void> {
        const state = this.requireState();
        if (state.pendingAction?.id !== pending.id) {
            throw new Error(`Pending document action changed while accepting ${pending.path}`);
        }
        if (acceptedOperationId === undefined) {
            this.clearPendingDocumentOperation(pending);
        } else if (acceptedOperationId === pending.documentOperationId) {
            pending.documentOperationPhase = 'accepted';
        } else {
            throw new Error(`Document operation id changed while accepting ${pending.path}`);
        }
        pending.targetHash = contentHash(new TextEncoder().encode(visibleContent));
        await this.stateStore.save(state);
    }

    private async preparePendingDocumentOperation(
        filePath: string,
        ancestor: string,
        target: string,
        expectedEntityId: string,
    ): Promise<void> {
        const state = this.requireState();
        const pending = state.pendingAction;
        if (!pending || (pending.documentOperationPhase && pending.documentOperationPhase !== 'settled')) {
            throw new Error(`Cannot prepare another document operation for ${filePath}`);
        }
        const [ancestorHash, requestedTargetHash] = await Promise.all([
            this.stateStore.saveBaseline(new TextEncoder().encode(ancestor)),
            this.stateStore.saveBaseline(new TextEncoder().encode(target)),
        ]);
        pending.entityId = expectedEntityId;
        pending.documentOperationId = `${state.replicaId}:${pending.id}:${randomUUID()}`;
        pending.documentOperationPhase = 'prepared';
        pending.documentAncestorHash = ancestorHash;
        pending.documentRequestedTargetHash = requestedTargetHash;
        await this.stateStore.save(state);
    }

    private clearPendingDocumentOperation(pending: PendingReplicaAction): void {
        pending.documentOperationId = undefined;
        pending.documentOperationPhase = undefined;
        pending.documentAncestorHash = undefined;
        pending.documentRequestedTargetHash = undefined;
    }

    private async recoverPendingDocumentOperation(
        pending: PendingReplicaAction,
    ): Promise<'none' | 'advanced' | 'waiting'> {
        if (!pending.documentOperationId || !pending.documentOperationPhase) {
            return 'none';
        }
        if (pending.documentOperationPhase === 'settled') {
            return 'none';
        }
        if (!pending.entityId) {
            throw new Error(`Pending document operation has no immutable entity id: ${pending.path}`);
        }
        const inspection = await this.vfs.inspectDocumentOperation(
            this.remoteUri(pending.path),
            pending.entityId,
            pending.documentOperationId,
        );
        if (inspection.disposition === 'unknown') {
            if (pending.documentOperationPhase !== 'prepared') {
                throw new Error(
                    `Cannot prove disposition of accepted document operation `
                    + `${pending.documentOperationId}`,
                );
            }
            if (!pending.documentAncestorHash || !pending.documentRequestedTargetHash) {
                throw new Error(`Pending document replay inputs are incomplete: ${pending.path}`);
            }
            const [ancestorBytes, targetBytes] = await Promise.all([
                this.stateStore.readBaseline(pending.documentAncestorHash),
                this.stateStore.readBaseline(pending.documentRequestedTargetHash),
            ]);
            if (!ancestorBytes || !targetBytes) {
                throw new Error(`Pending document replay inputs are missing: ${pending.path}`);
            }
            const decoder = new TextDecoder('utf-8', {fatal: true});
            const ancestor = decoder.decode(ancestorBytes);
            const target = decoder.decode(targetBytes);
            const accepted = await this.vfs.acceptDocumentSnapshotAgainstAncestor(
                this.remoteUri(pending.path),
                ancestor,
                target,
                true,
                pending.entityId,
                pending.documentOperationId,
            );
            void accepted.acknowledgement.catch(() => {});
            await this.recordPendingDocumentAcceptance(pending, accepted.opId, accepted.content);
            return accepted.opId === undefined ? 'advanced' : 'waiting';
        }
        pending.targetHash = contentHash(new TextEncoder().encode(inspection.content));
        pending.documentOperationPhase = inspection.disposition === 'settled'
            ? 'settled'
            : 'accepted';
        await this.stateStore.save(this.requireState());
        return inspection.disposition === 'settled' ? 'advanced' : 'waiting';
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
            case 'move-local': {
                const localTarget = pending.targetPath === undefined
                    ? undefined
                    : scan.local.get(pending.targetPath);
                const remoteTarget = pending.targetPath === undefined
                    ? undefined
                    : scan.remote.get(pending.targetPath);
                const localOwnershipMatches = pending.expectedLocalIdentity !== undefined
                    ? localTarget?.localIdentity === pending.expectedLocalIdentity
                    : localTarget !== undefined
                        && (
                            pending.expectedLocalHash === undefined
                            || localTarget.hash === pending.expectedLocalHash
                        )
                        && (
                            pending.expectedLocalTreeHash === undefined
                            || localTarget.treeHash === pending.expectedLocalTreeHash
                        );
                return pending.targetPath !== undefined
                    && localTarget !== undefined
                    && localOwnershipMatches
                    && remoteTarget?.entityId === pending.entityId
                    && (
                        pending.expectedLocalIdentity === undefined
                        || local?.localIdentity !== pending.expectedLocalIdentity
                    )
                    && remote?.entityId !== pending.entityId;
            }
            case 'move-remote': {
                const localTarget = pending.targetPath === undefined
                    ? undefined
                    : scan.local.get(pending.targetPath);
                const remoteTarget = pending.targetPath === undefined
                    ? undefined
                    : scan.remote.get(pending.targetPath);
                const localOwnershipMatches = pending.expectedLocalIdentity !== undefined
                    ? localTarget?.localIdentity === pending.expectedLocalIdentity
                    : localTarget !== undefined
                        && (
                            pending.expectedLocalHash === undefined
                            || localTarget.hash === pending.expectedLocalHash
                        )
                        && (
                            pending.expectedLocalTreeHash === undefined
                            || localTarget.treeHash === pending.expectedLocalTreeHash
                        );
                return pending.targetPath !== undefined
                    && localOwnershipMatches
                    && remoteTarget?.entityId === pending.entityId
                    && (
                        pending.expectedLocalIdentity === undefined
                        || local?.localIdentity !== pending.expectedLocalIdentity
                    )
                    && remote?.entityId !== pending.entityId;
            }
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
        const local = scan.local.get(pending.path);
        const remote = scan.remote.get(pending.path);
        if (pending.kind === 'delete-local') {
            if (local && local.hash === pending.expectedLocalHash) {
                await this.deleteLocalSnapshot(pending.path, local, scan.local);
                return true;
            }
            return false;
        }
        if (pending.kind === 'delete-remote') {
            if (
                remote
                && remote.hash === pending.expectedRemoteHash
                && (
                    pending.expectedRemoteTreeHash === undefined
                    || remote.treeHash === pending.expectedRemoteTreeHash
                )
                && (
                    pending.expectedRemoteStructureHash === undefined
                    || remote.remoteStructureHash === pending.expectedRemoteStructureHash
                )
                && remote.entityId === pending.entityId
                && !remote.readonly
            ) {
                await this.removeRemoteSnapshot(remote);
                return true;
            }
            return false;
        }
        if (pending.kind === 'move-local') {
            const target = pending.targetPath === undefined
                ? undefined
                : scan.local.get(pending.targetPath);
            const remoteTarget = pending.targetPath === undefined
                ? undefined
                : scan.remote.get(pending.targetPath);
            const sourceOwnershipMatches = pending.expectedLocalIdentity !== undefined
                ? local?.localIdentity === pending.expectedLocalIdentity
                : local?.hash === pending.expectedLocalHash
                    && (
                        pending.expectedLocalTreeHash === undefined
                        || local?.treeHash === pending.expectedLocalTreeHash
                    );
            if (
                pending.targetPath
                && local !== undefined
                && remote === undefined
                && sourceOwnershipMatches
                && target === undefined
                && remoteTarget?.entityId === pending.entityId
            ) {
                await this.ensureLocalParent(pending.targetPath);
                await this.moveLocalSnapshotAgainstBaseline(
                    pending.path,
                    pending.targetPath,
                    local,
                );
                return true;
            }
            return false;
        }
        if (pending.kind === 'move-remote') {
            const target = pending.targetPath === undefined
                ? undefined
                : scan.remote.get(pending.targetPath);
            const localTarget = pending.targetPath === undefined
                ? undefined
                : scan.local.get(pending.targetPath);
            const localOwnershipMatches = pending.expectedLocalIdentity !== undefined
                ? localTarget?.localIdentity === pending.expectedLocalIdentity
                : localTarget !== undefined
                    && (
                        pending.expectedLocalHash === undefined
                        || localTarget.hash === pending.expectedLocalHash
                    )
                    && (
                        pending.expectedLocalTreeHash === undefined
                        || localTarget.treeHash === pending.expectedLocalTreeHash
                    );
            if (
                pending.targetPath
                && remote !== undefined
                && remote.entityId === pending.entityId
                && local === undefined
                && localOwnershipMatches
                && target === undefined
                && !remote.readonly
            ) {
                await this.moveRemoteSnapshotAgainstBaseline(
                    pending.path,
                    pending.targetPath,
                    remote,
                );
                return true;
            }
            return false;
        }
        if (
            pending.kind === 'resolve-conflict'
            && local
            && !local.localVariants
            && local.hash === pending.targetHash
            && (
                remote === undefined
                || (
                    remote.entityId === pending.entityId
                    && remote.hash === pending.expectedRemoteHash
                )
            )
        ) {
            await this.applyResolvedLocalSnapshot(local, remote);
            return true;
        }
        if (pending.kind !== 'push' && pending.kind !== 'pull' && pending.kind !== 'resolve-conflict') {
            return false;
        }
        if (
            remote
            && remote.hash === pending.targetHash
            && local?.hash === pending.expectedLocalHash
        ) {
            await this.writeLocalSnapshot(remote, local);
            return true;
        }
        if (
            remote
            && remote.hash === pending.targetHash
            && !local
            && pending.expectedLocalHash === undefined
        ) {
            await this.writeLocalSnapshot(remote);
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

    private async refreshConvergedState(
        state: ReplicaState,
        scan: ScanPair,
        resolvedConflictPath?: string,
    ): Promise<void> {
        const protectedDocumentEntityIds = new Set(
            (await this.vfs.inspectPendingDocumentRecoveries())
                .map(recovery => recovery.documentId),
        );
        const baselinesAdvancedDuringScan = new Map<string, BaselineEntry | undefined>();
        if (scan.baselineAtStart) {
            const candidatePaths = new Set([
                ...scan.baselineAtStart.keys(),
                ...Object.keys(state.entries),
            ]);
            for (const filePath of candidatePaths) {
                const current = state.entries[filePath];
                if (!sameBaselineEntry(scan.baselineAtStart.get(filePath), current)) {
                    baselinesAdvancedDuringScan.set(
                        filePath,
                        current === undefined ? undefined : {...current},
                    );
                }
            }
        }
        if (resolvedConflictPath === undefined) {
            refreshConvergedBaseline(
                state,
                scan.local,
                scan.remote,
                protectedDocumentEntityIds,
            );
        } else {
            // Baseline calculation is synchronous. Hide the resolved record
            // only from that calculation, never from concurrent gateways or
            // recovery observers while durable snapshots are being written.
            const record = state.conflicts[resolvedConflictPath];
            delete state.conflicts[resolvedConflictPath];
            try {
                refreshConvergedBaseline(
                    state,
                    scan.local,
                    scan.remote,
                    protectedDocumentEntityIds,
                );
            } finally {
                if (record) {
                    state.conflicts[resolvedConflictPath] = record;
                }
            }
        }
        for (const [filePath, current] of baselinesAdvancedDuringScan) {
            if (current === undefined) {
                delete state.entries[filePath];
            } else {
                state.entries[filePath] = current;
            }
            this.traceBaselineWrite('preserve-newer-than-scan', filePath, current);
        }
        for (const [filePath, baseline] of Object.entries(state.entries)) {
            // Document baselines already retain their exact UTF-16 text in
            // state.entries. Only binary baselines need a separate
            // content-addressed blob for later conflict recovery.
            if (baseline.fileType !== 'file') {
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

    /** Preserve every common-base descendant when a tree changes only its path. */
    private relocateBaselineSubtree(
        state: ReplicaState,
        oldPath: string,
        newPath: string,
    ): void {
        if (oldPath === newPath) {
            return;
        }
        const moving = Object.entries(state.entries)
            .filter(([candidate]) => (
                candidate === oldPath || candidate.startsWith(`${oldPath}/`)
            ));
        if (moving.length === 0) {
            return;
        }
        const movingPaths = new Set(moving.map(([candidate]) => candidate));
        const relocated = moving.map(([candidate, entry]) => {
            const suffix = candidate.slice(oldPath.length);
            return [`${newPath}${suffix}`, entry] as const;
        });
        for (const [candidate] of relocated) {
            if (state.entries[candidate] && !movingPaths.has(candidate)) {
                throw new Error(`Cannot relocate baseline onto existing path ${candidate}`);
            }
        }
        for (const [candidate] of moving) {
            delete state.entries[candidate];
        }
        for (const [candidate, entry] of relocated) {
            state.entries[candidate] = entry;
        }
    }

    private async scanBoth(): Promise<ScanPair> {
        const projectionEpoch = this.localProjectionCas.observationEpoch;
        const baselineAtStart = new Map(
            Object.entries(this.state?.entries ?? {})
                .map(([filePath, entry]) => [filePath, {...entry}] as const),
        );
        const iteration = this.activeReconcile?.iteration ?? 0;
        this.markReconcilePhase('scan:init', iteration);
        await this.vfs.init();
        this.markReconcilePhase('scan:remote', iteration);
        const remote = await this.scanRemote();
        this.markReconcilePhase('scan:local', iteration);
        const local = await this.scanLocal(remote);
        return { local, remote, projectionEpoch, baselineAtStart };
    }

    /** The remote transaction namespace is never projected into a local replica. */
    private async assertNoLocalReservedNamespace(): Promise<void> {
        const entries = await vscode.workspace.fs.readDirectory(this.baseUri);
        const reserved = entries.find(([name]) => (
            name === REMOTE_SAFE_SYNC_DIRECTORY
            || name.startsWith(`${REMOTE_SAFE_SYNC_DIRECTORY}.staging-`)
        ));
        if (!reserved) {
            return;
        }
        throw new Error(
            `Local path ${reserved[0]} is reserved for remote safe-sync recovery; `
            + 'move or rename it before opening this replica',
        );
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
            const snapshot = this.state && isPathConflicted(filePath, this.state)
                ? await this.vfs.readAuthoritativeFileWithoutRecovery(this.remoteUri(filePath))
                : await this.vfs.readAuthoritativeFile(this.remoteUri(filePath));
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
        this.attachRemoteStructureHashes(result);
        return result;
    }

    /** Match the path/type-only folder hash used by authoritative move verification. */
    private attachRemoteStructureHashes(entries: Map<string, ReplicaSnapshot>): void {
        const paths = [...entries.keys()].sort();
        for (const [folderPath, folder] of entries) {
            if (folder.fileType !== 'folder') {
                continue;
            }
            const prefix = `${folderPath}/`;
            const tree = paths
                .filter(candidate => candidate.startsWith(prefix))
                .map(candidate => ({
                    path: candidate.slice(prefix.length),
                    type: entries.get(candidate)!.fileType,
                }));
            folder.remoteStructureHash = contentHash(
                new TextEncoder().encode(JSON.stringify(tree)),
            );
        }
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
                if ((type & vscode.FileType.File) === 0
                    && (type & vscode.FileType.Directory) === 0) {
                    throw new Error(`Unsupported local filesystem entity: ${filePath}`);
                }
                const observed = await this.observeLocalEntry(
                    uri,
                    (type & vscode.FileType.Directory) !== 0,
                );
                if (!observed) {
                    // The pathname vanished between the directory listing and
                    // this read. Projection is not mutation: the disk
                    // compare-and-swap displaces a pathname while installing
                    // its bytes, and a real delete is observed by the watcher
                    // that owns it. Failing the scan here would only re-read
                    // the same window at a hotter rate.
                    continue;
                }
                const identity = observed.identity;
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
                const openDocument = openDocuments.get(uri.fsPath);
                const diskContent = observed.content!;
                // During an explicit conflict choice, the type the user chose is
                // the canonical one. Keeping the old baseline type here would
                // make a file->document replacement impossible to prove
                // converged: the bytes may match forever while the stale
                // classification keeps reporting a type mismatch. A choice of
                // the remote entity is the mirror image of that, so the local
                // variant's type must not survive it either.
                const pendingResolutionType = this.state?.pendingAction?.kind === 'resolve-conflict'
                    && this.state.pendingAction.path === filePath
                    && this.resolvingResolutions?.get(filePath) !== 'remote'
                    ? this.state.conflicts[filePath]?.localType
                    : undefined;
                const resolvingType = this.resolutionTypeOverrides?.get(filePath)
                    ?? pendingResolutionType
                    ?? (this.isPathResolving(filePath)
                        ? remote.get(filePath)?.fileType
                        : undefined);
                const knownType = resolvingType
                    ?? this.state?.entries[filePath]?.fileType
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

    /**
     * Read one listed entry's identity and bytes as a single observation.
     * A pathname that is missing on both attempts is absent for this scan;
     * one that reappears was mid-projection, not deleted.
     */
    private async observeLocalEntry(
        uri: vscode.Uri,
        directory: boolean,
    ): Promise<{identity: string; content?: Uint8Array} | undefined> {
        for (let attempt = 0; attempt < 2; attempt++) {
            if (attempt > 0) {
                await new Promise(resolve => setTimeout(resolve, PROJECTION_OBSERVATION_RETRY_MS));
            }
            try {
                const identity = await this.localIdentity(uri);
                return directory
                    ? {identity}
                    : {identity, content: await vscode.workspace.fs.readFile(uri)};
            } catch (error) {
                if (!(error instanceof Error) || !this.isMissingPathFailure(error)) {
                    throw error;
                }
            }
        }
        return undefined;
    }

    private async localIdentity(uri: vscode.Uri): Promise<string> {
        const stat = await lstat(uri.fsPath);
        return `${stat.dev}:${stat.ino}`;
    }

    private async moveLocalSnapshotAgainstBaseline(
        oldPath: string,
        newPath: string,
        expected: ReplicaSnapshot | undefined,
    ): Promise<void> {
        if (!expected || expected.path !== oldPath || expected.localVariants) {
            throw new StaleLocalSnapshotError(`Local move has no stable source: ${oldPath}`);
        }
        const before = await this.scanLocal(this.lastScan?.remote ?? new Map());
        const source = before.get(oldPath);
        if (
            !source
            || before.has(newPath)
            || source.fileType !== expected.fileType
            || source.hash !== expected.hash
            || source.localIdentity !== expected.localIdentity
            || source.treeHash !== expected.treeHash
        ) {
            throw new StaleLocalSnapshotError(`Local move source changed: ${oldPath}`);
        }
        const oldUri = this.localUri(oldPath);
        const newUri = this.localUri(newPath);
        const oldSubtreePrefix = `${oldUri.fsPath}${path.sep}`;
        const openDocument = vscode.workspace.textDocuments?.find(document => (
            document.uri.fsPath === oldUri.fsPath
            || document.uri.fsPath.startsWith(oldSubtreePrefix)
        ));
        if (openDocument) {
            const edit = new vscode.WorkspaceEdit();
            edit.renameFile(oldUri, newUri, {overwrite: false, ignoreIfExists: false});
            if (!await vscode.workspace.applyEdit(edit)) {
                throw new StaleLocalSnapshotError(`VS Code refused local move: ${oldPath}`);
            }
        } else {
            await vscode.workspace.fs.rename(oldUri, newUri, {overwrite: false});
        }
        const after = await this.scanLocal(this.lastScan?.remote ?? new Map());
        const target = after.get(newPath);
        if (
            after.has(oldPath)
            || !target
            || target.fileType !== expected.fileType
            || target.hash !== expected.hash
            || target.localIdentity !== expected.localIdentity
            || target.treeHash !== expected.treeHash
        ) {
            throw new StaleLocalSnapshotError(`Local move target changed: ${newPath}`);
        }
    }

    private async moveRemoteSnapshotAgainstBaseline(
        oldPath: string,
        newPath: string,
        expected: ReplicaSnapshot | undefined,
    ): Promise<void> {
        if (!expected?.entityId || expected.path !== oldPath || expected.readonly) {
            throw new ConcurrentRemoteResourceError(this.remoteUri(oldPath).path);
        }
        const expectedHash = expected.fileType === 'folder'
            ? expected.remoteStructureHash
            : expected.hash;
        if (!expectedHash) {
            throw new Error(`Remote move lacks an authoritative hash for ${oldPath}`);
        }
        await this.vfs.moveResourceAgainstBaseline(
            this.remoteUri(oldPath),
            this.remoteUri(newPath),
            expected.entityId,
            expected.fileType,
            expectedHash,
        );
    }

    private async writeLocalSnapshot(
        remote: ReplicaSnapshot,
        expectedLocal?: ReplicaSnapshot,
    ): Promise<void> {
        if (remote.fileType === 'folder') {
            if (expectedLocal === undefined) {
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
            expectedLocal?.hash,
            remote.fileType === 'doc',
            expectedLocal?.localVariants?.diskHash,
        );
    }

    private async writeLocalContent(
        filePath: string,
        content: Uint8Array,
        expectedLocalHash?: string,
        asText = false,
        expectedDiskHash = expectedLocalHash,
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
                openDocument ? 'editor' : 'disk',
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
        const diskBefore = await this.readCurrentLocalBytes(uri);
        if (
            (diskBefore === undefined) !== (expectedDiskHash === undefined)
            || (
                diskBefore !== undefined
                && contentHash(diskBefore) !== expectedDiskHash
            )
        ) {
            throw new StaleLocalSnapshotError(
                `Disk content changed during scan: ${filePath}`,
                diskBefore,
                'disk',
            );
        }
        let editorProjected = false;
        if (openDocument) {
            const expectedText = new TextDecoder().decode(current!);
            const targetText = new TextDecoder().decode(content);
            if (!await this.projectEditorCas(filePath, expectedText, targetText)) {
                throw new StaleLocalSnapshotError(
                    `VS Code changed while projecting synchronized content for ${filePath}`,
                    new TextEncoder().encode(openDocument.getText()),
                    'editor',
                );
            }
            editorProjected = expectedText !== targetText;
        }
        // Projection is not mutation: materialize the same proven bytes with a
        // filesystem CAS, without making TextDocument.save() part of the
        // remote transaction.
        const projection = await this.localProjectionCas.replace(
            filePath,
            expectedDiskHash,
            content,
        );
        if (projection.recoveryCandidate) {
            if (editorProjected) {
                await this.projectEditorCas(
                    filePath,
                    new TextDecoder().decode(content),
                    new TextDecoder().decode(current!),
                );
            }
            await this.registerProjectionRecoveryConflicts([
                projection.recoveryCandidate,
            ]);
            throw new LocalProjectionConflictError(filePath);
        }
        const projectionAlreadyConverged = projection.actual !== undefined
            && contentHash(projection.actual) === contentHash(content);
        if (!projection.applied && !projectionAlreadyConverged) {
            // This projection did not happen. Leaving the editor on the target
            // text would split the editor and disk branches while the caller
            // re-derives its expected hash from only one of them, so the same
            // pair would keep failing instead of converging. The rollback is
            // itself a compare-and-swap: a keystroke that arrived during the
            // disk transaction wins and is reported as the local endpoint.
            if (editorProjected && !await this.projectEditorCas(
                filePath,
                new TextDecoder().decode(content),
                new TextDecoder().decode(current!),
            )) {
                throw new StaleLocalSnapshotError(
                    `VS Code changed while projecting synchronized content for ${filePath}`,
                    new TextEncoder().encode(openDocument!.getText()),
                    'editor',
                );
            }
            throw new StaleLocalSnapshotError(
                `Local content changed while projecting synchronized content: ${filePath}`,
                projection.actual,
                'disk',
            );
        }
        const diskAfter = await vscode.workspace.fs.readFile(uri);
        const visibleAfter = openDocument
            ? new TextEncoder().encode(openDocument.getText())
            : diskAfter;
        if (
            contentHash(diskAfter) !== contentHash(content)
            || contentHash(visibleAfter) !== contentHash(content)
        ) {
            const diskChanged = contentHash(diskAfter) !== contentHash(content);
            throw new StaleLocalSnapshotError(
                `Local content changed while saving synchronized text: ${filePath}`,
                diskChanged ? diskAfter : visibleAfter,
                diskChanged ? 'disk' : 'editor',
                content,
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

        if (!await this.projectEditorCas(
            filePath,
            openDocument.getText(),
            new TextDecoder().decode(content),
        )) {
            throw new StaleLocalSnapshotError(
                `VS Code changed while projecting merged content for ${filePath}`,
                new TextEncoder().encode(openDocument.getText()),
            );
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
        const projection = await this.localProjectionCas.replace(
            filePath,
            variants.diskHash,
            content,
        );
        if (projection.recoveryCandidate) {
            await this.registerProjectionRecoveryConflicts([
                projection.recoveryCandidate,
            ]);
            throw new LocalProjectionConflictError(filePath);
        }
        if (!projection.applied) {
            throw new StaleLocalSnapshotError(
                `Disk content changed while projecting merged content: ${filePath}`,
                projection.actual,
            );
        }
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
        expectedDiskHash = expectedLocalHash,
        expectedRemoteEntityId?: string,
    ): Promise<string> {
        let remoteText = authoritativeText;
        let localAncestor = expectedLocalText;
        let localHash = expectedLocalHash;
        let diskAncestor = expectedDiskHash === expectedLocalHash
            ? expectedLocalText
            : undefined;
        for (let attempt = 0; attempt < 10; attempt++) {
            try {
                await this.writeLocalContent(
                    filePath,
                    new TextEncoder().encode(remoteText),
                    localHash,
                    true,
                    expectedDiskHash,
                );
                return remoteText;
            } catch (error) {
                if (!(error instanceof StaleLocalSnapshotError) || !error.actualContent) {
                    throw error;
                }
                const newerLocalText = new TextDecoder('utf-8', { fatal: true })
                    .decode(error.actualContent);
                if (error.diskProjectionBase) {
                    const projectedBase = new TextDecoder('utf-8', {fatal: true})
                        .decode(error.diskProjectionBase);
                    const projectedHash = contentHash(error.diskProjectionBase);
                    // Both local endpoints reached this plugin projection
                    // before the reported later mutation. Retrying from the
                    // older scan ancestor would apply the remote delta twice.
                    diskAncestor = projectedBase;
                    expectedDiskHash = projectedHash;
                    localAncestor = projectedBase;
                    localHash = projectedHash;
                }
                const endpoint = error.endpoint ?? 'editor';
                const ancestor = endpoint === 'disk' ? diskAncestor : localAncestor;
                if (ancestor === undefined) {
                    throw new Error(
                        `Cannot prove ${endpoint} ancestry while converging ${filePath}`,
                    );
                }
                const merged = mergeConcurrentText(ancestor, newerLocalText, remoteText);
                if (!expectedRemoteEntityId) {
                    throw new Error(`Cannot identify remote document while converging ${filePath}`);
                }
                await this.preparePendingDocumentOperation(
                    filePath,
                    remoteText,
                    merged,
                    expectedRemoteEntityId,
                );
                remoteText = await this.acceptPendingDocumentOperation(
                    filePath,
                    remoteText,
                    merged,
                    expectedRemoteEntityId,
                );
                if (endpoint === 'disk') {
                    diskAncestor = newerLocalText;
                    expectedDiskHash = contentHash(error.actualContent);
                    const editorOpen = vscode.workspace.textDocuments.some(
                        document => document.uri.fsPath === this.localUri(filePath).fsPath,
                    );
                    if (!editorOpen) {
                        localAncestor = newerLocalText;
                        localHash = expectedDiskHash;
                    }
                } else {
                    localAncestor = newerLocalText;
                    localHash = contentHash(error.actualContent);
                }
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
        this.traceBaselineWrite(
            `commit-converged:${state.pendingAction.kind}`, filePath, state.entries[filePath],
        );
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
        expectedTargets?: ReadonlyMap<string, ReplicaSnapshot>,
    ): Promise<void> {
        if (!expected) {
            return;
        }
        if (expected.fileType === 'folder') {
            if (
                !expectedTargets
                && (expected.treeHash === undefined || expected.localIdentity === undefined)
            ) {
                throw new StaleLocalSnapshotError(
                    `Cannot prove local directory ownership before deletion: ${filePath}`,
                );
            }
            // A folder's resource hash is intentionally constant. A recursive
            // delete based on that value would erase children created after the
            // scan. Re-read the subtree and require both the observed tree hash
            // and every owned member to match before deleting anything.
            const targets = expectedTargets
                ?? await this.scanLocal(this.lastScan?.remote ?? new Map());
            const expectedRoot = targets.get(filePath);
            if (
                !expectedRoot
                || expectedRoot.fileType !== 'folder'
                || expectedRoot.hash !== expected.hash
                || (
                    expected.treeHash !== undefined
                    && expectedRoot.treeHash !== expected.treeHash
                )
            ) {
                throw new StaleLocalSnapshotError(
                    `Local directory ownership changed before deletion: ${filePath}`,
                );
            }

            const current = await this.scanLocal(this.lastScan?.remote ?? new Map());
            const actualRoot = current.get(filePath);
            if (
                !actualRoot
                || actualRoot.fileType !== 'folder'
                || (
                    expected.treeHash !== undefined
                    && actualRoot.treeHash !== expected.treeHash
                )
                || (
                    expected.localIdentity !== undefined
                    && actualRoot.localIdentity !== expected.localIdentity
                )
            ) {
                throw new StaleLocalSnapshotError(
                    `Local directory changed before deletion: ${filePath}`,
                );
            }
            await this.deleteOwnedLocalTarget(filePath, expected, targets);
            return;
        }
        const uri = this.localUri(filePath);
        const openDocument = vscode.workspace?.textDocuments?.find(
            document => document.uri.fsPath === uri.fsPath,
        );
        // scanLocal treats a dirty text buffer as the logical local endpoint.
        // The deletion CAS must inspect that same endpoint; comparing its hash
        // to the older on-disk projection makes an explicit remote choice
        // impossible to complete even though the archived editor candidate is
        // unchanged.
        const current = await this.readCurrentLocalBytes(uri, openDocument);
        if (!current || contentHash(current) !== expected.hash) {
            throw new StaleLocalSnapshotError(`Local content changed before deletion: ${filePath}`);
        }
        if (expected.localIdentity !== undefined) {
            let actualIdentity: string;
            try {
                actualIdentity = await this.localIdentity(uri);
            } catch {
                throw new StaleLocalSnapshotError(`Local content disappeared before deletion: ${filePath}`);
            }
            if (actualIdentity !== expected.localIdentity) {
                throw new StaleLocalSnapshotError(`Local identity changed before deletion: ${filePath}`);
            }
        }
        const expectedDiskHash = expected.localVariants?.diskHash ?? expected.hash;
        const deletion = await this.localProjectionCas.remove(filePath, expectedDiskHash);
        if (deletion.recoveryCandidate) {
            await this.registerProjectionRecoveryConflicts([
                deletion.recoveryCandidate,
            ]);
            throw new LocalProjectionConflictError(filePath);
        }
        if (!deletion.applied) {
            throw new StaleLocalSnapshotError(
                `Local content changed while deleting synchronized content: ${filePath}`,
                deletion.actual,
                'disk',
            );
        }
    }

    private topLevelSnapshots(snapshots: ReplicaSnapshot[]): ReplicaSnapshot[] {
        const paths = new Set(snapshots.map(snapshot => snapshot.path));
        return snapshots.filter(snapshot => {
            let parent = this.parentPath(snapshot.path);
            while (parent) {
                if (paths.has(parent)) {
                    return false;
                }
                parent = this.parentPath(parent);
            }
            return true;
        });
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

    /**
     * A missing local path can be a delayed projection or watcher event.
     * Require a second scan of the same remote entity before a local absence
     * becomes a destructive remote-delete intent.
     */
    private isStableLocalDeletion(
        filePath: string,
        baseline: BaselineEntry | undefined,
        remote: ReplicaSnapshot | undefined,
    ): boolean {
        if (
            !baseline
            || !remote
            || remote.entityId === undefined
            || remote.entityId !== baseline.entityId
            || remote.hash !== baseline.hash
            || (
                baseline.fileType === 'folder'
                && baseline.treeHash !== remote.treeHash
                && remote.remoteStructureHash !== EMPTY_REMOTE_FOLDER_TREE_HASH
            )
        ) {
            return false;
        }
        const candidate = {
            remoteEntityId: remote.entityId,
            remoteHash: remote.hash,
            remoteTreeHash: remote.treeHash,
        };
        const candidates = this.deletionCandidates();
        const previous = candidates.get(filePath);
        const sameObservation = previous !== undefined
            && previous.remoteEntityId === candidate.remoteEntityId
            && previous.remoteHash === candidate.remoteHash
            && previous.remoteTreeHash === candidate.remoteTreeHash;
        if (!sameObservation) {
            candidates.set(filePath, {
                ...candidate,
                since: Date.now(),
            });
            this.scheduleReconcile(
                `confirm local deletion: ${filePath}`,
                LOCAL_DELETE_STABILITY_MS,
            );
            return false;
        }
        if (Date.now() - previous.since < LOCAL_DELETE_STABILITY_MS) {
            this.scheduleReconcile(
                `confirm local deletion: ${filePath}`,
                LOCAL_DELETE_STABILITY_MS - (Date.now() - previous.since),
            );
            return false;
        }
        candidates.delete(filePath);
        return true;
    }

    /**
     * Whether a local pathname's absence has held still long enough to mean it.
     *
     * A pathname is briefly absent for reasons nobody authored: an atomic
     * projection displaces it while installing its bytes, and a tool that
     * rewrites a file unlinks the old inode before the replacement is linked
     * back. Reading the first such observation as a deletion turns a remote
     * edit that happened to arrive inside that window into a conflict over a
     * file that never left, and a conflict is not a state the next scan can
     * take back. Only an absence that survives the same confirmation window a
     * remote deletion must survive is a deletion.
     */
    private isStableLocalAbsence(filePath: string): boolean {
        const absences = this.localAbsenceObservations();
        const since = absences.get(filePath);
        if (since === undefined) {
            absences.set(filePath, Date.now());
            this.scheduleReconcile(
                `confirm local absence: ${filePath}`,
                LOCAL_DELETE_STABILITY_MS,
            );
            return false;
        }
        const waited = Date.now() - since;
        if (waited < LOCAL_DELETE_STABILITY_MS) {
            this.scheduleReconcile(
                `confirm local absence: ${filePath}`,
                LOCAL_DELETE_STABILITY_MS - waited,
            );
            return false;
        }
        absences.delete(filePath);
        return true;
    }

    private localAbsenceObservations(): Map<string, number> {
        if (!this.localAbsences) {
            this.localAbsences = new Map();
        }
        return this.localAbsences;
    }

    /**
     * Whether a remote pathname's absence has held still long enough to mean it.
     *
     * Replacing a remote entity at its own path is two updates, a delete and a
     * create, and between them the project tree really does hold nothing
     * there. That gap is not a deletion anybody performed: acting on it either
     * removes a local file its author never asked to remove, or freezes the
     * path in a conflict whose only remote evidence is the vacancy. This is
     * the mirror of [[isStableLocalAbsence]] and waits the same window.
     */
    private isStableRemoteAbsence(filePath: string): boolean {
        const absences = this.remoteAbsenceObservations();
        const since = absences.get(filePath);
        if (since === undefined) {
            absences.set(filePath, Date.now());
            this.scheduleReconcile(
                `confirm remote absence: ${filePath}`,
                REMOTE_DELETE_STABILITY_MS,
            );
            return false;
        }
        const waited = Date.now() - since;
        if (waited < REMOTE_DELETE_STABILITY_MS) {
            this.scheduleReconcile(
                `confirm remote absence: ${filePath}`,
                REMOTE_DELETE_STABILITY_MS - waited,
            );
            return false;
        }
        absences.delete(filePath);
        return true;
    }

    private remoteAbsenceObservations(): Map<string, number> {
        if (!this.remoteAbsences) {
            this.remoteAbsences = new Map();
        }
        return this.remoteAbsences;
    }

    private clearLocalDeletionCandidates(scan: ScanPair): void {
        const candidates = this.deletionCandidates();
        for (const filePath of candidates.keys()) {
            if (scan.local.has(filePath) || !scan.remote.has(filePath)) {
                candidates.delete(filePath);
            }
        }
        const absences = this.localAbsenceObservations();
        for (const filePath of absences.keys()) {
            if (scan.local.has(filePath) || !scan.remote.has(filePath)) {
                absences.delete(filePath);
            }
        }
        const remoteAbsences = this.remoteAbsenceObservations();
        for (const filePath of remoteAbsences.keys()) {
            if (scan.remote.has(filePath) || !scan.local.has(filePath)) {
                remoteAbsences.delete(filePath);
            }
        }
    }

    private deletionCandidates(): Map<string, {
        remoteEntityId: string;
        remoteHash: string;
        remoteTreeHash?: string;
        since: number;
    }> {
        if (!this.localDeletionCandidates) {
            this.localDeletionCandidates = new Map();
        }
        return this.localDeletionCandidates;
    }

    private async createConflict(filePath: string, reason: string, scan: ScanPair): Promise<void> {
        const state = this.requireState();
        if (state.conflicts[filePath]) {
            return;
        }
        const machine = this.textReplicas?.get(filePath);
        if (machine) {
            const frozen = await machine.freezeForConflict(reason);
            await this.createTextConflict(frozen);
            return;
        }
        const id = `${this.timestamp()}-${randomUUID()}`;
        const artifactDirectory = `.overleaf/conflicts/unresolved/${id}`;
        const localFile = await this.archiveSubtree(artifactDirectory, 'local', filePath, scan.local);
        let remoteFile = await this.archiveSubtree(artifactDirectory, 'remote', filePath, scan.remote);
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
        let remoteHash = remote?.hash;
        let remoteEntityId = remote?.entityId;
        if (remote?.fileType === 'doc') {
            try {
                const authoritative = await this.vfs.readAuthoritativeFileWithoutRecovery(
                    this.remoteUri(filePath),
                );
                if (
                    authoritative.fileType !== 'doc'
                    || authoritative.entityId !== remote.entityId
                    || authoritative.revision === undefined
                ) {
                    throw new ConcurrentRemoteResourceError(this.remoteUri(filePath).path);
                }
                remoteFile = `${artifactDirectory}/remote/${filePath}`;
                await this.writeArtifact(remoteFile, authoritative.content);
                remoteHash = contentHash(authoritative.content);
                remoteEntityId = authoritative.entityId;
                remoteRevision = authoritative.revision;
            } catch {
                // Conflict evidence is still complete enough to recover the
                // three snapshots. Do not pair scan bytes with a later revision.
                remoteRevision = undefined;
            }
        }
        const record: ConflictRecord = {
            path: filePath,
            artifactDirectory,
            reason,
            createdAt: new Date().toISOString(),
            baseFile,
            baseHash: baseline?.hash,
            baseType: baseline?.fileType,
            baselineEntityId: baseline?.entityId,
            localFile,
            remoteFile,
            localHash: scan.local.get(filePath)?.hash,
            localType: scan.local.get(filePath)?.fileType,
            remoteHash,
            remoteType: remote?.fileType,
            remoteEntityId,
            remoteRevision,
        };
        await this.writeArtifact(
            `${artifactDirectory}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                schemaVersion: 3,
                ...record,
                baseAvailable: baseFile !== undefined,
            }, null, 2)),
        );
        state.conflicts[filePath] = record;
        await this.stateStore.save(state);
        this.status = { status: 'need-attention', message: `Conflict paused: ${filePath}` };
    }

    /**
     * Turn every CAS recovery handoff into a durable path fence before file
     * watchers can observe the replica. The projection transaction remains
     * replayable until the conflict index has committed.
     */
    private async registerProjectionRecoveryConflicts(
        candidates: LocalProjectionRecoveryCandidate[],
    ): Promise<void> {
        const state = this.requireState();
        for (const candidate of candidates) {
            const existing = state.conflicts[candidate.path];
            if (
                existing?.artifactDirectory === candidate.artifactDirectory
                && existing.localFile === candidate.localFile
                && existing.localHash === candidate.localHash
            ) {
                const recovered = await readFile(this.localUri(candidate.localFile).fsPath);
                if (contentHash(recovered) !== candidate.localHash) {
                    throw new Error(`Recovered projection candidate changed: ${candidate.path}`);
                }
                existing.projectionRecoveryTransactionId = candidate.retainTransaction
                    ? candidate.transactionId
                    : undefined;
                await this.writeConflictManifest(existing, {
                    recoveryTransactionId: candidate.transactionId,
                    recoveryOperation: candidate.operation,
                    projectionExpectedHash: candidate.expectedHash,
                    projectionTargetHash: candidate.targetHash,
                });
                await this.stateStore.save(state);
                if (!candidate.retainTransaction) {
                    await this.localProjectionCas.acknowledgeRecovery(candidate.transactionId);
                }
                continue;
            }

            let record: ConflictRecord;
            if (existing) {
                const recovered = await readFile(this.localUri(candidate.localFile).fsPath);
                if (contentHash(recovered) !== candidate.localHash) {
                    throw new Error(`Recovered projection candidate changed: ${candidate.path}`);
                }
                const localFile = `${existing.artifactDirectory}/projection-recovery-`
                    + `${candidate.transactionId}/${candidate.path}`;
                await this.writeArtifact(localFile, recovered);
                existing.localFile = localFile;
                existing.localHash = candidate.localHash;
                existing.localType = state.entries[candidate.path]?.fileType
                    ?? existing.localType
                    ?? 'file';
                existing.projectionTargetHash = candidate.targetHash;
                existing.projectionRecoveryTransactionId = candidate.retainTransaction
                    ? candidate.transactionId
                    : undefined;
                record = existing;
            } else {
                const baseline = state.entries[candidate.path];
                const baseContent = await this.readCommonBase(baseline);
                const baseFile = baseContent === undefined
                    ? undefined
                    : `${candidate.artifactDirectory}/base/${candidate.path}`;
                if (baseContent !== undefined && baseFile) {
                    await this.writeArtifact(baseFile, baseContent);
                }

                let remoteFile: string | undefined;
                let remoteHash: string | undefined;
                let remoteEntityId: string | undefined;
                let remoteRevision: number | undefined;
                let remoteType: BaselineFileType | undefined;
                try {
                    const remote = await this.vfs.readAuthoritativeFileWithoutRecovery(
                        this.remoteUri(candidate.path),
                    );
                    remoteFile = `${candidate.artifactDirectory}/remote/${candidate.path}`;
                    await this.writeArtifact(remoteFile, remote.content);
                    remoteHash = contentHash(remote.content);
                    remoteEntityId = remote.entityId;
                    remoteRevision = remote.revision;
                    remoteType = remote.fileType;
                } catch (error) {
                    if (!this.isFileNotFound(error)) {
                        throw error;
                    }
                }

                record = {
                    path: candidate.path,
                    artifactDirectory: candidate.artifactDirectory,
                    reason: 'Recovered a local writer that changed a displaced projection inode; ancestry is not provable',
                    createdAt: new Date().toISOString(),
                    baseFile,
                    baseHash: baseline?.hash ?? candidate.expectedHash,
                    baseType: baseline?.fileType,
                    baselineEntityId: baseline?.entityId,
                    localFile: candidate.localFile,
                    localHash: candidate.localHash,
                    localType: baseline?.fileType ?? 'file',
                    remoteFile,
                    remoteHash,
                    remoteType,
                    remoteEntityId,
                    remoteRevision,
                    projectionTargetHash: candidate.targetHash,
                    projectionRecoveryTransactionId: candidate.retainTransaction
                        ? candidate.transactionId
                        : undefined,
                };
            }

            await this.writeConflictManifest(record, {
                recoveryTransactionId: candidate.transactionId,
                recoveryOperation: candidate.operation,
                projectionExpectedHash: candidate.expectedHash,
                projectionTargetHash: candidate.targetHash,
            });
            state.conflicts[candidate.path] = record;
            await this.stateStore.save(state);
            if (!candidate.retainTransaction) {
                await this.localProjectionCas.acknowledgeRecovery(candidate.transactionId);
            }
            this.output.appendLine(
                `[recovery] isolated displaced local writer for ${candidate.path}`,
            );
            this.status = {
                status: 'need-attention',
                message: `Recovered projection conflict: ${candidate.path}`,
            };
        }
    }

    private async registerIsolatedDocumentRecovery(
        error: DocumentRecoveryIsolationError,
    ): Promise<void> {
        const recovery = await this.vfs.inspectIsolatedDocumentRecovery(error.documentId);
        if (!recovery) {
            throw new Error(
                `Terminal document recovery evidence disappeared for ${error.path}`,
            );
        }
        const filePath = normalizeRelativePath(error.path);
        if (this.textReplicas?.has(filePath)) {
            await this.dropTextReplica(filePath, 'resource-change', error.reason);
        }
        await this.registerOrphanDocumentRecoveries([recovery]);
        this.scheduleReconcile(`document recovery isolated: ${error.path}`, 0);
    }

    private async registerOrphanDocumentRecoveries(
        recoveries: Array<PendingDocumentRecovery | IsolatedDocumentRecovery>,
    ): Promise<void> {
        if (recoveries.length === 0) {
            return;
        }
        const state = this.requireState();
        const livePathsByEntityId = new Map(
            this.vfs.walk(entity => entity._type === 'doc')
                .map(({entity, path: entityPath}) => [
                    entity._id,
                    normalizeRelativePath(entityPath),
                ] as const)
                .filter(([, entityPath]) => entityPath !== ''),
        );
        for (const recovery of recoveries) {
            const isolation = 'path' in recovery ? recovery : undefined;
            if (!isolation && recovery.pendingOperationIds.length === 0) {
                // Startup recovery is about journaled local operations. A
                // settled checkpoint is neither an orphan candidate nor a
                // reason to create user-visible recovery artifacts.
                continue;
            }
            const livePath = livePathsByEntityId.get(recovery.documentId);
            if (!isolation && livePath !== undefined && !this.isIgnored(livePath)) {
                // The normal immutable-id attach path reopens and replays it.
                continue;
            }
            const baselinePair = Object.entries(state.entries).find(([, entry]) => (
                entry.fileType === 'doc' && entry.entityId === recovery.documentId
            ));
            const conflictPair = Object.entries(state.conflicts).find(([, record]) => (
                record.baselineEntityId === recovery.documentId
                || record.remoteEntityId === recovery.documentId
                || record.retainedRemoteEntityId === recovery.documentId
                || record.documentRecoveries?.some(candidate => (
                    candidate.entityId === recovery.documentId
                )) === true
            ));
            const pendingActionPath = state.pendingAction?.entityId === recovery.documentId
                ? state.pendingAction.path
                : undefined;
            const pendingAiIntentPath = Object.values(state.aiCommitIntents ?? {})
                .find(intent => intent.entityId === recovery.documentId)?.path;
            // A completed AI receipt is still durable ancestry/path evidence:
            // its operation may be journaled while the remote ACK is pending.
            const completedAiCommitPath = Object.values(state.aiCompletedCommits ?? {})
                .find(commit => commit.entityId === recovery.documentId)?.path;
            const journalPath = recovery.lastKnownPath === undefined
                ? undefined
                : normalizeRelativePath(recovery.lastKnownPath);
            const historicalPath = baselinePair?.[0]
                ?? conflictPair?.[0]
                ?? pendingActionPath
                ?? pendingAiIntentPath
                ?? completedAiCommitPath;
            const durableJournalPath = journalPath && !isHardIgnored(journalPath)
                ? journalPath
                : undefined;
            const canDetachUnmappedWal = !isolation
                && livePath === undefined
                && historicalPath === undefined
                && durableJournalPath === undefined;
            if (canDetachUnmappedWal) {
                await this.registerDetachedDocumentRecovery(recovery);
                continue;
            }
            // An authoritative live identity is path proof even when the path is
            // reserved from ordinary synchronization. Only a self-reported WAL
            // binding is rejected when it points into a hard-ignored namespace.
            const filePath = isolation
                ? normalizeRelativePath(isolation.path)
                : historicalPath ?? durableJournalPath ?? livePath;
            if (!filePath) {
                throw new Error(
                    `Document recovery path invariant failed for ${recovery.documentId}`,
                );
            }
            this.assertSafePath(filePath);
            const baseline = state.entries[filePath] ?? baselinePair?.[1];
            const existing = state.conflicts[filePath];
            const preIsolationLocal = isolation && existing?.localFile
                ? {
                    file: existing.localFile,
                    hash: existing.localHash,
                    type: existing.localType,
                }
                : undefined;
            const artifactDirectory = existing?.artifactDirectory
                ?? `.overleaf/conflicts/unresolved/`
                    + `${isolation ? 'ot-history' : 'orphan-wal'}-`
                    + `${this.timestamp()}-${randomUUID()}`;
            const key = contentHash(new TextEncoder().encode(recovery.documentId)).slice(0, 20);
            const visible = new TextEncoder().encode(recovery.visibleContent);
            const acknowledged = new TextEncoder().encode(recovery.acknowledgedContent);
            const visibleFile = `${artifactDirectory}/wal-recovery/${key}/visible/${filePath}`;
            const acknowledgedFile = `${artifactDirectory}/wal-recovery/`
                + `${key}/acknowledged/${filePath}`;
            await this.writeArtifact(visibleFile, visible);
            await this.writeArtifact(acknowledgedFile, acknowledged);

            let baseFile = existing?.baseFile;
            if (!baseFile) {
                const base = await this.readCommonBase(baseline);
                if (base) {
                    baseFile = `${artifactDirectory}/base/${filePath}`;
                    await this.writeArtifact(baseFile, base);
                }
            }
            let remoteFile = existing?.remoteFile;
            let remoteHash = existing?.remoteHash;
            let remoteEntityId = existing?.remoteEntityId;
            let remoteRevision = existing?.remoteRevision;
            let remoteType = existing?.remoteType;
            try {
                const remote = await this.vfs.readAuthoritativeFileWithoutRecovery(
                    this.remoteUri(filePath),
                );
                remoteFile = `${artifactDirectory}/remote/${filePath}`;
                await this.writeArtifact(remoteFile, remote.content);
                remoteHash = contentHash(remote.content);
                remoteEntityId = remote.entityId;
                remoteRevision = remote.revision;
                remoteType = remote.fileType;
            } catch (error) {
                if (!this.isFileNotFound(error)) {
                    throw error;
                }
            }
            const documentRecovery = {
                entityId: recovery.documentId,
                visibleFile,
                acknowledgedFile,
                visibleHash: contentHash(visible),
                acknowledgedHash: contentHash(acknowledged),
                revision: recovery.revision,
                pendingOperationIds: [...recovery.pendingOperationIds],
                epoch: recovery.epoch,
                snapshotHash: recovery.hash,
            };
            const record: ConflictRecord = existing ?? {
                path: filePath,
                artifactDirectory,
                reason: isolation?.reason
                    ?? 'Recovered journaled text for a remote document identity that was deleted or replaced',
                createdAt: new Date().toISOString(),
                baseFile,
                baseHash: baseline?.hash,
                baseType: baseline?.fileType,
                baselineEntityId: baseline?.entityId,
                localFile: visibleFile,
                localHash: documentRecovery.visibleHash,
                localType: 'doc',
                remoteFile,
                remoteHash,
                remoteType,
                remoteEntityId,
                remoteRevision,
            };
            record.baseFile ??= baseFile;
            record.localFile = visibleFile;
            record.localHash = documentRecovery.visibleHash;
            record.localType = 'doc';
            record.remoteFile = remoteFile;
            record.remoteHash = remoteHash;
            record.remoteType = remoteType;
            record.remoteEntityId = remoteEntityId;
            record.remoteRevision = remoteRevision;
            const prior = [...(record.documentRecoveries ?? [])]
                .filter(candidate => candidate.entityId !== recovery.documentId);
            record.documentRecoveries = [...prior, documentRecovery];
            await this.writeConflictManifest(record, isolation ? {
                isolatedLiveDocumentRecovery: true,
                recoveryFailure: isolation.reason,
                recoveryFailedAt: isolation.failedAt,
                recoveryWalRevision: isolation.revision,
                recoveryPendingOperationIds: isolation.pendingOperationIds,
                preIsolationLocal,
            } : {orphanDocumentRecovery: true});
            state.conflicts[filePath] = record;
            await this.stateStore.save(state);
            this.output.appendLine(
                `[recovery] isolated ${isolation ? 'live' : 'orphan'} document WAL `
                + `${recovery.documentId} for ${filePath}`,
            );
        }
    }

    private async registerDetachedDocumentRecovery(
        recovery: PendingDocumentRecovery,
    ): Promise<void> {
        if (recovery.pendingOperationIds.length === 0) {
            throw new Error(
                `Refusing to detach settled document WAL ${recovery.documentId}`,
            );
        }
        const freshPath = await this.vfs.resolveAuthoritativeDocumentPath(
            recovery.documentId,
        );
        if (freshPath !== undefined) {
            throw new DetachedRecoveryPathAppearedError(
                `Document ${recovery.documentId} became reachable at ${freshPath} before `
                + 'detached recovery; startup will retry normal immutable-id replay',
            );
        }
        const state = this.requireState();
        let prepared: PreparedDocumentRecovery;
        try {
            prepared = await this.vfs.prepareDocumentRecoveryById(recovery.documentId);
        } catch (error) {
            if (/No durable document recovery state exists/.test(this.asError(error).message)) {
                return;
            }
            throw error;
        }
        const livePathBeforeArchive = await this.vfs.resolveAuthoritativeDocumentPath(
            recovery.documentId,
        );
        if (livePathBeforeArchive !== undefined) {
            throw new DetachedRecoveryPathAppearedError(
                `Document ${recovery.documentId} became reachable at ${livePathBeforeArchive} `
                + 'before detached recovery; startup will retry normal immutable-id replay',
            );
        }
        const existing = state.detachedDocumentRecoveries?.[recovery.documentId];
        const artifactDirectory = existing?.artifactDirectory
            ?? `.overleaf/conflicts/unresolved/detached-wal-`
                + `${this.timestamp()}-${randomUUID()}`;
        const visible = new TextEncoder().encode(prepared.visibleContent);
        const acknowledged = new TextEncoder().encode(prepared.acknowledgedContent);
        const visibleFile = `${artifactDirectory}/local-candidate.txt`;
        const acknowledgedFile = `${artifactDirectory}/last-acknowledged-base.txt`;
        await this.writeArtifact(visibleFile, visible);
        await this.writeArtifact(acknowledgedFile, acknowledged);
        const record: DetachedDocumentRecovery = {
            documentId: prepared.documentId,
            artifactDirectory,
            reason: 'A journaled document operation had no provable project path; '
                + 'the complete candidate was detached before filesystem observation',
            createdAt: existing?.createdAt ?? new Date().toISOString(),
            visibleFile,
            acknowledgedFile,
            visibleHash: contentHash(visible),
            acknowledgedHash: contentHash(acknowledged),
            revision: prepared.revision,
            pendingOperationIds: [...prepared.pendingOperationIds],
            epoch: prepared.epoch,
            snapshotHash: prepared.hash,
            walRetired: false,
        };
        state.detachedDocumentRecoveries ??= {};
        state.detachedDocumentRecoveries[record.documentId] = record;
        await this.writeDetachedRecoveryManifest(record);
        await this.stateStore.save(state);
        try {
            await this.vfs.commitDiscardDocumentRecovery(prepared.token);
        } catch (error) {
            this.output.appendLine(
                `[recovery] detached WAL ${record.documentId} remains fenced for retry: `
                + this.asError(error).message,
            );
            return;
        }
        record.walRetired = true;
        await this.writeDetachedRecoveryManifest(record);
        await this.stateStore.save(state);
        this.output.appendLine(
            `[recovery] archived unmapped document WAL ${record.documentId} at `
            + record.artifactDirectory,
        );
    }

    private writeDetachedRecoveryManifest(record: DetachedDocumentRecovery): Promise<void> {
        return this.writeArtifact(
            `${record.artifactDirectory}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                schemaVersion: 1,
                ...record,
                currentRemoteAvailable: false,
                recoveryDisposition: record.walRetired
                    ? 'archived-and-retired'
                    : 'archived-pending-retirement',
            }, null, 2)),
        );
    }

    private async recoverDetachedRecoveryArtifactLocations(): Promise<void> {
        const state = this.requireState();
        let changed = false;
        for (const record of Object.values(state.detachedDocumentRecoveries ?? {})) {
            if (await this.localArtifactExists(record.artifactDirectory)) {
                continue;
            }
            const resolvedDirectory = record.artifactDirectory.replace(
                '.overleaf/conflicts/unresolved/',
                '.overleaf/conflicts/resolved/',
            );
            if (
                resolvedDirectory === record.artifactDirectory
                || !await this.localArtifactExists(resolvedDirectory)
            ) {
                throw new Error(
                    `Detached recovery evidence disappeared for ${record.documentId}`,
                );
            }
            this.relocateDetachedRecoveryArtifacts(
                record,
                record.artifactDirectory,
                resolvedDirectory,
            );
            await this.writeDetachedRecoveryManifest(record);
            changed = true;
        }
        if (changed) {
            await this.stateStore.save(state);
        }
    }

    private relocateDetachedRecoveryArtifacts(
        record: DetachedDocumentRecovery,
        source: string,
        target: string,
    ): void {
        const relocate = (filePath: string): string => (
            filePath === source || filePath.startsWith(`${source}/`)
                ? `${target}${filePath.slice(source.length)}`
                : filePath
        );
        record.artifactDirectory = target;
        record.visibleFile = relocate(record.visibleFile);
        record.acknowledgedFile = relocate(record.acknowledgedFile);
    }

    private async acknowledgeDetachedDocumentRecovery(documentId: string): Promise<void> {
        const state = this.requireState();
        const record = state.detachedDocumentRecoveries?.[documentId];
        if (!record) {
            return;
        }
        if (!record.walRetired) {
            throw new Error(`Detached WAL retirement is incomplete: ${documentId}`);
        }
        if (record.artifactDirectory.startsWith('.overleaf/conflicts/unresolved/')) {
            const source = record.artifactDirectory;
            const target = source.replace(
                '.overleaf/conflicts/unresolved/',
                '.overleaf/conflicts/resolved/',
            );
            await vscode.workspace.fs.rename(
                this.localUri(source),
                this.localUri(target),
                {overwrite: false},
            );
            this.relocateDetachedRecoveryArtifacts(record, source, target);
            await this.writeDetachedRecoveryManifest(record);
        }
        delete state.detachedDocumentRecoveries![documentId];
        await this.stateStore.save(state);
        this.scheduleReconcile(`detached recovery acknowledged: ${documentId}`, 0);
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
        side: string,
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
            if (side.includes('local') && snapshot.localVariants) {
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
        if (uri.scheme !== undefined && uri.scheme !== 'file') {
            throw new Error(`Conflict artifact requires a file URI: ${filePath}`);
        }
        await writeFileDurably(uri.fsPath, content);
    }

    private writeConflictManifest(
        record: ConflictRecord,
        extra: Record<string, unknown> = {},
    ): Promise<void> {
        return this.writeArtifact(
            `${record.artifactDirectory}/manifest.json`,
            new TextEncoder().encode(JSON.stringify({
                schemaVersion: 3,
                ...record,
                baseAvailable: record.baseFile !== undefined,
                ...extra,
            }, null, 2)),
        );
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
        const detachedRecoveries = Object.values(
            this.state?.detachedDocumentRecoveries ?? {},
        );
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
            `Active path conflicts: ${Object.keys(this.state?.conflicts ?? {}).length}`,
            `Detached WAL recoveries: ${detachedRecoveries.length}`,
            `Local editor/disk overlaps: ${localWriterOverlaps.length}`,
            '',
            ...this.reportGroup('Local only', localOnly),
            ...this.reportGroup('Remote only', remoteOnly),
            ...this.reportGroup('Different', different),
            ...this.reportGroup('Local editor/disk overlaps', localWriterOverlaps),
            ...this.reportGroup(
                'Detached WAL recovery artifacts',
                detachedRecoveries.map(record => (
                    `${record.documentId}: ${record.artifactDirectory}`
                )),
            ),
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
        if (
            this.state.pendingAction
            || Object.keys(this.state.conflicts).length > 0
            || Object.keys(this.state.detachedDocumentRecoveries ?? {}).length > 0
        ) {
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
        await this.reconcileQueue.run(async () => {
            if (this.disposed || this.handoffInProgress) {
                throw new Error('Safe-sync ownership retirement is in progress');
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
                await this.handleReconcileError(error);
            }
        });
    }

    private async initializeFromRemote(scan: ScanPair): Promise<void> {
        const state = this.requireState();
        await this.archiveBootstrap('overleaf-baseline', 'local', scan.local, scan.remote);
        const localExtras = this.topLevelSnapshots(
            [...scan.local.values()].filter(snapshot => !scan.remote.has(snapshot.path)),
        );
        for (const snapshot of localExtras) {
            await this.deleteLocalSnapshot(snapshot.path, snapshot, scan.local);
        }
        const remoteEntries = [...scan.remote.values()].sort((left, right) => (
            left.path.split('/').length - right.path.split('/').length
        ));
        for (const remote of remoteEntries) {
            const local = scan.local.get(remote.path);
            if (local?.fileType !== remote.fileType && local) {
                await this.deleteLocalSnapshot(remote.path, local, scan.local);
            }
            if (!local || local.fileType !== remote.fileType || local.hash !== remote.hash) {
                await this.writeLocalSnapshot(
                    remote,
                    local?.fileType === remote.fileType ? local : undefined,
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
        let working = await this.scanBoth();
        for (;;) {
            const remote = [...working.remote.values()]
                .filter(candidate => {
                    const local = working.local.get(candidate.path);
                    return !local || local.fileType !== candidate.fileType;
                })
                .sort((left, right) => (
                    right.path.split('/').length - left.path.split('/').length
                ))[0];
            if (!remote) {
                break;
            }
            try {
                await this.removeRemoteSnapshot(remote);
            } catch (error) {
                if (!this.isFileNotFound(error)) {
                    throw error;
                }
            }
            working = await this.scanBoth();
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
                // Baseline selection is explicit and one-shot, but its OT
                // admission still spans a crash boundary. Derive one stable
                // logical id from the durable replica and exact input hashes
                // so repeating the same selection can only observe/reuse the
                // already journaled operation, never submit a duplicate edit.
                const bootstrapOperationId = [
                    state.replicaId,
                    'bootstrap',
                    remote.entityId,
                    remote.hash,
                    local.hash,
                ].join(':');
                const actual = await this.vfs.writeDocumentAgainstAncestor(
                    this.remoteUri(local.path),
                    remote.text,
                    local.text,
                    remote.entityId,
                    bootstrapOperationId,
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
            vscode.window.showErrorMessage(this.asError(error).message);
        }
    }

    private async showDetachedDocumentRecovery(): Promise<void> {
        const records = Object.values(
            this.requireState().detachedDocumentRecoveries ?? {},
        );
        const selected = await vscode.window.showQuickPick(
            records.map(record => ({
                label: record.documentId,
                description: record.walRetired ? 'WAL archived' : 'WAL retirement pending',
                record,
            })),
            {ignoreFocusOut: true, title: 'Detached safe-sync recovery artifacts'},
        );
        if (!selected) {
            return;
        }
        const openManifest = 'Open archived manifest';
        const acknowledge = 'Acknowledge and move archive to resolved';
        const action = await vscode.window.showQuickPick(
            [openManifest, acknowledge],
            {ignoreFocusOut: true, title: `Detached recovery ${selected.record.documentId}`},
        );
        if (action === openManifest) {
            const document = await vscode.workspace.openTextDocument(
                this.localUri(`${selected.record.artifactDirectory}/manifest.json`),
            );
            await vscode.window.showTextDocument(document, {preview: false});
        } else if (action === acknowledge) {
            await this.acknowledgeDetachedDocumentRecovery(selected.record.documentId);
        }
    }

    private async resolveConflictRecord(
        filePath: string,
        resolution: ConflictResolution,
    ): Promise<string> {
        if (this.resolvingPaths.has(filePath)) {
            throw new Error(`Conflict resolution is already running for ${filePath}`);
        }
        this.resolvingPaths.add(filePath);
        this.resolvingResolutions ??= new Map<string, ConflictResolution>();
        this.resolvingResolutions.set(filePath, resolution);
        try {
            return await this.resolveConflictRecordOwned(filePath, resolution);
        } finally {
            this.resolutionTypeOverrides?.delete(filePath);
            this.resolvingResolutions?.delete(filePath);
            this.resolvingPaths.delete(filePath);
        }
    }

    private async resolveConflictRecordOwned(
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
        await this.recoverConflictArtifactLocation(record);
        await this.refreshProjectionRecoveryCandidate(record);
        await this.retireTextReplicasUnder(filePath);
        await this.drainTextConflictTasks();
        await this.drainIsolatedRevisionTasks(filePath);
        this.clearProjectionReceiptsUnder(filePath);
        let scan = await this.scanBoth();
        await this.archiveResolutionCandidates(record, scan);
        await this.drainIsolatedRevisionTasks(filePath);
        await this.adjudicateDocumentRecoveries(record, scan);
        // Recovery commit closes old WAL owners. Re-observe remote state only
        // afterwards so a late, already-accepted server operation is included
        // in the candidate the user is choosing between.
        scan = await this.scanBoth();
        await this.archiveResolutionCandidates(record, scan);
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
        await this.refreshConvergedState(state, canonicalScan, filePath);
        await this.stateStore.save(state);

        let restoredSiblingPath: string | undefined;
        if (resolution !== 'remote') {
            if (resolution === 'local' && record.localType) {
                this.resolutionTypeOverrides ??= new Map<string, BaselineFileType>();
                this.resolutionTypeOverrides.set(record.path, record.localType);
            }
            if (record.localFile) {
                const target = resolution === 'both'
                    ? this.conflictSibling(record.path)
                    : record.path;
                if (resolution === 'both') {
                    restoredSiblingPath = target;
                    if (record.localType) {
                        this.resolutionTypeOverrides ??= new Map<string, BaselineFileType>();
                        this.resolutionTypeOverrides.set(target, record.localType);
                    }
                }
                await this.copyLocalTree(
                    record.localFile,
                    target,
                    resolution === 'local' ? canonicalScan.local : undefined,
                    record.localType,
                );
            } else if (resolution === 'local') {
                const canonicalLocal = canonicalScan.local.get(record.path);
                if (canonicalLocal) {
                    await this.deleteLocalSnapshot(record.path, canonicalLocal, canonicalScan.local);
                }
            }
        }

        if (resolution === 'local') {
            await this.convergeArchivedLocalResolution(record);
        } else if (restoredSiblingPath) {
            try {
                await this.convergeRestoredSibling(restoredSiblingPath);
            } finally {
                this.resolutionTypeOverrides?.delete(restoredSiblingPath);
            }
        }

        const resolvedArtifactDirectory = await this.moveConflictToResolved(record);
        delete state.conflicts[record.path];
        try {
            await this.stateStore.save(state);
        } catch (error) {
            // Keep the in-memory record recoverable when durable finalization
            // fails after the atomic artifact rename.
            state.conflicts[record.path] = record;
            throw error;
        }
        this.scheduleReconcile(`conflict resolved: ${record.path}`, 0);
        return resolvedArtifactDirectory;
    }

    private async convergeRestoredSibling(filePath: string): Promise<void> {
        await this.convergeLocalResolutionSubtree(filePath);
    }

    private async convergeArchivedLocalResolution(record: ConflictRecord): Promise<void> {
        await this.convergeLocalResolutionSubtree(record.path, record.path);
    }

    /**
     * Project an archived local choice one resource at a time. Each mutation
     * has its own durable journal boundary, so a folder resolution can resume
     * after a crash without treating a partially copied subtree as converged.
     */
    private async convergeLocalResolutionSubtree(
        filePath: string,
        resolvedConflictPath?: string,
    ): Promise<void> {
        const state = this.requireState();
        let mutationCount = 0;
        let maximumMutations = 32;
        while (mutationCount <= maximumMutations) {
            let scan = await this.scanBoth();
            const subtreeSize = [...new Set([
                ...scan.local.keys(),
                ...scan.remote.keys(),
            ])].filter(candidate => (
                candidate === filePath || candidate.startsWith(`${filePath}/`)
            )).length;
            maximumMutations = Math.max(maximumMutations, subtreeSize * 4 + 16);

            if (state.pendingAction) {
                if (!this.pendingActionCompleted(state.pendingAction, scan)) {
                    const resumed = await this.resumePendingAction(state.pendingAction, scan);
                    if (!resumed) {
                        throw new Error(
                            `Conflict resolution journal cannot safely resume ${filePath}`,
                        );
                    }
                    mutationCount++;
                    continue;
                }
                state.pendingAction = undefined;
                await this.stateStore.save(state);
            }

            if (this.isSubtreeConverged(filePath, scan)) {
                await this.refreshConvergedState(state, scan, resolvedConflictPath);
                await this.stateStore.save(state);
                return;
            }

            const remoteExtras = this.topLevelSnapshots(
                [...scan.remote.values()].filter(snapshot => (
                    (snapshot.path === filePath || snapshot.path.startsWith(`${filePath}/`))
                    && !scan.local.has(snapshot.path)
                )),
            );
            const remoteExtra = remoteExtras[0];
            if (remoteExtra) {
                if (remoteExtra.readonly) {
                    throw new Error(`The current Overleaf entity is read-only: ${remoteExtra.path}`);
                }
                await this.journalAction(
                    'delete-remote',
                    remoteExtra.path,
                    undefined,
                    remoteExtra,
                    undefined,
                    () => this.removeRemoteSnapshot(remoteExtra),
                );
                mutationCount++;
                continue;
            }

            const localCandidate = [...scan.local.values()]
                .filter(snapshot => (
                    snapshot.path === filePath || snapshot.path.startsWith(`${filePath}/`)
                ))
                .sort((left, right) => (
                    left.path.split('/').length - right.path.split('/').length
                    || left.path.localeCompare(right.path)
                ))
                .find(snapshot => {
                    const remote = scan.remote.get(snapshot.path);
                    return !remote
                        || remote.fileType !== snapshot.fileType
                        || remote.hash !== snapshot.hash;
                });
            if (!localCandidate || localCandidate.localVariants) {
                throw new StaleLocalSnapshotError(
                    `Archived local subtree changed while resolving ${filePath}`,
                );
            }
            const remote = scan.remote.get(localCandidate.path);
            if (remote?.readonly) {
                throw new Error(`The current Overleaf entity is read-only: ${localCandidate.path}`);
            }
            await this.journalAction(
                'resolve-conflict',
                localCandidate.path,
                localCandidate,
                remote,
                localCandidate.hash,
                () => this.applyResolvedLocalSnapshot(localCandidate, remote),
                undefined,
                remote?.entityId,
                remote?.fileType === 'doc'
                    && remote.text !== undefined
                    && localCandidate.fileType === 'doc'
                    && localCandidate.text !== undefined
                    ? {ancestor: remote.text, target: localCandidate.text}
                    : undefined,
            );
            mutationCount++;
        }
        throw new Error(`Conflict subtree kept changing while resolving ${filePath}`);
    }

    private async applyResolvedLocalSnapshot(
        local: ReplicaSnapshot,
        observedRemote: ReplicaSnapshot | undefined,
    ): Promise<void> {
        let remote = observedRemote;
        if (remote && remote.fileType !== local.fileType) {
            await this.removeRemoteSnapshot(remote);
            remote = undefined;
        }
        if (local.fileType === 'folder') {
            if (!remote) {
                await this.vfs.mkdir(this.remoteUri(local.path));
            }
            return;
        }
        if (!local.content) {
            throw new Error(`Archived local choice has no content: ${local.path}`);
        }
        if (!remote) {
            const created = await this.vfs.createTypedFile(
                this.remoteUri(local.path),
                local.content,
                local.fileType,
            );
            await this.updatePendingTarget(created.content);
            if (contentHash(created.content) !== local.hash) {
                await this.writeLocalContent(
                    local.path,
                    created.content,
                    local.hash,
                    local.fileType === 'doc',
                );
            }
            return;
        }
        if (remote.fileType === 'doc' && local.fileType === 'doc') {
            if (remote.text === undefined || local.text === undefined || !remote.entityId) {
                throw new Error(`Resolved document is not valid UTF-8: ${local.path}`);
            }
            const actual = await this.acceptPendingDocumentOperation(
                local.path,
                remote.text,
                local.text,
                remote.entityId,
            );
            await this.convergeTextToLocal(
                local.path,
                actual,
                local.text,
                local.hash,
                local.localVariants?.diskHash,
                remote.entityId,
            );
            return;
        }
        if (remote.fileType === 'file' && local.fileType === 'file' && remote.entityId) {
            await this.vfs.writeResourceAgainstBaseline(
                this.remoteUri(local.path),
                remote.entityId,
                remote.hash,
                local.content,
            );
            await this.updatePendingTarget(local.content);
            return;
        }
        throw new Error(`Unsupported archived local type transition for ${local.path}`);
    }

    private async refreshProjectionRecoveryCandidate(record: ConflictRecord): Promise<void> {
        const transactionId = record.projectionRecoveryTransactionId;
        if (!transactionId) {
            return;
        }
        const inspection = await this.localProjectionCas.inspectRecovery(transactionId);
        if (inspection.status === 'busy') {
            throw new Error(
                `A local writer still has ${record.path} open; close it before resolving this conflict`,
            );
        }
        if (inspection.status === 'missing') {
            if (
                record.projectionRecoveryArchivedTransactionId !== transactionId
                || record.projectionRecoveryArchivedHash === undefined
                || record.localHash !== record.projectionRecoveryArchivedHash
            ) {
                throw new Error(
                    `Projection recovery transaction disappeared before its latest writer bytes `
                    + `were durably proven for ${record.path}`,
                );
            }
            record.projectionRecoveryTransactionId = undefined;
            await this.writeConflictManifest(record, {
                projectionRecoveryState: 'already-acknowledged',
            });
            await this.stateStore.save(this.requireState());
            return;
        }
        const candidate = inspection.candidate;
        if (candidate.path !== record.path || candidate.transactionId !== transactionId) {
            throw new Error(`Projection recovery identity changed for ${record.path}`);
        }
        const latest = await readFile(this.localUri(candidate.localFile).fsPath);
        if (contentHash(latest) !== candidate.localHash) {
            throw new Error(`Projection recovery candidate changed for ${record.path}`);
        }
        const localFile = `${record.artifactDirectory}/projection-recovery-`
            + `${transactionId}/${record.path}`;
        await this.writeArtifact(localFile, latest);
        record.localFile = localFile;
        record.localHash = candidate.localHash;
        record.localType ??= this.requireState().entries[record.path]?.fileType ?? 'file';
        record.projectionTargetHash = candidate.targetHash;
        record.projectionRecoveryArchivedTransactionId = transactionId;
        record.projectionRecoveryArchivedHash = candidate.localHash;
        await this.writeConflictManifest(record, {
            projectionRecoveryState: 'latest-writer-closed',
            recoveryOperation: candidate.operation,
            projectionExpectedHash: candidate.expectedHash,
        });
        await this.stateStore.save(this.requireState());
        await this.localProjectionCas.acknowledgeRecovery(transactionId);
        record.projectionRecoveryTransactionId = undefined;
        await this.writeConflictManifest(record, {
            projectionRecoveryState: 'archived-and-acknowledged',
        });
        await this.stateStore.save(this.requireState());
    }

    private async adjudicateDocumentRecoveries(
        record: ConflictRecord,
        scan: ScanPair,
    ): Promise<void> {
        const baseline = this.requireState().entries[record.path];
        const currentRemote = scan.remote.get(record.path);
        const ids = new Set<string>();
        if ((record.baseType ?? baseline?.fileType) === 'doc' && record.baselineEntityId) {
            ids.add(record.baselineEntityId);
        }
        if ((record.remoteType ?? currentRemote?.fileType) === 'doc' && record.remoteEntityId) {
            ids.add(record.remoteEntityId);
        }
        if (currentRemote?.fileType === 'doc' && currentRemote.entityId) {
            ids.add(currentRemote.entityId);
        }
        for (const recovery of record.documentRecoveries ?? []) {
            ids.add(recovery.entityId);
        }

        const prepared: PreparedDocumentRecovery[] = [];
        for (const entityId of ids) {
            try {
                prepared.push(await this.vfs.prepareDocumentRecoveryById(entityId));
            } catch (error) {
                if (/No durable document recovery state exists/.test(this.asError(error).message)) {
                    continue;
                }
                throw error;
            }
        }
        if (prepared.length === 0) {
            return;
        }

        const recoveries = [...(record.documentRecoveries ?? [])];
        for (const recovery of prepared) {
            const visible = new TextEncoder().encode(recovery.visibleContent);
            const acknowledged = new TextEncoder().encode(recovery.acknowledgedContent);
            const visibleHash = contentHash(visible);
            const acknowledgedHash = contentHash(acknowledged);
            const hasCandidate = recovery.pendingOperationIds.length > 0
                || visibleHash !== acknowledgedHash;
            const key = contentHash(new TextEncoder().encode(recovery.documentId)).slice(0, 20);
            const visibleFile = hasCandidate
                ? `${record.artifactDirectory}/wal-recovery/${key}/visible/${record.path}`
                : undefined;
            const acknowledgedFile = hasCandidate
                ? `${record.artifactDirectory}/wal-recovery/${key}/acknowledged/${record.path}`
                : undefined;
            if (visibleFile && acknowledgedFile) {
                await this.writeArtifact(visibleFile, visible);
                await this.writeArtifact(acknowledgedFile, acknowledged);
            }
            const entry = {
                entityId: recovery.documentId,
                visibleFile,
                acknowledgedFile,
                visibleHash,
                acknowledgedHash,
                revision: recovery.revision,
                pendingOperationIds: [...recovery.pendingOperationIds],
                epoch: recovery.epoch,
                snapshotHash: recovery.hash,
            };
            const previous = recoveries.findIndex(candidate => candidate.entityId === recovery.documentId);
            if (previous < 0) {
                recoveries.push(entry);
            } else {
                recoveries[previous] = entry;
            }
        }
        record.documentRecoveries = recoveries;
        await this.writeConflictManifest(record, {
            documentRecoveryPreparedAt: new Date().toISOString(),
        });
        await this.stateStore.save(this.requireState());

        for (const recovery of prepared) {
            await this.vfs.commitDiscardDocumentRecovery(recovery.token);
        }
    }

    private async archiveResolutionCandidates(
        record: ConflictRecord,
        scan: ScanPair,
    ): Promise<void> {
        const revision = `resolution-${this.timestamp()}-${randomUUID()}`;
        const localFile = await this.archiveSubtree(
            record.artifactDirectory,
            `${revision}-local`,
            record.path,
            scan.local,
        );
        const remoteFile = await this.archiveSubtree(
            record.artifactDirectory,
            `${revision}-remote`,
            record.path,
            scan.remote,
        );
        const local = scan.local.get(record.path);
        const remote = scan.remote.get(record.path);
        const localIsRecoveredProjection = record.projectionTargetHash !== undefined
            && local?.hash === record.projectionTargetHash
            && record.localHash !== local.hash;
        const localIsOrphanJournalCandidate = record.documentRecoveries?.some(recovery => (
            recovery.visibleFile === record.localFile
            && recovery.visibleHash === record.localHash
            && recovery.pendingOperationIds.length > 0
        )) === true;
        if (localFile && !localIsRecoveredProjection && !localIsOrphanJournalCandidate) {
            record.localFile = localFile;
            record.localHash = local?.hash;
            record.localType ??= local?.fileType;
        }
        if (remoteFile) {
            record.remoteFile = remoteFile;
            record.remoteHash = remote?.hash;
            record.remoteType = remote?.fileType;
            record.remoteEntityId = remote?.entityId;
        }
        if (remote?.fileType === 'doc') {
            try {
                const authoritative = await this.vfs.readAuthoritativeFileWithoutRecovery(
                    this.remoteUri(record.path),
                );
                if (
                    authoritative.fileType !== 'doc'
                    || authoritative.entityId !== remote.entityId
                    || authoritative.revision === undefined
                ) {
                    throw new ConcurrentRemoteResourceError(this.remoteUri(record.path).path);
                }
                const exactRemoteFile = `${record.artifactDirectory}/${revision}-remote/${record.path}`;
                await this.writeArtifact(exactRemoteFile, authoritative.content);
                record.remoteFile = exactRemoteFile;
                record.remoteHash = contentHash(authoritative.content);
                record.remoteEntityId = authoritative.entityId;
                record.remoteRevision = authoritative.revision;
            } catch {
                // The durable bytes above remain the authoritative candidate.
                record.remoteRevision = undefined;
            }
        }
        await this.writeConflictManifest(record, {
            resolutionSnapshotAt: new Date().toISOString(),
        });
        await this.stateStore.save(this.requireState());
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
                await this.deleteLocalSnapshot(filePath, localRoot, current.local);
            }
            const verification = await this.scanBoth();
            const divergence = this.describeSubtreeDivergence(filePath, verification);
            if (divergence === undefined) {
                return;
            }
            if (attempt >= 4) {
                throw new Error(
                    `Overleaf kept changing while resolving ${filePath}; conflict remains `
                    + `paused (${divergence})`,
                );
            }
            return this.restoreRemotePath(filePath, verification, attempt + 1);
        }
        const localExtras = this.topLevelSnapshots(
            [...current.local.values()].filter(snapshot => (
                (snapshot.path === filePath || snapshot.path.startsWith(`${filePath}/`))
                && !current.remote.has(snapshot.path)
            )),
        );
        for (const extra of localExtras) {
            await this.deleteLocalSnapshot(extra.path, extra, current.local);
        }
        current = await this.scanBoth();
        for (const remote of remoteEntries) {
            let local = current.local.get(remote.path);
            if (local && local.fileType !== remote.fileType) {
                await this.deleteLocalSnapshot(remote.path, local, current.local);
                current = await this.scanBoth();
                local = current.local.get(remote.path);
            }
            if (!local || local.fileType !== remote.fileType || local.hash !== remote.hash) {
                await this.writeLocalSnapshot(
                    remote,
                    local?.fileType === remote.fileType ? local : undefined,
                );
                current = await this.scanBoth();
            }
        }
        const verification = await this.scanBoth();
        const divergence = this.describeSubtreeDivergence(filePath, verification);
        if (divergence !== undefined) {
            if (attempt >= 4) {
                throw new Error(
                    `Overleaf kept changing while resolving ${filePath}; conflict remains `
                    + `paused (${divergence})`,
                );
            }
            return this.restoreRemotePath(filePath, verification, attempt + 1);
        }
    }

    private isSubtreeConverged(filePath: string, scan: ScanPair): boolean {
        return this.describeSubtreeDivergence(filePath, scan) === undefined;
    }

    /** Name the first path that still differs, for a resolution that gave up. */
    private describeSubtreeDivergence(
        filePath: string,
        scan: ScanPair,
    ): string | undefined {
        const paths = new Set(
            [...scan.local.keys(), ...scan.remote.keys()]
                .filter(candidate => candidate === filePath || candidate.startsWith(`${filePath}/`)),
        );
        for (const candidate of paths) {
            const local = scan.local.get(candidate);
            const remote = scan.remote.get(candidate);
            if (!local || !remote) {
                return `${candidate} exists only ${local ? 'locally' : 'remotely'}`;
            }
            if (local.localVariants) {
                return `${candidate} still holds unmerged local variants`;
            }
            if (local.fileType !== remote.fileType) {
                return `${candidate} is a local ${local.fileType} and a remote ${remote.fileType}`;
            }
            if (local.hash !== remote.hash) {
                return `${candidate} content differs`;
            }
        }
        return undefined;
    }

    private async copyLocalTree(
        sourcePath: string,
        targetPath: string,
        expectedTargets?: ReadonlyMap<string, ReplicaSnapshot>,
        forcedType?: BaselineFileType,
    ): Promise<void> {
        const source = this.localUri(sourcePath);
        const stat = await vscode.workspace.fs.stat(source);
        const expected = expectedTargets?.get(targetPath);
        if ((stat.type & vscode.FileType.Directory) !== 0) {
            if (expected && expected.fileType !== 'folder') {
                await this.deleteLocalSnapshot(targetPath, expected, expectedTargets);
            }
            await vscode.workspace.fs.createDirectory(this.localUri(targetPath));
            const sourceEntries = await vscode.workspace.fs.readDirectory(source);
            if (expectedTargets) {
                const sourceNames = new Set(sourceEntries.map(([name]) => name));
                for (const [candidate, snapshot] of expectedTargets) {
                    if (
                        this.parentPath(candidate) === targetPath
                        && !sourceNames.has(path.posix.basename(candidate))
                    ) {
                        await this.deleteOwnedLocalTarget(
                            candidate,
                            snapshot,
                            expectedTargets,
                        );
                    }
                }
            }
            for (const [name] of sourceEntries) {
                await this.copyLocalTree(
                    `${sourcePath}/${name}`,
                    `${targetPath}/${name}`,
                    expectedTargets,
                    undefined,
                );
            }
        } else {
            if (expected?.fileType === 'folder') {
                await this.deleteOwnedLocalTarget(
                    targetPath,
                    expected,
                    expectedTargets!,
                );
            }
            const content = await vscode.workspace.fs.readFile(source);
            await this.writeLocalContent(
                targetPath,
                content,
                expected && expected.fileType !== 'folder' ? expected.hash : undefined,
                forcedType === 'doc'
                    ? true
                    : forcedType === 'file'
                        ? false
                        : isEditableTextCandidate(targetPath, content),
            );
        }
    }

    /** Delete only the exact canonical tree observed before local restoration. */
    private async deleteOwnedLocalTarget(
        filePath: string,
        expected: ReplicaSnapshot,
        expectedTargets: ReadonlyMap<string, ReplicaSnapshot>,
    ): Promise<void> {
        if (expected.fileType !== 'folder') {
            await this.deleteLocalSnapshot(filePath, expected, expectedTargets);
            return;
        }
        const expectedTarget = expectedTargets.get(filePath);
        if (
            !expectedTarget
            || expectedTarget.fileType !== 'folder'
            || expectedTarget.hash !== expected.hash
            || (
                expected.treeHash !== undefined
                && expectedTarget.treeHash !== expected.treeHash
            )
        ) {
            throw new StaleLocalSnapshotError(
                `Local directory ownership changed during deletion: ${filePath}`,
            );
        }
        if (expected.localIdentity !== undefined) {
            let actualIdentity: string;
            try {
                actualIdentity = await this.localIdentity(this.localUri(filePath));
            } catch {
                throw new StaleLocalSnapshotError(`Local directory disappeared before deletion: ${filePath}`);
            }
            if (actualIdentity !== expected.localIdentity) {
                throw new StaleLocalSnapshotError(`Local directory identity changed before deletion: ${filePath}`);
            }
        }
        const expectedChildren = [...expectedTargets]
            .filter(([candidate]) => this.parentPath(candidate) === filePath)
            .sort(([left], [right]) => left.localeCompare(right));
        let actualChildren: string[];
        try {
            actualChildren = (await vscode.workspace.fs.readDirectory(this.localUri(filePath)))
                .map(([name]) => name)
                .sort();
        } catch {
            throw new StaleLocalSnapshotError(`Local directory changed during deletion: ${filePath}`);
        }
        const expectedNames = expectedChildren
            .map(([candidate]) => path.posix.basename(candidate))
            .sort();
        if (JSON.stringify(actualChildren) !== JSON.stringify(expectedNames)) {
            throw new StaleLocalSnapshotError(
                `Local directory changed during conflict resolution: ${filePath}`,
            );
        }
        for (const [candidate, child] of expectedChildren) {
            await this.deleteOwnedLocalTarget(candidate, child, expectedTargets);
        }
        try {
            await vscode.workspace.fs.delete(
                this.localUri(filePath),
                {recursive: false, useTrash: false},
            );
        } catch {
            // A member may have appeared after the last directory listing.
            // There is no compare-and-delete primitive in VS Code's FS API,
            // so fail closed and let reconciliation obtain a fresh snapshot.
            throw new StaleLocalSnapshotError(
                `Local directory changed during deletion: ${filePath}`,
            );
        }
    }

    private async moveConflictToResolved(record: ConflictRecord): Promise<string> {
        if (record.artifactDirectory.startsWith('.overleaf/conflicts/resolved/')) {
            if (!await this.localArtifactExists(record.artifactDirectory)) {
                throw new Error(`Resolved conflict artifact is missing for ${record.path}`);
            }
            await this.writeConflictManifest(record, {resolvedAt: new Date().toISOString()});
            await this.stateStore.save(this.requireState());
            return record.artifactDirectory;
        }
        const target = record.artifactDirectory.replace(
            '.overleaf/conflicts/unresolved/',
            '.overleaf/conflicts/resolved/',
        );
        const sourceFsPath = this.localUri(record.artifactDirectory).fsPath;
        const targetFsPath = this.localUri(target).fsPath;
        const sourceParent = path.dirname(sourceFsPath);
        const targetParent = path.dirname(targetFsPath);
        await createDirectoryDurably(targetParent, 0o700);
        await rename(sourceFsPath, targetFsPath);
        await syncDirectoryDurably(sourceParent);
        if (targetParent !== sourceParent) {
            await syncDirectoryDurably(targetParent);
        }
        this.rewriteConflictArtifactLocation(record, record.artifactDirectory, target);
        await this.writeConflictManifest(record, {resolvedAt: new Date().toISOString()});
        await this.stateStore.save(this.requireState());
        return target;
    }

    /** Recover the atomic-rename/durable-state crash window idempotently. */
    private async recoverConflictArtifactLocation(record: ConflictRecord): Promise<void> {
        if (await this.localArtifactExists(record.artifactDirectory)) {
            return;
        }
        if (!record.artifactDirectory.startsWith('.overleaf/conflicts/unresolved/')) {
            throw new Error(`Conflict artifact is missing for ${record.path}`);
        }
        const resolved = record.artifactDirectory.replace(
            '.overleaf/conflicts/unresolved/',
            '.overleaf/conflicts/resolved/',
        );
        if (!await this.localArtifactExists(resolved)) {
            throw new Error(`Conflict artifact is missing for ${record.path}`);
        }
        this.rewriteConflictArtifactLocation(record, record.artifactDirectory, resolved);
        await this.writeConflictManifest(record, {recoveredResolvedArtifact: true});
        await this.stateStore.save(this.requireState());
    }

    private rewriteConflictArtifactLocation(
        record: ConflictRecord,
        source: string,
        target: string,
    ): void {
        const relocate = (filePath: string | undefined): string | undefined => (
            filePath === undefined
                ? undefined
                : filePath === source || filePath.startsWith(`${source}/`)
                    ? `${target}${filePath.slice(source.length)}`
                    : filePath
        );
        record.artifactDirectory = target;
        record.baseFile = relocate(record.baseFile);
        record.localFile = relocate(record.localFile);
        record.remoteFile = relocate(record.remoteFile);
        record.retainedRemoteFile = relocate(record.retainedRemoteFile);
        if (record.documentRecoveries) {
            for (const recovery of record.documentRecoveries) {
                recovery.visibleFile = relocate(recovery.visibleFile);
                recovery.acknowledgedFile = relocate(recovery.acknowledgedFile);
            }
        }
    }

    private async localArtifactExists(filePath: string): Promise<boolean> {
        try {
            await vscode.workspace.fs.stat(this.localUri(filePath));
            return true;
        } catch (error) {
            if (this.isFileNotFound(error)) {
                return false;
            }
            throw error;
        }
    }

    private conflictSibling(filePath: string): string {
        const extension = path.posix.extname(filePath);
        const stem = extension ? filePath.slice(0, -extension.length) : filePath;
        return `${stem}.local-conflict-${this.timestamp()}${extension}`;
    }

    private async protectedResolvedArtifacts(): Promise<string[]> {
        const protectedArtifacts: string[] = [];
        for (const record of Object.values(this.requireState().conflicts)) {
            if (record.artifactDirectory.startsWith('.overleaf/conflicts/resolved/')) {
                protectedArtifacts.push(record.artifactDirectory);
                continue;
            }
            if (record.artifactDirectory.startsWith('.overleaf/conflicts/unresolved/')) {
                const crashRelocated = record.artifactDirectory.replace(
                    '.overleaf/conflicts/unresolved/',
                    '.overleaf/conflicts/resolved/',
                );
                if (await this.localArtifactExists(crashRelocated)) {
                    protectedArtifacts.push(crashRelocated);
                }
            }
        }
        return protectedArtifacts;
    }

    private async cleanupResolvedArtifacts(): Promise<void> {
        const protectedArtifacts = await this.protectedResolvedArtifacts();
        if (protectedArtifacts.length > 0) {
            vscode.window.showErrorMessage(
                'Resolved artifacts still contain active conflict recovery evidence; '
                + 'finish those conflicts before cleanup.',
            );
            return;
        }
        const confirmation = await vscode.window.showWarningMessage(
            'Permanently delete all resolved safe-sync artifacts?',
            { modal: true },
            'Delete resolved artifacts',
        );
        if (confirmation !== 'Delete resolved artifacts') {
            return;
        }
        await this.reconcileQueue.run(async () => {
            // Conflict resolution uses this same queue. Re-check only after
            // acquiring it because a resolution may durably rename active
            // evidence while the modal confirmation is open.
            if ((await this.protectedResolvedArtifacts()).length > 0) {
                vscode.window.showErrorMessage(
                    'Resolved artifacts gained active conflict recovery evidence; cleanup was cancelled.',
                );
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
        });
    }

    private handleReconcileError(error: unknown): void | Promise<void> {
        const normalized = this.asError(error);
        const actionPath = this.currentActionPath;
        if (normalized instanceof DetachedRecoveryPathAppearedError) {
            this.currentActionPath = undefined;
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, 0);
            return;
        }
        if (normalized instanceof DocumentRecoveryIsolationError) {
            this.currentActionPath = undefined;
            return this.registerIsolatedDocumentRecovery(normalized);
        }
        if (normalized instanceof LocalProjectionConflictError) {
            this.currentActionPath = undefined;
            const state = this.requireState();
            if (state.pendingAction?.path === normalized.filePath) {
                state.pendingAction = undefined;
                return this.stateStore.save(state).then(() => {
                    this.scheduleReconcile(normalized.message, 0);
                });
            }
            this.scheduleReconcile(normalized.message, 0);
            return;
        }
        if (normalized instanceof RetainedRemoteCandidateError) {
            this.currentActionPath = undefined;
            if (!actionPath || !this.lastScan) {
                this.failProject(normalized);
                return;
            }
            return this.isolateRetainedRemoteCandidate(
                actionPath,
                normalized,
                this.lastScan,
            );
        }
        if (normalized instanceof StaleLocalSnapshotError
            || normalized instanceof ConcurrentRemoteResourceError
            || (actionPath !== undefined && this.isMissingPathFailure(normalized))) {
            this.currentActionPath = undefined;
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, 100);
            return;
        }
        if (this.isRemoteCapacityFailure(normalized)) {
            this.currentActionPath = undefined;
            this.status = {
                status: 'need-attention',
                message: `Overleaf project is full: ${normalized.message}`,
            };
            this.output.appendLine(`[retry] ${normalized.message}`);
            this.scheduleReconcile(normalized.message, REMOTE_CAPACITY_RETRY_MS);
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

    private async isolateRetainedRemoteCandidate(
        filePath: string,
        error: RetainedRemoteCandidateError,
        scan: ScanPair,
    ): Promise<void> {
        const state = this.requireState();
        try {
            await this.createConflict(
                filePath,
                `A concurrently changed remote ${error.fileType} was retained under `
                    + `the safe-sync recovery namespace (${error.entityId})`,
                scan,
            );
            const record = state.conflicts[filePath];
            if (!record) {
                throw new Error(`Retained remote candidate has no conflict record: ${filePath}`);
            }
            record.retainedRemoteEntityId = error.entityId;
            record.retainedRemoteHash = error.archivedHash;
            record.retainedRemoteType = error.fileType;
            if (error.fileType !== 'folder') {
                const content = await this.vfs.readRetainedRemoteCandidate(error);
                const retainedHash = contentHash(content);
                const retainedFile = `${record.artifactDirectory}/retained-remote/${filePath}`;
                await this.writeArtifact(retainedFile, content);
                record.retainedRemoteFile = retainedFile;
                record.retainedRemoteHash = retainedHash;
            }
            await this.writeConflictManifest(record, {
                retainedRemoteCandidate: true,
                retainedRemoteHashAtFailure: error.archivedHash,
            });
            await this.stateStore.save(state);
            if (state.pendingAction?.path === filePath) {
                state.pendingAction = undefined;
                await this.stateStore.save(state);
            }
            this.status = {
                status: 'need-attention',
                message: `Retained remote conflict: ${filePath}`,
            };
        } catch (conflictError) {
            this.failProject(this.asError(conflictError));
        }
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
        if (actionPath !== undefined) {
            return false;
        }
        if (error instanceof WalCorruptionError) {
            return true;
        }
        // A missing entry is never a durable-state failure. The remote tree
        // moved under an unscoped read — a document deleted and recreated
        // under a new immutable id does exactly this — and the retry below
        // rescans both endpoints instead of stopping every other path.
        if (this.isMissingPathFailure(error)) {
            return false;
        }
        return /\bWAL\b|\bcheckpoint\b|\bbinding\b/i.test(this.durableStateText(error));
    }

    /**
     * Message text describing safe-sync's own durable state, with file
     * locations removed. A vscode.FileSystemError carries the offending URI
     * as its entire message and node:fs quotes the path in its own, so
     * classifying on the raw text decides project-wide fate from a file
     * name: a document called orphan-wal.tex read as WAL corruption and
     * paused every path in the project, and firewall.tex or walkthrough.tex
     * would have done the same to a user.
     */
    private durableStateText(error: Error): string {
        return error.message
            .replace(/[A-Za-z][A-Za-z0-9+.-]*:\/\/\S*/g, ' ')
            .replace(/'[^']*'/g, ' ')
            .replace(/(?:^|\s)\/\S*/g, ' ');
    }

    /**
     * A file that vanished under a text task describes a delete, not a lost OT
     * ancestry. Deleting a document while the plugin is projecting into it is
     * ordinary multi-endpoint work: the resource scan observes the deletion and
     * propagates it, where a conflict artifact would instead block the delete
     * and demand a manual resolution for a file the user meant to remove.
     */
    private isMissingPathFailure(error: Error): boolean {
        // `code` covers both Node errno failures and vscode.FileSystemError,
        // whose own code is a string on the same property.
        const code = (error as NodeJS.ErrnoException).code;
        return code === 'ENOENT' || code === 'ENOTDIR' || code === 'FileNotFound';
    }

    /**
     * A project that has reached its entity limit refuses every endpoint's
     * next write. That is one project-wide condition for the user to act on,
     * not a divergence at whichever path asked first: a conflict artifact here
     * would demand a manual resolution for content that never diverged, and
     * would keep demanding it after the project had room again.
     */
    private isRemoteCapacityFailure(error: Error): boolean {
        return /file limit/i.test(error.message);
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
            )
            // The Overleaf API reports a failed request as "<status>: <body>",
            // with no "HTTP" for the test above to find, so a 500 read as an
            // ordinary path failure. The server refusing one attempt is not a
            // divergence: isolating it archived a conflict and demanded a
            // manual resolution for content that never forked.
            || /(?:^|:\s)[45]\d\d:\s/.test(error.message);
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
        this.projectPauseEvents++;
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

    /**
     * Name the writer behind a baseline change on a reserved E2E path.
     *
     * The baseline is the common ancestor every later decision is measured
     * against, and several code paths may write it. When one records content
     * nobody authored, "the baseline is wrong" does not say which path put it
     * there, and the writers are far enough apart that guessing costs a full
     * live cycle each time.
     */
    private traceBaselineWrite(
        writer: string,
        filePath: string,
        entry: BaselineEntry | undefined,
    ): void {
        if (!/^hershel-e2e-/.test(filePath)) {
            return;
        }
        this.output.appendLine(`[baseline] ${writer} ${filePath} ${JSON.stringify({
            entityId: entry?.entityId,
            hash: entry?.hash?.slice(0, 12),
            textLength: entry?.text?.length,
        })}`);
    }

    private timestamp(): string {
        return new Date().toISOString().replace(/[:.]/g, '-');
    }

    private asError(error: unknown): Error {
        return error instanceof Error ? error : new Error(String(error));
    }

    private isFileNotFound(error: unknown): boolean {
        // Both filesystem APIs are used here: vscode.workspace.fs reports
        // FileNotFound, node:fs reports ENOENT. A guard that recognised only
        // the first turned an ordinary "the user deleted it" race into an
        // unresolvable conflict for callers that read through node:fs.
        return error instanceof Error && this.isMissingPathFailure(error);
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
        if (Object.keys(this.state?.detachedDocumentRecoveries ?? {}).length > 0) {
            items.splice(3, 0, {
                label: 'Open detached WAL recovery artifacts ...',
                callback: () => this.showDetachedDocumentRecovery(),
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

function planEditorReplacements(ancestor: string, target: string): EditorReplacement[] {
    const operation = TextOperation.fromDiff(ancestor, target);
    const replacements: EditorReplacement[] = [];
    let offset = 0;
    let pending: EditorReplacement | undefined;
    const flush = () => {
        if (pending) {
            replacements.push(pending);
            pending = undefined;
        }
    };
    for (const component of operation.components) {
        if (component.kind === 'retain') {
            flush();
            offset += component.count;
        } else if (component.kind === 'insert') {
            pending ??= { startOffset: offset, endOffset: offset, text: '' };
            pending.text += component.text;
        } else {
            pending ??= { startOffset: offset, endOffset: offset, text: '' };
            offset += component.count;
            pending.endOffset = offset;
        }
    }
    flush();
    return replacements;
}
