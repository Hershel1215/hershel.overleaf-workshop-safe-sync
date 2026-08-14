/* eslint-disable @typescript-eslint/naming-convention */
import * as vscode from 'vscode';
import { createHash, randomUUID } from 'crypto';
import * as path from 'path';
import { BaseAPI, Identity, MemberEntity, ProjectSettingsSchema } from '../api/base';
import { isSocketIOTimeout, SocketIOAPI, UpdateSchema } from '../api/socketio';
import { ELEGANT_NAME, OUTPUT_FOLDER_NAME, ROOT_NAME, URI_SCHEME } from '../consts';
import { GlobalStateManager } from '../utils/globalStateManager';
import { ClientManager } from '../collaboration/clientManager';
import { EventBus } from '../utils/eventBus';
import { SCMCollectionProvider } from '../scm/scmCollectionProvider';
import { ExtendedBaseAPI, ProjectLinkedFileProvider, UrlLinkedFileProvider } from '../api/extendedBase';
import { DocumentJournal } from '../sync/documentJournal';
import {
    AcceptedDocumentOperation,
    DocumentOperationInspection,
    DocumentSessionRetiredError,
    DurableDocumentSession,
    DurableLegacyDocumentUpdate,
} from '../sync/durableDocumentSession';
import { DocumentStateStore } from '../sync/documentStateStore';
import { contentHash } from '../sync/syncPolicy';
import { evaluateSafeSyncCapability } from '../sync/capabilityPolicy';
import { ReplicaLock } from '../sync/replicaLock';

const __OUTPUTS_ID = `${ROOT_NAME}-outputs`;
export const REMOTE_SAFE_SYNC_DIRECTORY = '.hershel-safe-sync';
export const REMOTE_SAFE_SYNC_OWNER_FILE = '.owner.json';
const REMOTE_SAFE_SYNC_STAGING_PREFIX = `${REMOTE_SAFE_SYNC_DIRECTORY}.staging-`;
const REMOTE_SAFE_SYNC_HISTORY_DIRECTORY = 'history';
const REMOTE_SAFE_SYNC_TRASH_DIRECTORY = 'trash';
const REMOTE_SAFE_SYNC_OWNER_SCHEMA_VERSION = 1;
/** Recent retained transactions kept regardless of age. */
const MAX_RETAINED_REMOTE_TRANSACTIONS = 24;
/** A full day of retained history before routine reclamation considers it. */
const RETAINED_REMOTE_TRANSACTION_AGE_MS = 24 * 60 * 60 * 1000;
/** No transaction of any endpoint is still being written into after this. */
const RETAINED_REMOTE_TRANSACTION_INFLIGHT_MS = 60 * 1000;
const RETAINED_REMOTE_PRUNE_BUDGET_MS = 2000;
/** Deletes per reclamation pass, so maintenance stays a trickle. */
const RETAINED_REMOTE_PRUNE_BATCH = 16;
const RETAINED_REMOTE_PRESSURED_PRUNE_BUDGET_MS = 10000;
const DOCUMENT_RECONNECT_RETRY_BASE_MS = 250;
const DOCUMENT_RECONNECT_RETRY_MAX_MS = 10000;
const REMOTE_MUTATION_OBSERVATION_TIMEOUT_MS = 5000;
const REMOTE_MUTATION_OBSERVATION_INTERVAL_MS = 50;
export const EMPTY_REMOTE_FOLDER_TREE_HASH = contentHash(
    new TextEncoder().encode('[]'),
);
const MISSING_REMOTE_FOLDER_TREE_HASH = contentHash(
    new TextEncoder().encode('missing-remote-folder'),
);

export type FileType = 'doc' | 'file' | 'folder' | 'outputs';
export type FolderKey = 'docs' | 'fileRefs' | 'folders' | 'outputs';
const FolderKeys: {[_type:string]: FolderKey} = {
    'folder': 'folders',
    'doc': 'docs',
    'file': 'fileRefs',
    'outputs': 'outputs',
};

export interface FileEntity {
    _id: string,
    name: string,
    _type?: FileType,
    readonly?: boolean,
}

export interface DocumentEntity extends FileEntity {
    version?: number,
    mtime?: number,
    lastVersion?: number,
    localCache?: string,
    remoteCache?: string,
}

export interface FileRefEntity extends FileEntity {
    linkedFileData: ProjectLinkedFileProvider | UrlLinkedFileProvider | null,
    created: string, //ISO date string
}

export interface OutputFileEntity extends FileEntity {
    path: string, //output file name
    url: string, // `project/${projectId}/user/${userId}/output/${build}/output/${path}`
    type: string, //output file type (postfix)
    build: string, //build id
}

export interface FolderEntity extends FileEntity {
    docs: Array<DocumentEntity>,
    fileRefs: Array<FileRefEntity>,
    folders: Array<FolderEntity>,
    outputs?: Array<OutputFileEntity>,
}

export interface ProjectEntity {
    _id: string,
    name: string,
    rootDoc_id: string,
    rootFolder: Array<FolderEntity>,
    publicAccessLevel: string, //"tokenBased"
    compiler: string,
    spellCheckLanguage: string,
    deletedDocs: Array<{
        _id: string,
        name: string,
        deletedAt: string,
    }>,
    members: Array<MemberEntity>,
    invites: Array<MemberEntity>,
    owner: MemberEntity,
    features: {[key:string]:any},
    settings: ProjectSettingsSchema,
}

export interface SafeSyncCapability {
    writable: boolean;
    protocol: 'legacy-ot-v1' | 'read-only';
    reason: string;
}

export interface AuthoritativeFileSnapshot {
    content: Uint8Array;
    entityId: string;
    fileType: 'doc' | 'file';
    revision?: number;
}

/** One journal-visible document state, published in OT transition order. */
export interface DocumentProjectionChange {
    content: string;
    documentId: string;
    path: string;
    revision: number;
    /**
     * Monotonic ordinal of the session state this content was read from.
     * A revision only advances when the server acknowledges, so a burst of
     * local operations publishes several distinct contents at one revision;
     * this is what tells a late consumer which of them is the newer.
     */
    sequence: number;
}

export interface CreatedTypedFile {
    entityId: string;
    content: Uint8Array;
}

export interface RetainedRemoteCandidateInspection {
    entityId: string;
    fileType: 'doc' | 'file' | 'folder';
    hash: string;
    content?: Uint8Array;
    retainedPath: string;
}

export interface PreparedDocumentRecovery {
    token: string;
    documentId: string;
    visibleContent: string;
    acknowledgedContent: string;
    revision: number;
    pendingOperationIds: string[];
    lastKnownPath?: string;
    epoch: number;
    hash: string;
}

export interface PendingDocumentRecovery extends Omit<PreparedDocumentRecovery, 'token'> {}

export interface IsolatedDocumentRecovery extends PendingDocumentRecovery {
    path: string;
    reason: string;
    failedAt: string;
}

interface DocumentRecoverySnapshot {
    visibleContent: string;
    acknowledgedContent: string;
    revision: number;
    pendingOperationIds: string[];
    lastKnownPath?: string;
}

interface RemoteSafeSyncOwnerMarker {
    schemaVersion: number;
    projectId: string;
    extensionId: string;
    extensionName: string;
    namespaceId: string;
}

export class ConcurrentRemoteResourceError extends Error {
    constructor(readonly path: string) {
        super(`Remote resource changed after the common baseline: ${path}`);
    }
}

export class RetainedRemoteCandidateError extends ConcurrentRemoteResourceError {
    constructor(
        readonly uri: vscode.Uri,
        readonly entityId: string,
        readonly archivedHash: string,
        readonly fileType: 'doc' | 'file' | 'folder',
    ) {
        super(uri.path);
        this.name = 'RetainedRemoteCandidateError';
        this.message = `Changed remote candidate ${entityId} was retained for ${uri.path}`;
    }
}

class DocumentSessionFactoryCleanupError extends Error {
    constructor(openError: unknown, closeError: unknown) {
        const opening = openError instanceof Error ? openError.message : String(openError);
        const closing = closeError instanceof Error ? closeError.message : String(closeError);
        super(`Document session failed (${opening}) and its WAL owner could not close (${closing})`);
    }
}

/** A server response was reachable but cannot prove the durable WAL ancestry. */
export class DocumentOtHistoryError extends Error {
    constructor(message: string, error?: unknown) {
        const detail = error === undefined
            ? message
            : `${message}: ${error instanceof Error ? error.message : String(error)}`;
        super(detail);
        this.name = 'DocumentOtHistoryError';
    }
}

/** One path is fenced because its durable OT history could not be proved. */
export class DocumentRecoveryIsolationError extends Error {
    constructor(
        readonly documentId: string,
        readonly path: string,
        readonly reason: string,
    ) {
        super(`Safe synchronization isolated ${path}: ${reason}`);
        this.name = 'DocumentRecoveryIsolationError';
    }
}

class DocumentRecoveryAdvancedError extends Error {}

class DocumentRecoveryPendingError extends Error {}

export class File implements vscode.FileStat {
    type: vscode.FileType;
    name: string;
    ctime: number;
    mtime: number;
    size: number;
    permissions?: vscode.FilePermission;
    constructor(name: string, type: vscode.FileType, ctime?: number, permissions?:vscode.FilePermission) {
        this.type = type;
        this.name = name;
        this.ctime = ctime || Date.now();
        this.mtime = Date.now();
        this.size = 0;
        this.permissions = permissions;
    }
}

export function parseUri(uri: vscode.Uri) {
    const query:any = uri.query.split('&').reduce((acc, v) => {
        const [key,value] = v.split('=');
        return {...acc, [key]:value};
    }, {});
    const [userId, projectId] = [query.user, query.project];
    const _pathParts = uri.path.split('/');
    const serverName = uri.authority;
    const projectName = decodeURIComponent(_pathParts[1]);
    const pathParts = _pathParts.splice(2);
    const identifier = `${userId}/${projectId}/${projectName}`;
    return {userId, projectId, serverName, projectName, identifier, pathParts};
}

/**
 * Retained transaction folders are named from the ISO instant that opened
 * them, so their creation time is readable without another round trip. A name
 * this cannot parse belongs to something else and is never reclaimed.
 */
function retainedRemoteTransactionTime(name: string): number | undefined {
    const stamp = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{3})-/.exec(name);
    if (!stamp) {
        return undefined;
    }
    const [year, month, day, hour, minute, second, millisecond] = stamp
        .slice(1)
        .map(part => Number(part));
    const parsed = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
    return Number.isFinite(parsed) ? parsed : undefined;
}

/** Overleaf refuses a new entity once the project reaches its file limit. */
function isRemoteEntityLimitFailure(error: unknown): boolean {
    return error instanceof Error && /file limit/i.test(error.message);
}

export class VirtualFileSystem extends vscode.Disposable {
    private root?: ProjectEntity;
    private currentVersion?: number;
    private context: vscode.ExtensionContext;
    private api: BaseAPI;
    private socket: SocketIOAPI;
    private publicId?: string;
    private userId: string;
    private isDirty: boolean = true;
    private initializing?: Promise<ProjectEntity>;
    private retirementPromise?: Promise<void>;
    private connectionReady = false;
    private retryConnection: number = 0;
    private retryTimer?: NodeJS.Timeout;
    /** Whether a "Reconnecting..." notification is currently shown */
    private reconnectingNotification: boolean = false;
    private retiring = false;
    /** Timestamp of last disconnect for debounce */
    private lastDisconnectTime: number = 0;
    /** Whether event handlers have been registered on the current socket */
    private handlersRegistered: boolean = false;
    private outputBuildId?: string;
    private compileGroup?: string;
    private clsiServerId?: string;
    private pdfDownloadDomain?: string;
    private readonly documentJournalDirectory: string;
    /** Session generations that have published at least one durable state. */
    private readonly documentProjectionSources = new WeakSet<
        Promise<DurableDocumentSession>
    >();
    private readonly documentProjectionListeners = new Set<
        (change: DocumentProjectionChange) => void
    >();
    private documentJournalLock?: ReplicaLock;
    private documentJournalLockPromise?: Promise<ReplicaLock>;
    private readonly legacyDocumentStateStore: DocumentStateStore;
    private readonly documentSessions = new Map<string, Promise<DurableDocumentSession>>();
    private readonly documentSessionFailures = new Map<string, Error>();
    private documentSessionRetryTimers = new Map<string, NodeJS.Timeout>();
    private documentSessionRetryAttempts = new Map<string, number>();
    /** Immutable ids whose failed WAL owner must be reopened after reconnect. */
    private documentSessionReopenIds = new Set<string>();
    /** Immutable ids currently spending their single fresh-generation proof. */
    private documentSessionFreshProofIds = new Set<string>();
    /** Terminal, path-scoped WAL recoveries waiting for Local Replica archival. */
    private documentRecoveryIsolations = new Map<string, {
        path: string;
        reason: string;
        failedAt: string;
    }>();
    private documentSessionsClosePromise?: Promise<void>;
    private readonly documentSessionClosures = new Set<Promise<void>>();
    private documentSessionCloseError?: Error;
    private readonly documentPendingOperations = new Map<string, boolean>();
    private logicalAcknowledgementCount = 0;
    private readonly documentRecoveryFences = new Set<string>();
    private readonly documentRecoveryDiscardTasks = new Map<string, Promise<void>>();
    private readonly logicalDocumentAcknowledgements = new Map<string, Set<Promise<string>>>();
    private readonly documentAcceptAdmissions = new Map<string, Set<Promise<void>>>();
    private readonly documentJournalEpochs = new Map<string, number>();
    private readonly preparedDocumentRecoveries = new Map<string, PreparedDocumentRecovery>();
    private readonly preparedDocumentRecoveryById = new Map<string, string>();
    private readonly committingPreparedDocumentRecoveries = new Set<string>();
    private remoteSafeSyncNamespacePromise?: Promise<FolderEntity>;
    /** One background reclamation at a time; never on an operation's path. */
    private retainedRemotePrune?: Promise<void>;
    private notify: (events:vscode.FileChangeEvent[])=>void;
    private clientManagerItem?: {manager: ClientManager, triggers: vscode.Disposable[]};
    private scmCollectionItem?: {collection: SCMCollectionProvider, triggers: vscode.Disposable[]};

    public readonly origin: vscode.Uri;
    public readonly projectName: string;
    public readonly serverName: string;
    public readonly projectId: string;

    constructor(context: vscode.ExtensionContext, uri: vscode.Uri, notify: (events:vscode.FileChangeEvent[])=>void) {
        // define the dispose behavior
        super(() => {
            // dispose all triggers of clientManager
            this.clientManagerItem?.triggers.forEach((trigger) => trigger.dispose());
            this.clientManagerItem = undefined;
            // dispose all triggers of scmCollection
            this.scmCollectionItem?.triggers.forEach((trigger) => trigger.dispose());
            this.scmCollectionItem = undefined;
            // disconnect socketio
            // this.socket.disconnect();
            this.resetDocumentSessions();
            this.documentProjectionListeners.clear();
        });

        const {userId,projectId,serverName,projectName} = parseUri(uri);
        this.serverName = serverName;
        this.projectName = projectName;
        this.origin = uri.with({path: '/'+projectName});
        this.userId = userId;
        this.projectId = projectId;
        this.context = context;
        this.notify = notify;
        const projectKey = createHash('sha256')
            .update(`${serverName}\0${projectId}`)
            .digest('hex');
        this.documentJournalDirectory = path.join(
            context.globalStorageUri.fsPath,
            'safe-sync-v2',
            projectKey,
        );
        this.legacyDocumentStateStore = new DocumentStateStore(context, serverName, projectId);

        const res = GlobalStateManager.initSocketIOAPI(this.context, this.serverName, projectId);
        if (res) {
            this.api = res.api;
            this.socket = res.socket;
        } else {
            throw new Error( vscode.l10n.t('Cannot init SocketIOAPI for {serverName}', {serverName}) );
        }
    }

    get _userId() {
        return this.userId;
    }

    /**
     * Retire the remote socket and durable document sessions before a second
     * extension host acquires the local-replica writer lease.
     */
    async retireForReplicaHandoff(): Promise<void> {
        if (this.retirementPromise) {
            return this.retirementPromise;
        }
        this.retiring = true;
        this.connectionReady = false;
        const initializing = this.initializing;
        // Publish the terminal promise before disposing managers. Their dispose
        // callbacks can re-enter VFS retirement through the owning SCM.
        const retirement = Promise.resolve().then(async () => {
            if (this.retryTimer) {
                clearTimeout(this.retryTimer);
                this.retryTimer = undefined;
            }
            this.disposeRuntimeManagers();
            this.socket.retire();
            const results = await Promise.allSettled([
                initializing?.then(() => undefined, () => undefined),
                this.drainDocumentSessions(),
            ]);
            const failure = results.find(
                (result): result is PromiseRejectedResult => result.status === 'rejected',
            );
            if (failure) {
                throw failure.reason;
            }
            await this.releaseDocumentJournalOwnership();
            if (this.initializing === initializing) {
                this.initializing = undefined;
            }
            // Retiring is terminal. Do not leave an initialized project graph
            // reachable after the local-replica writer lease is released.
            this.root = undefined;
            this.disposeRuntimeManagers();
        });
        this.retirementPromise = retirement;
        return retirement;
    }

    async init() : Promise<ProjectEntity> {
        if (this.retiring) {
            throw new Error('Remote project session is retiring for replica handoff');
        }
        // A membership RPC that timed out invalidates the socket without
        // disconnecting the project: root stays resolved and connectionReady
        // stays true. Readiness that ignores the transport therefore answers
        // "ready" forever on a socket that refuses every joinDoc, and callers
        // that treat the refusal as transient retry against it indefinitely.
        // An invalidated transport is not ready, and the reconnect it needs
        // has to be a fresh attempt: an initializing promise that settled
        // before the invalidation would resolve immediately and rebuild
        // nothing.
        const transportInvalidated = this.socket.needsReinit;
        if (this.root && this.connectionReady && !transportInvalidated) {
            return Promise.resolve(this.root);
        }

        if (!this.initializing || transportInvalidated) {
            this.initializing = this.initializingPromise;
        }
        return this.initializing;
    }

    private get initializingPromise(): Promise<ProjectEntity> {
        const MAX_RETRIES = 5;
        const BASE_DELAY_MS = 1000; // 1 second base delay

        this.assertNotRetiring();

        // if retry connection exhausted, show error
        if (this.retryConnection >= MAX_RETRIES) {
            this.retryConnection = 0;
            this.initializing = undefined;
            this.reconnectingNotification = false;
            vscode.window.showErrorMessage(
                vscode.l10n.t('Connection lost: {serverName}', {serverName:this.serverName}),
                vscode.l10n.t('Reload'),
                vscode.l10n.t('Retry'),
            ).then((choice) => {
                if (this.retiring) {
                    return;
                }
                if (choice === vscode.l10n.t('Reload')) {
                    vscode.commands.executeCommand("workbench.action.reloadWindow");
                } else if (choice === vscode.l10n.t('Retry')) {
                    this.retryConnection = 0;
                    this.handlersRegistered = false;
                    this.socket.init(); // Recreate socket after all auto-reconnect attempts exhausted
                    this.initializing = this.initializingPromise;
                    this.init().catch(() => {});
                }
            });
            throw new Error( vscode.l10n.t('Connection lost') );
        }

        // exponential backoff delay: 1s, 2s, 4s, 8s, 16s
        const delayMs = this.retryConnection > 0 ? Math.min(BASE_DELAY_MS * Math.pow(2, this.retryConnection - 1), 16000) : 0;

        // Show reconnecting notification on first retry
        if (this.retryConnection === 1 && !this.reconnectingNotification) {
            this.reconnectingNotification = true;
            vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: vscode.l10n.t('Reconnecting to {serverName}...', {serverName:this.serverName}),
                cancellable: false,
            }, async () => {
                // Keep the notification visible while reconnecting
                await new Promise<void>((resolve) => {
                    const check = () => {
                        if (this.retiring || this.root || this.retryConnection >= MAX_RETRIES) {
                            this.reconnectingNotification = false;
                            resolve();
                        } else {
                            setTimeout(check, 500);
                        }
                    };
                    check();
                });
            });
        }

        // Wait for backoff delay before retrying
        const attemptReconnect = async (): Promise<ProjectEntity> => {
            if (this.retiring) {
                throw new Error('Remote project session is retiring for replica handoff');
            }
            if (delayMs > 0) {
                await new Promise(resolve => setTimeout(resolve, delayMs));
            }
            if (this.retiring) {
                throw new Error('Remote project session is retiring for replica handoff');
            }

            // Only recreate the socket when the connection scheme has changed
            // (e.g., v1→v2 after connectionRejected). For transient disconnects,
            // socket.io's built-in auto-reconnect handles re-establishing the TCP
            // connection without creating a new one — avoiding TCP RST packets.
            if (this.socket.needsReinit) {
                this.socket.init();
                this.handlersRegistered = false;
            }

            // Register event handlers once on the current socket
            if (!this.handlersRegistered) {
                this.remoteWatch();
                this.handlersRegistered = true;
            }

            this.root = undefined;
            try {
                const project = await this.socket.joinProject(this.projectId);
                this.assertNotRetiring();
                // Reset retry counter on success
                this.retryConnection = 0;
                this.reconnectingNotification = false;
                // fetch project settings
                const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
                this.assertNotRetiring();
                const settings = await this.api.getProjectSettings(identity, this.projectId);
                this.assertNotRetiring();
                project.settings = settings.settings!;
                this.root = project;
                await this.resumeDocumentSessionTransports();
                this.assertNotRetiring();
                this.connectionReady = true;
                const activeCondition = (vscode.workspace.workspaceFolders===undefined) || (vscode.workspace.workspaceFolders?.[0].uri.scheme!==URI_SCHEME) || (vscode.workspace.workspaceFolders?.[0].uri===this.origin);
                // Register: [collaboration] ClientManager on Statusbar
                if (activeCondition && !this.clientManagerItem) {
                    const clientManager = new ClientManager(this, this.context, this.publicId||'', this.socket);
                    this.clientManagerItem = {
                        manager: clientManager,
                        triggers: clientManager.triggers,
                    };
                }
                // Register: [scm] SCMCollectionProvider in explorer
                if (activeCondition && !this.scmCollectionItem) {
                    const scmCollection = new SCMCollectionProvider(this, this.context);
                    this.scmCollectionItem = {
                        collection: scmCollection,
                        triggers: scmCollection.triggers,
                    };
                }
                // trigger the first compile
                vscode.commands.executeCommand(`${ROOT_NAME}.compileManager.compile`);
                return project;
            } catch {
                if (this.retiring) {
                    throw this.retirementError();
                }
                this.connectionReady = false;
                this.root = undefined;
                this.retryConnection += 1;
                return this.initializingPromise;
            }
        };

        return attemptReconnect();
    }

    private retirementError(): Error {
        return new Error('Remote project session is retiring for replica handoff');
    }

    private assertNotRetiring(): void {
        if (this.retiring) {
            throw this.retirementError();
        }
    }

    private disposeRuntimeManagers(): void {
        this.clientManagerItem?.triggers.forEach(trigger => trigger.dispose());
        this.clientManagerItem = undefined;
        this.scmCollectionItem?.triggers.forEach(trigger => trigger.dispose());
        this.scmCollectionItem = undefined;
    }

    get isInvisibleMode() {
        return this.socket.isUsingAlternativeConnectionScheme;
    }

    toggleInvisibleMode() {
        // Clear disconnect debounce to prevent false retry trigger during mode switch
        this.lastDisconnectTime = 0;
        this.handlersRegistered = false; // Will re-register on the new socket scheme
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
            this.retryTimer = undefined;
        }
        this.socket.toggleAlternativeConnectionScheme(this.origin.toString(), this.root);
        this.socket.disconnect(); // jump to `onDisconnected` handler
    }

    async _resolveUri(uri: vscode.Uri) {
        // resolve path
        const [parentFolder, fileName] = await (async () => {
            const {pathParts} = parseUri(uri);
            const root = await this.init();

            let currentFolder = root.rootFolder[0];
            for (let i = 0; i < pathParts.length-1; i++) {
                const folderName = pathParts[i];
                const folder = currentFolder.folders.find((folder) => folder.name === folderName);
                if (folder) {
                    currentFolder = folder;
                } else {
                    throw vscode.FileSystemError.FileNotFound(uri);
                }
            }
            const fileName = pathParts[pathParts.length-1];
            return [currentFolder, fileName];
        })();
        // resolve file
        const [fileEntity, fileType, fileId] = (() => {
            for (const _type of Object.keys(FolderKeys)) {
                let entity = parentFolder[ FolderKeys[_type] ]?.find((entity) => entity.name === fileName);
                if (!fileName && _type==='folder') { entity = parentFolder; }
                if (entity) {
                    return [entity, _type as FileType, entity._id];
                }
            }
            return [];
        })();
        return {parentFolder, fileName, fileEntity, fileType, fileId};
    }

    _resolveById(entityId: string, root?: FolderEntity, path?:string):{
        parentFolder: FolderEntity, fileEntity: FileEntity, fileType:FileType, path:string
    } | undefined {
        if (!this.root) {
            throw vscode.FileSystemError.FileNotFound();
        }
        root = root || this.root.rootFolder[0];
        path = path || '/';

        if (root._id === entityId) {
            return {parentFolder: root, fileType: 'folder', fileEntity: root, path};
        } else {
            // search files in root
            for (const _type of Object.keys(FolderKeys)) {
                const key = FolderKeys[_type];
                if (key==='folders') { continue; }
                const entity = root[key]?.find((entity) => entity._id === entityId);
                if (entity) {
                    return {parentFolder: root, fileType: _type as FileType, fileEntity: entity, path:path+entity.name};
                }
            }
            // recursive search
            for (const folder of root.folders) {
                const res = this._resolveById(entityId, folder, path+folder.name+'/');
                if (res) { return res; }
            }
        }
        return undefined;
    }

    walk(filter:(entity:FileEntity)=>boolean): {entity:FileEntity, path:string}[] {
        const result = [];
        const folders = this.root ? [{entity:this.root.rootFolder[0], path:'/'}] : [];

        // apply filter to root folder
        filter(folders[0].entity) && result.push(folders[0]);
        // walk through all folders
        for (const folder of folders) {
            for (const [key,value] of Object.entries(FolderKeys)) {
                if (value==='folders') {
                    folder.entity[value]?.forEach((entity) => {
                        folders.push({entity, path:folder.path+entity.name+'/'});
                    });
                }
                folder.entity[value]?.forEach((entity) => {
                    entity._type = key as FileType;
                    filter(entity) && result.push({ entity, path:folder.path+entity.name });
                });
            };
        }

        return result;
    }

    private insertEntity(parentFolder: FolderEntity, fileType:FileType, entity: FileEntity) {
        const key = FolderKeys[fileType];
        const index = parentFolder[key]?.findIndex((e) => e._id === entity._id);
        if (index===undefined || index<0) {
            parentFolder[key]?.push(entity as any);
        }
    }

    private removeEntity(parentFolder: FolderEntity, fileType:FileType, entity: FileEntity) {
        const key = FolderKeys[fileType];
        const index = parentFolder[key]?.findIndex((e) => e._id === entity._id);
        if (index!==undefined && index>=0) {
            parentFolder[key]?.splice(index, 1);
            return true;
        } else {
            return false;
        }
    }

    private removeEntityById(parentFolder: FolderEntity, fileType:FileType, entityId: string, recursive?:boolean) {
        const key = FolderKeys[fileType];
        const index = parentFolder[key]?.findIndex((e) => e._id === entityId);
        if (index!==undefined && index>=0) {
            parentFolder[key]?.splice(index, 1);
            return true;
        } else {
            return false;
        }
    }

    private remoteWatch(): void {
        this.socket.updateEventHandlers({
            onDisconnected: () => {
                if (this.retiring) {
                    return;
                }
                // Bypass the first initialization, which has no session to
                // rebuild. An invalidated transport is not that case: a timed
                // out membership RPC disconnects the socket precisely so this
                // handler can rebuild it, and the same failure has usually
                // already cleared root. Returning here would leave the socket
                // poisoned, and every later membership RPC rejects instantly
                // against that flag, so callers that treat the rejection as
                // transient retry forever without anything reconnecting.
                if (this.root === undefined && !this.socket.needsReinit) { return; }
                console.log("Disconnected");
                this.connectionReady = false;
                this.suspendDocumentSessionTransports();
                this.root = undefined;
                // Debounce: ignore rapid disconnect/reconnect cycles (within 2 seconds)
                const now = Date.now();
                if (now - this.lastDisconnectTime < 2000 && !this.socket.needsReinit) {
                    console.log("Disconnected: debounced (too soon since last disconnect)");
                    return;
                }
                this.lastDisconnectTime = now;
                // Clear any pending retry timer
                if (this.retryTimer) {
                    clearTimeout(this.retryTimer);
                }
                // Publish the reconnect promise immediately as a readiness
                // barrier. Its retry counter supplies the 1s backoff, while
                // document operations wait instead of opening a session on
                // the disconnected socket or resolved old project promise.
                this.retryConnection += 1;
                const previousInitialization = this.initializing;
                const reconnect = Promise.resolve(previousInitialization)
                    .catch(() => undefined)
                    .then(() => {
                        if (this.retiring) {
                            throw this.retirementError();
                        }
                        if (this.root && this.connectionReady && !this.socket.needsReinit) {
                            return this.root;
                        }
                        return this.initializingPromise;
                    });
                this.initializing = reconnect;
                void reconnect.catch(() => {});
            },
            onConnectionAccepted: (publicId:string) => {
                if (this.retiring) {
                    return;
                }
                this.retryConnection = 0;
                this.reconnectingNotification = false;
                this.lastDisconnectTime = 0;
                if (this.retryTimer) {
                    clearTimeout(this.retryTimer);
                    this.retryTimer = undefined;
                }
                this.publicId = publicId;
            },
            onFileCreated: (parentFolderId:string, type:FileType, entity:FileEntity) => {
                const res = this._resolveById(parentFolderId);
                if (res) {
                    const {fileEntity,path} = res;
                    const entityPath = path + entity.name;
                    this.insertEntity(fileEntity as FolderEntity, type, entity);
                    this.notify([
                        {type: vscode.FileChangeType.Created, uri: this.pathToUri(entityPath)}
                    ]);
                }
            },
            onFileRenamed: (entityId:string, newName:string) => {
                const res = this._resolveById(entityId);
                if (res) {
                    const {fileEntity} = res;
                    const oldName = fileEntity.name;
                    const newPath = `${res.path.slice(0, -oldName.length)}${newName}`;
                    fileEntity.name = newName;
                    if (res.fileType === 'doc') {
                        this.bindLiveDocumentPath(entityId, newPath);
                    }
                    this.notify([
                        {type: vscode.FileChangeType.Deleted, uri: this.pathToUri(res.path)},
                        {type: vscode.FileChangeType.Created, uri: this.pathToUri(newPath)}
                    ]);
                }
            },
            onFileRemoved: (entityId:string) => {
                const res = this._resolveById(entityId);
                if (res) {
                    const {parentFolder, fileType, fileEntity} = res;
                    const session = this.documentSessions.get(entityId);
                    this.documentSessions.delete(entityId);
                    this.documentSessionFailures.delete(entityId);
                    this.documentSessionReopenIds?.delete(entityId);
                    this.documentSessionFreshProofIds?.delete(entityId);
                    this.cancelDocumentSessionRetry(entityId);
                    if (session) {
                        void this.trackDocumentSessionClose(session).catch(error => {
                            console.error(
                                `[hershel-safe-sync] failed to close removed document ${entityId}`,
                                error,
                            );
                        });
                    }
                    // Keep the WAL until Local Replica reconciliation proves
                    // whether this remote delete races a journaled local edit.
                    this.removeEntity(parentFolder, fileType, fileEntity);
                    this.notify([
                        {type: vscode.FileChangeType.Deleted, uri: this.pathToUri(res.path)}
                    ]);
                }
            },
            onFileMoved: (entityId:string, folderId:string) => {
                this.applyRemoteMove(entityId, folderId);
            },
            onFileChanged: (update:UpdateSchema) => {
                const res = this._resolveById(update.doc);
                if (res===undefined) { return; }

                const doc = res.fileEntity as DocumentEntity;
                const session = this.documentSessions.get(update.doc);
                if (session) {
                    session
                        .then(value => value.receiveUpdate(update))
                        .catch(error => {
                            console.error(`[hershel-safe-sync] failed to apply update for ${res.path}`, error);
                        });
                } else {
                    // An unopened document has no local operations. Invalidate
                    // its caches and fetch one authoritative snapshot on demand.
                    doc.remoteCache = undefined;
                    doc.localCache = undefined;
                    doc.version = undefined;
                    if (update.op) {
                        this.notify([
                            {type: vscode.FileChangeType.Changed, uri: this.pathToUri(res.path)}
                        ]);
                    }
                }
            },
            onSpellCheckLanguageUpdated: (language:string) => {
                if (this.root) {
                    this.root.spellCheckLanguage = language;
                    EventBus.fire('spellCheckLanguageUpdateEvent', {language});
                }
            },
            onCompilerUpdated: (compiler:string) => {
                if (this.root) {
                    this.root.compiler = compiler;
                    EventBus.fire('compilerUpdateEvent', {compiler});
                }
            },
            onRootDocUpdated: (rootDocId:string) => {
                //NOTE: do not sync rootDocId
                // if (this.root) {
                //     this.root.rootDoc_id = rootDocId;
                //     EventBus.fire('rootDocUpdateEvent', {rootDocId});
                // }
            },
        });
    }

    private applyRemoteMove(entityId: string, folderId: string): void {
        const oldPath = this._resolveById(entityId);
        const destination = this._resolveById(folderId);
        if (!oldPath || !destination || destination.fileType !== 'folder') {
            return;
        }
        const newParentFolder = destination.fileEntity as FolderEntity;
        if (oldPath.parentFolder._id === newParentFolder._id) {
            return;
        }
        this.removeEntity(oldPath.parentFolder, oldPath.fileType, oldPath.fileEntity);
        this.insertEntity(newParentFolder, oldPath.fileType, oldPath.fileEntity);
        const newPath = path.posix.join(destination.path, oldPath.fileEntity.name);
        if (oldPath.fileType === 'doc') {
            this.bindLiveDocumentPath(entityId, newPath);
        }
        this.notify([
            {type: vscode.FileChangeType.Deleted, uri: this.pathToUri(oldPath.path)},
            {
                type: vscode.FileChangeType.Created,
                uri: this.pathToUri(newPath),
            },
        ]);
    }

    private bindLiveDocumentPath(documentId: string, path: string): void {
        if (this.normalizeRemoteRelativePath(path).startsWith(`${REMOTE_SAFE_SYNC_DIRECTORY}/`)) {
            return;
        }
        const registered = this.documentSessions.get(documentId);
        if (!registered) {
            return;
        }
        void registered
            .then(session => session.bindPath(path))
            .catch(error => {
                console.error(
                    `[hershel-safe-sync] failed to bind moved document ${documentId} to ${path}`,
                    error,
                );
            });
    }

    pathToUri(...path: string[]): vscode.Uri {
        return vscode.Uri.joinPath(this.origin, ...path);
    }

    /** Subscribe without routing ordered text revisions through VS Code's coalescing watcher. */
    onDidChangeDocumentProjection(
        listener: (change: DocumentProjectionChange) => void,
    ): vscode.Disposable {
        this.documentProjectionListeners.add(listener);
        return {
            dispose: () => this.documentProjectionListeners.delete(listener),
        };
    }

    /** Whether Changed events for this entity already have an ordered direct source. */
    hasLiveDocumentProjection(documentId: string | undefined): boolean {
        if (documentId === undefined || this.documentSessionFailures.has(documentId)) {
            return false;
        }
        const session = this.documentSessions.get(documentId);
        return session !== undefined && this.documentProjectionSources.has(session);
    }

    private publishDocumentProjection(change: DocumentProjectionChange): void {
        for (const listener of this.documentProjectionListeners) {
            try {
                listener(change);
            } catch (error) {
                // An observer must never break the durable OT transition that
                // produced this projection. Async observers own their errors.
                console.error('[hershel-safe-sync] document projection observer failed', error);
            }
        }
    }

    get supportsRealtimeTextOt(): boolean {
        return !this.socket.isUsingAlternativeConnectionScheme;
    }

    get safeSyncCapability(): SafeSyncCapability {
        return evaluateSafeSyncCapability(
            this.serverName,
            this.supportsRealtimeTextOt,
            this.root?.features?.hershelSafeSyncProtocol,
        );
    }

    get isSafeSyncConnectionReady(): boolean {
        return this.connectionReady
            && !this.retiring
            && this.documentSessionFailures.size === 0
            && (this.documentSessionRetryAttempts?.size ?? 0) === 0
            && (this.documentSessionReopenIds?.size ?? 0) === 0
            && this.documentSessionCloseError === undefined
            && this.documentSessionsClosePromise === undefined
            && this.documentSessionClosures.size === 0;
    }

    get hasPendingDocumentOperations(): boolean {
        if (
            this.logicalAcknowledgementCount > 0
            || [...this.documentPendingOperations.values()].some(Boolean)
            || [...this.documentAcceptAdmissions.values()].some(admissions => admissions.size > 0)
            || this.documentSessionsClosePromise !== undefined
            || this.documentSessionClosures.size > 0
            || this.documentSessionCloseError !== undefined
        ) {
            return true;
        }
        const roots = this.root?.rootFolder ?? [];
        const folders = [...roots];
        for (let index = 0; index < folders.length; index++) {
            const folder = folders[index];
            folders.push(...folder.folders);
            if (folder.docs.some(doc => (
                doc.localCache !== undefined
                && doc.localCache !== doc.remoteCache
            ))) {
                return true;
            }
        }
        return false;
    }

    private async getDocumentSession(
        doc: DocumentEntity,
        path: string,
    ): Promise<DurableDocumentSession> {
        this.assertDocumentRecoveryAvailable(doc._id);
        this.assertDocumentSessionCloseHealthy();
        if (this.retiring) {
            throw new Error('Remote document session is retiring for replica handoff');
        }
        // A reconnect reuses the same WAL. Never open its next owner until the
        // previous session has checkpointed and closed the file descriptor.
        while (this.documentSessionsClosePromise) {
            await this.documentSessionsClosePromise;
            this.assertDocumentRecoveryAvailable(doc._id);
            this.assertDocumentSessionCloseHealthy();
            if (this.retiring) {
                throw new Error('Remote document session is retiring for replica handoff');
            }
        }
        await this.waitForDetachedDocumentSessionClosures();
        this.assertDocumentRecoveryAvailable(doc._id);
        const existing = this.documentSessions.get(doc._id);
            if (existing) {
                const session = await existing;
            const failure = this.documentSessionFailures.get(doc._id);
            if (failure || session.isFailed) {
                if (this.documentSessions.get(doc._id) === existing) {
                    this.documentSessions.delete(doc._id);
                    this.cancelDocumentSessionRetry(doc._id);
                    await this.trackDocumentSessionClose(existing);
                } else {
                    await this.waitForDetachedDocumentSessionClosures();
                }
                throw failure ?? new Error(`Document session failed for ${doc._id}`);
            }
            this.assertDocumentRecoveryAvailable(doc._id);
                if (!this.connectionReady) {
                    session.suspendTransport();
                }
                await session.bindPath(path);
                return session;
        }
        // An invalidated transport still reports connectionReady, because the
        // project socket was never disconnected — only its membership epoch
        // became unknowable. Opening a session on it would join a socket that
        // refuses every joinDoc, so rebuild first.
        if (!this.connectionReady || this.socket.needsReinit) {
            await this.init();
        }
        this.assertDocumentRecoveryAvailable(doc._id);
        this.assertDocumentSessionCloseHealthy();
        if (this.retiring || !this.connectionReady) {
            throw new Error('Remote document session is waiting for project reconnection');
        }
        const reconnected = this.documentSessions.get(doc._id);
        if (reconnected) {
            return this.getDocumentSession(doc, path);
        }

        let created!: Promise<DurableDocumentSession>;
        created = (async () => {
            this.assertDocumentRecoveryAvailable(doc._id);
            if (!this.supportsRealtimeTextOt) {
                throw new Error('Safe text synchronization requires the realtime Overleaf OT protocol');
            }
            let journal: DocumentJournal | undefined;
            try {
                await this.acquireDocumentJournalOwnership();
                this.assertDocumentRecoveryAvailable(doc._id);
                journal = await DocumentJournal.openExisting(this.documentJournalDirectory, doc._id);
                const legacy = journal
                    ? undefined
                    : await this.legacyDocumentStateStore.load(doc._id);
                const recoveringExistingState = journal !== undefined || legacy !== undefined;
                if (journal) {
                    await journal.bindPath(path, true);
                }
                let joined;
                let catchUpUpdates: DurableLegacyDocumentUpdate[] | undefined;
                if (recoveringExistingState) {
                    const recoveryRevision = journal?.revision ?? legacy!.state.revision;
                    joined = await this.socket.joinDoc(doc._id, recoveryRevision);
                    if (!Array.isArray(joined.updates)) {
                        throw new DocumentOtHistoryError(
                            'Server did not return an OT catch-up operation list',
                        );
                    }
                    const updates = joined.updates as DurableLegacyDocumentUpdate[];
                    catchUpUpdates = updates;
                    if (joined.version - recoveryRevision !== updates.length) {
                        throw new DocumentOtHistoryError(
                            `Server returned ${updates.length} catch-up operations for `
                            + `${joined.version - recoveryRevision} revisions`,
                        );
                    }
                } else {
                    joined = await this.socket.joinDoc(doc._id);
                    journal = await DocumentJournal.open({
                        directory: this.documentJournalDirectory,
                        documentId: doc._id,
                        initialRevision: joined.version,
                        initialContent: joined.docLines.join('\n'),
                        initialPath: path,
                    });
                }
                if (legacy) {
                    journal = await DocumentJournal.open({
                        directory: this.documentJournalDirectory,
                        documentId: doc._id,
                        initialRevision: legacy.state.revision,
                        initialContent: legacy.state.serverContent,
                        initialPath: path,
                    });
                    await journal.seedLegacyState(legacy.state);
                }
                if (!journal) {
                    throw new Error(`Document journal was not initialized for ${doc._id}`);
                }
                const serverContent = joined.docLines.join('\n');
                this.assertDocumentRecoveryAvailable(doc._id);
                if (joined.otType !== 'sharejs-text-ot') {
                    const message = `Unsupported Overleaf document OT type: ${joined.otType}`;
                    throw recoveringExistingState
                        ? new DocumentOtHistoryError(message)
                        : new Error(message);
                }
                let session: DurableDocumentSession;
                try {
                    session = await DurableDocumentSession.create({
                        documentId: doc._id,
                        authoritativeContent: serverContent,
                        authoritativeRevision: joined.version,
                        journal,
                        catchUpUpdates,
                        sendUpdate: update => this.socket.applyOtUpdate(doc._id, update as UpdateSchema),
                        getSourceId: () => this.publicId,
                        createOperationId: () => `${this.projectId}:${doc._id}:${randomUUID()}`,
                        onChange: (
                            content,
                            revision,
                            acknowledgedContent,
                            hasPendingOperations,
                            sequence,
                        ) => {
                            doc.version = revision;
                            doc.localCache = content;
                            doc.remoteCache = acknowledgedContent;
                            this.documentPendingOperations.set(doc._id, hasPendingOperations);
                            this.isDirty = true;
                            const currentPath = this.root
                                ? this._resolveById(doc._id)?.path ?? path
                                : path;
                            this.documentProjectionSources.add(created);
                            this.publishDocumentProjection({
                                content,
                                documentId: doc._id,
                                path: currentPath,
                                revision,
                                sequence,
                            });
                            this.notify([
                                {type: vscode.FileChangeType.Changed, uri: this.pathToUri(currentPath)}
                            ]);
                        },
                        onError: error => {
                            if (this.documentSessions.get(doc._id) === created) {
                                this.documentSessionFailures.set(doc._id, error);
                            }
                            doc.localCache = undefined;
                            doc.remoteCache = undefined;
                            console.error(`[hershel-safe-sync] document ${doc._id} isolated`, error);
                            const currentPath = this.root
                                ? this._resolveById(doc._id)?.path ?? path
                                : path;
                            vscode.window.showWarningMessage(
                                vscode.l10n.t('Safe synchronization isolated {path}: {message}', {
                                    path: currentPath,
                                    message: error.message,
                                })
                            );
                        },
                    });
                } catch (error) {
                    throw recoveringExistingState
                        && !(error instanceof DocumentOtHistoryError)
                        && !(error instanceof DocumentSessionRetiredError)
                        && !(error instanceof DocumentRecoveryIsolationError)
                        ? new DocumentOtHistoryError(
                            'Durable OT history could not reproduce the server snapshot',
                            error,
                        )
                        : error;
                }
                journal = undefined;
                return session;
            } catch (error) {
                if (journal) {
                    try {
                        await journal.close();
                    } catch (closeError) {
                        throw new DocumentSessionFactoryCleanupError(error, closeError);
                    }
                }
                throw error;
            }
        })();
        this.documentSessions.set(doc._id, created);
        void created.catch(error => {
            if (
                !(error instanceof DocumentSessionRetiredError)
                && this.documentSessions.get(doc._id) === created
            ) {
                this.documentSessionFailures.set(
                    doc._id,
                    error instanceof Error ? error : new Error(String(error)),
                );
            }
            // The factory closes every journal it acquired before an ordinary
            // rejection. Let the next read retry instead of pinning one
            // transient join/authentication failure until a full reconnect.
            // A cleanup failure is different: its WAL ownership is uncertain,
            // so keep that rejection fail-closed.
            if (
                !(error instanceof DocumentSessionFactoryCleanupError)
                && this.documentSessions.get(doc._id) === created
            ) {
                this.documentSessions.delete(doc._id);
                if (!this.documentSessionReopenIds?.has(doc._id)) {
                    this.cancelDocumentSessionRetry(doc._id);
                }
            }
        });
        let session: DurableDocumentSession;
        try {
            session = await created;
        } catch (error) {
            if (error instanceof DocumentOtHistoryError) {
                this.isolateDocumentRecovery(doc._id, path, error);
                throw this.documentRecoveryIsolationError(doc._id);
            }
            throw error;
        }
        this.assertDocumentRecoveryAvailable(doc._id);
        if (this.documentSessions.get(doc._id) === created) {
            this.documentSessionFailures.delete(doc._id);
            this.documentSessionFreshProofIds?.delete(doc._id);
            this.cancelDocumentSessionRetry(doc._id);
        }
        return session;
    }

    private suspendDocumentSessionTransports(): void {
        for (const session of this.documentSessions.values()) {
            void session.then(value => value.suspendTransport()).catch(error => {
                console.error('[hershel-safe-sync] failed to suspend document transport', error);
            });
        }
    }

    private async resumeDocumentSessionTransports(): Promise<void> {
        await Promise.all([...this.documentSessions.entries()].map(
            ([documentId, pendingSession]) => this.resumeDocumentSessionTransport(
                documentId,
                pendingSession,
            ),
        ));
        if (this.socket.needsReinit) {
            throw new Error('Document membership transport was invalidated during project recovery');
        }
    }

    private async resumeDocumentSessionTransport(
        documentId: string,
        pendingSession: Promise<DurableDocumentSession>,
    ): Promise<void> {
        let session: DurableDocumentSession | undefined;
        try {
            session = await pendingSession;
            if (!this.root || this.socket.needsReinit) {
                session.suspendTransport();
                return;
            }
            const resolved = this.root ? this._resolveById(documentId) : undefined;
            if (resolved?.fileType !== 'doc') {
                // The reconnect snapshot deleted or replaced this immutable
                // document id. Retire only its transport generation and
                // leave the journal for Local Replica recovery/adjudication.
                if (this.documentSessions.get(documentId) === pendingSession) {
                    this.documentSessions.delete(documentId);
                }
                const hasPendingOperations = session.hasPendingOperations;
                await this.trackDocumentSessionClose(Promise.resolve(session));
                if (!hasPendingOperations) {
                    this.documentPendingOperations.delete(documentId);
                }
                this.documentSessionFailures.delete(documentId);
                this.documentSessionReopenIds?.delete(documentId);
                this.cancelDocumentSessionRetry(documentId);
                return;
            }
            const recoveryRevision = session.revision;
            const joined = await this.socket.joinDoc(documentId, recoveryRevision);
            if (joined.otType !== 'sharejs-text-ot') {
                throw new DocumentOtHistoryError(
                    `Unsupported Overleaf document OT type: ${joined.otType}`,
                );
            }
            if (!Array.isArray(joined.updates)) {
                throw new DocumentOtHistoryError(
                    'Server did not return an OT catch-up operation list',
                );
            }
            const updates = (joined.updates as DurableLegacyDocumentUpdate[])
                .map(update => ({...update, doc: documentId}));
            if (joined.version - recoveryRevision !== updates.length) {
                throw new DocumentOtHistoryError(
                    `Server returned ${updates.length} catch-up operations for `
                    + `${joined.version - recoveryRevision} revisions`,
                );
            }
            await session.resumeTransport(
                joined.docLines.join('\n'),
                joined.version,
                updates,
            );
            this.documentSessionReopenIds?.delete(documentId);
            this.documentSessionFreshProofIds?.delete(documentId);
            this.documentSessionFailures.delete(documentId);
            this.cancelDocumentSessionRetry(documentId);
            this.notify?.([{
                type: vscode.FileChangeType.Changed,
                uri: this.pathToUri(resolved.path),
            }]);
        } catch (error) {
            const failure = error instanceof Error ? error : new Error(String(error));
            console.error(
                `[hershel-safe-sync] document reconnect ${documentId} failed`,
                failure,
            );
            if (isSocketIOTimeout(failure) && this.socket.needsReinit) {
                session?.suspendTransport();
                this.documentSessionFailures.delete(documentId);
                this.cancelDocumentSessionRetry(documentId);
                return;
            }
            if (failure instanceof DocumentSessionFactoryCleanupError) {
                this.documentSessionFailures.set(documentId, failure);
                return;
            }
            if (session?.isFailed || failure instanceof DocumentOtHistoryError) {
                this.documentSessionFailures.set(documentId, failure);
                // resumeTransport fail-stops its generation. Reusing that
                // object would retry assertHealthy forever, so first close its
                // WAL handle and then spend exactly one fresh-generation proof
                // against the same journal. A repeated deterministic mismatch
                // is path-scoped recovery evidence, not a reconnect retry.
                if (this.documentSessions.get(documentId) === pendingSession) {
                    this.documentSessions.delete(documentId);
                }
                this.cancelDocumentSessionRetry(documentId);
                this.documentSessionReopenIds ??= new Set();
                this.documentSessionFreshProofIds ??= new Set();
                this.documentSessionReopenIds.add(documentId);
                this.documentSessionFreshProofIds.add(documentId);
                try {
                    await this.trackDocumentSessionClose(pendingSession);
                } catch {
                    // trackDocumentSessionClose records the ownership failure;
                    // fail closed rather than opening a second WAL owner.
                    this.documentSessionReopenIds.delete(documentId);
                    return;
                }
                this.scheduleDocumentSessionRetry(documentId);
                return;
            }
            if (!session) {
                // The factory did not acquire a live generation. Its join or
                // authentication failure remains retryable; deterministic
                // factory proof failures are isolated by getDocumentSession.
                this.documentSessionReopenIds ??= new Set();
                this.documentSessionReopenIds.add(documentId);
                this.documentSessionFailures.delete(documentId);
                this.scheduleDocumentSessionRetry(documentId);
                return;
            }
            this.documentSessionFailures.delete(documentId);
            this.scheduleDocumentSessionRetry(documentId, pendingSession);
        }
    }

    private scheduleDocumentSessionRetry(
        documentId: string,
        pendingSession?: Promise<DurableDocumentSession>,
    ): void {
        this.documentSessionRetryTimers ??= new Map();
        this.documentSessionRetryAttempts ??= new Map();
        this.documentSessionReopenIds ??= new Set();
        const registered = this.documentSessions.get(documentId);
        if (
            this.retiring
            || (pendingSession
                ? registered !== pendingSession
                : registered !== undefined || !this.documentSessionReopenIds.has(documentId))
            || this.documentSessionRetryTimers.has(documentId)
        ) {
            return;
        }
        const attempt = (this.documentSessionRetryAttempts.get(documentId) ?? 0) + 1;
        this.documentSessionRetryAttempts.set(documentId, attempt);
        const delay = Math.min(
            DOCUMENT_RECONNECT_RETRY_BASE_MS * (2 ** Math.min(attempt - 1, 6)),
            DOCUMENT_RECONNECT_RETRY_MAX_MS,
        );
        const timer = setTimeout(() => {
            if (this.documentSessionRetryTimers.get(documentId) === timer) {
                this.documentSessionRetryTimers.delete(documentId);
            }
            if (this.retiring) {
                return;
            }
            const current = this.documentSessions.get(documentId);
            if (!this.root) {
                this.scheduleDocumentSessionRetry(documentId, current ?? pendingSession);
                return;
            }
            if (current) {
                if (pendingSession && current !== pendingSession) {
                    return;
                }
                void this.resumeDocumentSessionTransport(documentId, current);
                return;
            }
            if (!this.connectionReady) {
                this.scheduleDocumentSessionRetry(documentId);
                return;
            }
            void this.reopenDocumentSessionAfterFailure(documentId);
        }, delay);
        timer.unref();
        this.documentSessionRetryTimers.set(documentId, timer);
    }

    private async reopenDocumentSessionAfterFailure(documentId: string): Promise<void> {
        this.documentSessionReopenIds ??= new Set();
        if (this.retiring || !this.documentSessionReopenIds.has(documentId)) {
            return;
        }
        const resolved = this.root ? this._resolveById(documentId) : undefined;
        if (resolved?.fileType !== 'doc') {
            this.documentSessionReopenIds.delete(documentId);
            this.documentSessionFailures.delete(documentId);
            this.cancelDocumentSessionRetry(documentId);
            return;
        }
        try {
            await this.getDocumentSession(
                resolved.fileEntity as DocumentEntity,
                resolved.path,
            );
            this.documentSessionReopenIds.delete(documentId);
            this.documentSessionFreshProofIds?.delete(documentId);
            this.documentSessionFailures.delete(documentId);
            this.cancelDocumentSessionRetry(documentId);
            this.notify?.([{
                type: vscode.FileChangeType.Changed,
                uri: this.pathToUri(resolved.path),
            }]);
        } catch (error) {
            const failure = error instanceof Error ? error : new Error(String(error));
            if (failure instanceof DocumentRecoveryIsolationError) {
                this.documentSessionReopenIds.delete(documentId);
                this.documentSessionFreshProofIds?.delete(documentId);
                this.documentSessionFailures.delete(documentId);
                this.cancelDocumentSessionRetry(documentId);
                return;
            }
            if (
                failure instanceof DocumentSessionFactoryCleanupError
                || this.documentSessionCloseError
            ) {
                this.documentSessionFailures.set(documentId, failure);
                this.documentSessionReopenIds.delete(documentId);
                return;
            }
            this.documentSessionFailures.delete(documentId);
            this.documentSessionReopenIds.add(documentId);
            this.scheduleDocumentSessionRetry(documentId);
        }
    }

    private isolateDocumentRecovery(
        documentId: string,
        fallbackPath: string,
        failure: DocumentOtHistoryError,
    ): void {
        this.documentRecoveryIsolations ??= new Map();
        const currentPath = this.root
            ? this._resolveById(documentId)?.path ?? fallbackPath
            : fallbackPath;
        const reason = `OT history proof failed after one fresh generation: ${failure.message}`;
        this.documentRecoveryIsolations.set(documentId, {
            path: currentPath,
            reason,
            failedAt: new Date().toISOString(),
        });
        this.documentSessions.delete(documentId);
        this.documentSessionFailures.delete(documentId);
        this.documentSessionReopenIds?.delete(documentId);
        this.documentSessionFreshProofIds?.delete(documentId);
        this.cancelDocumentSessionRetry(documentId);
        console.error(
            `[hershel-safe-sync] terminal document recovery ${documentId} at ${currentPath}`,
            failure,
        );
        vscode.window.showWarningMessage(
            vscode.l10n.t('Safe synchronization isolated {path}: {message}', {
                path: currentPath,
                message: reason,
            }),
        );
        this.notify?.([{
            type: vscode.FileChangeType.Changed,
            uri: this.pathToUri(currentPath),
        }]);
    }

    private documentRecoveryIsolationError(documentId: string): DocumentRecoveryIsolationError {
        const isolation = this.documentRecoveryIsolations?.get(documentId);
        if (!isolation) {
            throw new Error(`Document ${documentId} has no terminal recovery isolation`);
        }
        return new DocumentRecoveryIsolationError(
            documentId,
            isolation.path,
            isolation.reason,
        );
    }

    /** Return the exact WAL candidate retained for one terminal live-document proof. */
    async inspectIsolatedDocumentRecovery(
        documentId: string,
    ): Promise<IsolatedDocumentRecovery | undefined> {
        const isolation = this.documentRecoveryIsolations?.get(documentId);
        if (!isolation) {
            return undefined;
        }
        await this.acquireDocumentJournalOwnership();
        await this.waitForDetachedDocumentSessionClosures();
        const snapshot = await this.readDocumentRecoverySnapshot(documentId, false);
        const epoch = this.documentJournalEpochs.get(documentId) ?? 0;
        return {
            documentId,
            ...snapshot,
            epoch,
            hash: this.hashDocumentRecoverySnapshot(documentId, snapshot, epoch),
            ...isolation,
        };
    }

    private cancelDocumentSessionRetry(documentId: string): void {
        const timer = this.documentSessionRetryTimers?.get(documentId);
        if (timer) {
            clearTimeout(timer);
        }
        this.documentSessionRetryTimers?.delete(documentId);
        this.documentSessionRetryAttempts?.delete(documentId);
    }

    private assertDocumentSessionCloseHealthy(): void {
        if (this.documentSessionCloseError) {
            throw this.documentSessionCloseError;
        }
    }

    private resetDocumentSessions(): void {
        void this.closeDocumentSessions().catch(error => {
            console.error('[hershel-safe-sync] failed to close a document session', error);
        });
    }

    private closeDocumentSessions(): Promise<void> {
        if (this.documentSessionsClosePromise) {
            return this.documentSessionsClosePromise;
        }
        for (const documentId of this.documentSessionRetryTimers?.keys() ?? []) {
            this.cancelDocumentSessionRetry(documentId);
        }
        this.documentSessionReopenIds?.clear();
        this.documentSessionFreshProofIds?.clear();
        const sessions = [...this.documentSessions.values()];
        this.documentSessions.clear();
        const closing = Promise.all(
            sessions.map(session => this.trackDocumentSessionClose(session)),
        ).then(() => undefined);
        this.documentSessionsClosePromise = closing;
        const clear = () => {
            if (this.documentSessionsClosePromise === closing) {
                this.documentSessionsClosePromise = undefined;
            }
        };
        void closing.then(clear, clear);
        return closing;
    }

    /** Drain every close generation after retirement has fenced new sessions. */
    private async drainDocumentSessions(): Promise<void> {
        let firstError = this.documentSessionCloseError;
        do {
            try {
                await this.closeDocumentSessions();
            } catch (error) {
                firstError ??= error instanceof Error ? error : new Error(String(error));
            }
            const closures = [...this.documentSessionClosures];
            if (closures.length > 0) {
                const results = await Promise.allSettled(closures);
                const failure = results.find(
                    (result): result is PromiseRejectedResult => result.status === 'rejected',
                );
                if (failure) {
                    firstError ??= failure.reason instanceof Error
                        ? failure.reason
                        : new Error(String(failure.reason));
                }
            }
        } while (
            this.documentSessionsClosePromise
            || this.documentSessions.size > 0
            || this.documentSessionClosures.size > 0
        );
        firstError ??= this.documentSessionCloseError;
        if (firstError) {
            throw firstError;
        }
    }

    private trackDocumentSessionClose(
        session: Promise<DurableDocumentSession>,
    ): Promise<void> {
        let tracked: Promise<void>;
        tracked = session
            .then(
                value => value.close(),
                error => {
                    // The factory owns and closes a journal until it returns a
                    // DurableDocumentSession. A normal factory rejection has
                    // therefore already released its WAL handle.
                    if (error instanceof DocumentSessionFactoryCleanupError) {
                        throw error;
                    }
                },
            )
            .catch(error => {
                const normalized = error instanceof Error ? error : new Error(String(error));
                this.documentSessionCloseError ??= normalized;
                throw normalized;
            })
            .finally(() => this.documentSessionClosures.delete(tracked));
        this.documentSessionClosures.add(tracked);
        return tracked;
    }

    private async acquireDocumentJournalOwnership(): Promise<void> {
        if (this.documentJournalLock) {
            return;
        }
        if (!this.documentJournalLockPromise) {
            const key = `document-wal\0${this.serverName}\0${this.projectId}`;
            const acquiring = ReplicaLock.acquire(
                this.context.globalStorageUri.fsPath,
                key,
                {
                    onLost: error => {
                        console.error('[hershel-safe-sync] document WAL ownership lost', error);
                        void this.retireForReplicaHandoff().catch(retireError => {
                            console.error(
                                '[hershel-safe-sync] failed to retire after WAL ownership loss',
                                retireError,
                            );
                        });
                    },
                },
            );
            this.documentJournalLockPromise = acquiring;
            void acquiring.catch(() => {
                if (this.documentJournalLockPromise === acquiring) {
                    this.documentJournalLockPromise = undefined;
                }
            });
        }
        const lock = await this.documentJournalLockPromise;
        if (this.retiring) {
            await lock.release();
            if (this.documentJournalLockPromise) {
                this.documentJournalLockPromise = undefined;
            }
            throw new Error('Remote document session is retiring for replica handoff');
        }
        this.documentJournalLock = lock;
    }

    private async releaseDocumentJournalOwnership(): Promise<void> {
        const lock = this.documentJournalLock;
        this.documentJournalLock = undefined;
        this.documentJournalLockPromise = undefined;
        await lock?.release();
    }

    /** Read server-authoritative content without recovering or sending a saved local operation. */
    async readAuthoritativeFile(uri: vscode.Uri): Promise<AuthoritativeFileSnapshot> {
        const {fileType, fileEntity} = await this._resolveUri(uri);
        if (!fileEntity || (fileType !== 'doc' && fileType !== 'file')) {
            throw vscode.FileSystemError.FileNotFound(uri);
        }
        if (fileType === 'doc') {
            const resolved = this._resolveById(fileEntity._id);
            const session = await this.getDocumentSession(
                fileEntity as DocumentEntity,
                resolved?.path ?? fileEntity.name,
            );
            return {
                content: new TextEncoder().encode(session.acknowledgedContent),
                entityId: fileEntity._id,
                fileType,
                revision: session.revision,
            };
        }
        return {
            content: await this.openFile(uri),
            entityId: fileEntity._id,
            fileType,
        };
    }

    /** Read a conflicted document without opening or replaying its local OT journal. */
    async readAuthoritativeFileWithoutRecovery(
        uri: vscode.Uri,
    ): Promise<AuthoritativeFileSnapshot> {
        const {fileType, fileEntity} = await this._resolveUri(uri);
        if (!fileEntity || (fileType !== 'doc' && fileType !== 'file')) {
            throw vscode.FileSystemError.FileNotFound(uri);
        }
        if (fileType === 'file') {
            return {
                content: await this.openFile(uri),
                entityId: fileEntity._id,
                fileType,
            };
        }
        const joined = await this.socket.joinDoc(fileEntity._id);
        if (joined.otType !== 'sharejs-text-ot') {
            throw new Error(`Unsupported Overleaf document OT type: ${joined.otType}`);
        }
        return {
            content: new TextEncoder().encode(joined.docLines.join('\n')),
            entityId: fileEntity._id,
            fileType,
            revision: joined.version,
        };
    }

    /** Read a document by immutable id without opening or mutating its recovery WAL. */
    async readAuthoritativeDocumentByIdWithoutRecovery(
        documentId: string,
    ): Promise<(AuthoritativeFileSnapshot & {path: string}) | undefined> {
        const resolved = this.root ? this._resolveById(documentId) : undefined;
        if (resolved?.fileType !== 'doc') {
            return undefined;
        }
        const joined = await this.socket.joinDoc(documentId);
        if (joined.otType !== 'sharejs-text-ot') {
            throw new Error(`Unsupported Overleaf document OT type: ${joined.otType}`);
        }
        return {
            content: new TextEncoder().encode(joined.docLines.join('\n')),
            entityId: documentId,
            fileType: 'doc',
            revision: joined.version,
            path: resolved.path,
        };
    }

    /** Refresh structural authority and resolve one immutable document id. */
    async resolveAuthoritativeDocumentPath(documentId: string): Promise<string | undefined> {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const project = await this.readFreshProjectSnapshot(identity);
        if (this.root?.settings !== undefined && project.settings === undefined) {
            project.settings = this.root.settings;
        }
        this.root = project;
        return this.resolveFreshDocumentPath(project, documentId);
    }

    /** Retire failed local recovery state after its conflict artifact is durable. */
    async discardDocumentRecovery(uri: vscode.Uri): Promise<void> {
        const {fileType, fileEntity} = await this._resolveUri(uri);
        if (fileType !== 'doc' || !fileEntity) {
            return;
        }
        await this.discardDocumentRecoveryById(fileEntity._id, fileEntity.name);
    }

    /** Adjudicate a WAL by immutable entity id even after its path was deleted or recreated. */
    async discardDocumentRecoveryById(
        documentId: string,
        name = documentId,
    ): Promise<void> {
        const activeDiscard = this.documentRecoveryDiscardTasks.get(documentId);
        if (activeDiscard) {
            return activeDiscard;
        }
        const resolved = this.root ? this._resolveById(documentId) : undefined;
        const doc = resolved?.fileType === 'doc'
            ? resolved.fileEntity as DocumentEntity
            : {_id: documentId, name} as DocumentEntity;
        const discard = this.discardDocumentRecoveryOwner(doc);
        this.documentRecoveryDiscardTasks.set(documentId, discard);
        try {
            await discard;
        } finally {
            if (this.documentRecoveryDiscardTasks.get(documentId) === discard) {
                this.documentRecoveryDiscardTasks.delete(documentId);
            }
        }
    }

    private async discardDocumentRecoveryOwner(doc: DocumentEntity): Promise<void> {
        const documentId = doc._id;
        this.documentRecoveryFences.add(documentId);
        let removed = false;
        let advanced = false;
        let pending = false;
        const expectedEpoch = this.documentJournalEpochs.get(documentId) ?? 0;
        try {
            // The project journal namespace and its lock are isomorphic. Even
            // deletion must own the same lease as open/append, otherwise a
            // second replica could unlink another process's live WAL.
            await this.acquireDocumentJournalOwnership();
            await this.drainDocumentAcceptAdmissions(documentId);
            if ((this.documentJournalEpochs.get(documentId) ?? 0) !== expectedEpoch) {
                advanced = true;
                throw new DocumentRecoveryAdvancedError(
                    `Document changed while conflict resolution was fencing ${doc.name}; review and retry`,
                );
            }
            await this.waitForDetachedDocumentSessionClosures();
            // The fence blocks new accepts while the current owner proves every
            // journaled operation settled. Unregister only after that proof, so
            // a pending or late server acceptance keeps its live WAL owner.
            const pendingSession = this.documentSessions.get(documentId);
            if (pendingSession) {
                // Never unlink a live WAL after a failed close. The conflict
                // remains recoverable and the terminal fence stays in place.
                let session: DurableDocumentSession | undefined;
                try {
                    session = await pendingSession;
                } catch (error) {
                    // A factory cancelled by the fence closes any journal it
                    // acquired before rejecting. That is an expected terminal
                    // cancellation, not a sticky ownership-close failure.
                    if (error instanceof DocumentSessionFactoryCleanupError) {
                        throw error;
                    }
                }
                if (session) {
                    await session.waitForTransportIdle();
                    if (session.hasPendingOperations) {
                        pending = true;
                        throw new DocumentRecoveryPendingError(
                            `Document ${doc.name} still has a journaled operation without a proven server disposition`,
                        );
                    }
                }
                this.documentSessions.delete(documentId);
                if (session) {
                    await this.trackDocumentSessionClose(Promise.resolve(session));
                }
            } else {
                const journal = await DocumentJournal.openExisting(
                    this.documentJournalDirectory,
                    documentId,
                );
                if (journal) {
                    const hasPendingOperations = journal.hasPendingOperations;
                    await journal.close();
                    if (hasPendingOperations) {
                        pending = true;
                        throw new DocumentRecoveryPendingError(
                            `Document ${doc.name} has durable operations without a proven server disposition`,
                        );
                    }
                }
            }
            await this.drainLogicalDocumentAcknowledgements(documentId);
            await this.waitForDetachedDocumentSessionClosures();
            await Promise.all([
                DocumentJournal.remove(this.documentJournalDirectory, documentId),
                this.legacyDocumentStateStore.remove(documentId),
            ]);
            await this.socket.leaveDoc(documentId).catch(() => {});
            doc.version = undefined;
            doc.localCache = undefined;
            doc.remoteCache = undefined;
            this.documentPendingOperations.delete(documentId);
            this.documentSessionFailures.delete(documentId);
            this.documentRecoveryIsolations?.delete(documentId);
            this.documentSessionFreshProofIds?.delete(documentId);
            removed = true;
        } finally {
            if (removed || advanced || pending) {
                this.documentRecoveryFences.delete(documentId);
            }
        }
    }

    /** Snapshot pending recovery state without deleting it. */
    async prepareDocumentRecoveryById(
        documentId: string,
    ): Promise<PreparedDocumentRecovery> {
        await this.acquireDocumentJournalOwnership();
        if (this.documentRecoveryFences.has(documentId)) {
            throw new DocumentRecoveryPendingError(
                `Document ${documentId} already has an active recovery fence`,
            );
        }
        this.documentRecoveryFences.add(documentId);
        const epoch = this.documentJournalEpochs.get(documentId) ?? 0;
        try {
            await this.drainDocumentAcceptAdmissions(documentId);
            if ((this.documentJournalEpochs.get(documentId) ?? 0) !== epoch) {
                throw new DocumentRecoveryAdvancedError(
                    `Document ${documentId} changed while recovery was prepared`,
                );
            }
            await this.waitForDetachedDocumentSessionClosures();
            const snapshot = await this.readDocumentRecoverySnapshot(documentId, true);
            if ((this.documentJournalEpochs.get(documentId) ?? 0) !== epoch) {
                throw new DocumentRecoveryAdvancedError(
                    `Document ${documentId} changed while recovery was prepared`,
                );
            }
            const previousToken = this.preparedDocumentRecoveryById.get(documentId);
            if (previousToken) {
                this.preparedDocumentRecoveries.delete(previousToken);
            }
            const prepared: PreparedDocumentRecovery = {
                token: randomUUID(),
                documentId,
                ...snapshot,
                epoch,
                hash: this.hashDocumentRecoverySnapshot(documentId, snapshot, epoch),
            };
            this.preparedDocumentRecoveries.set(prepared.token, prepared);
            this.preparedDocumentRecoveryById.set(documentId, prepared.token);
            return prepared;
        } finally {
            this.documentRecoveryFences.delete(documentId);
        }
    }

    /** Delete a prepared WAL only if its archived snapshot is still exact. */
    async commitDiscardDocumentRecovery(token: string): Promise<void> {
        const prepared = this.preparedDocumentRecoveries.get(token);
        if (!prepared || this.preparedDocumentRecoveryById.get(prepared.documentId) !== token) {
            throw new Error('Document recovery preparation token is missing or stale');
        }
        if (this.committingPreparedDocumentRecoveries.has(token)) {
            throw new DocumentRecoveryPendingError(
                `Document ${prepared.documentId} recovery commit is already running`,
            );
        }
        if (this.documentRecoveryDiscardTasks.has(prepared.documentId)) {
            throw new DocumentRecoveryPendingError(
                `Document ${prepared.documentId} already has a recovery discard`,
            );
        }
        this.committingPreparedDocumentRecoveries.add(token);
        try {
            await this.acquireDocumentJournalOwnership();
        } catch (error) {
            this.committingPreparedDocumentRecoveries.delete(token);
            throw error;
        }
        if (this.documentRecoveryFences.has(prepared.documentId)) {
            this.committingPreparedDocumentRecoveries.delete(token);
            throw new DocumentRecoveryPendingError(
                `Document ${prepared.documentId} already has an active recovery fence`,
            );
        }
        const documentId = prepared.documentId;
        this.documentRecoveryFences.add(documentId);
        try {
            await this.drainDocumentAcceptAdmissions(documentId);
            this.assertPreparedDocumentRecoveryCurrent(prepared);
            await this.waitForDetachedDocumentSessionClosures();

            const registered = this.documentSessions.get(documentId);
            if (registered) {
                const session = await registered;
                await session.waitForTransportIdle();
                this.assertPreparedDocumentRecoveryCurrent(
                    prepared,
                    this.snapshotDocumentSession(session),
                );
                if (this.documentSessions.get(documentId) === registered) {
                    this.documentSessions.delete(documentId);
                }
                await this.trackDocumentSessionClose(registered);
                this.assertPreparedDocumentRecoveryCurrent(
                    prepared,
                    this.snapshotDocumentSession(session),
                );
            } else {
                const snapshot = await this.readDocumentRecoverySnapshot(documentId, false);
                this.assertPreparedDocumentRecoveryCurrent(prepared, snapshot);
            }

            await this.drainLogicalDocumentAcknowledgements(documentId);
            await this.waitForDetachedDocumentSessionClosures();
            const finalSnapshot = await this.readDocumentRecoverySnapshot(documentId, false);
            this.assertPreparedDocumentRecoveryCurrent(prepared, finalSnapshot);

            await Promise.all([
                DocumentJournal.remove(this.documentJournalDirectory, documentId),
                this.legacyDocumentStateStore.remove(documentId),
            ]);
            await this.socket.leaveDoc(documentId).catch(() => {});
            const resolved = this.root ? this._resolveById(documentId) : undefined;
            if (resolved?.fileType === 'doc') {
                const doc = resolved.fileEntity as DocumentEntity;
                doc.version = undefined;
                doc.localCache = undefined;
                doc.remoteCache = undefined;
            }
            this.documentPendingOperations.delete(documentId);
            this.documentJournalEpochs.delete(documentId);
            this.documentSessionFailures.delete(documentId);
            this.documentRecoveryIsolations?.delete(documentId);
            this.documentSessionFreshProofIds?.delete(documentId);
            this.preparedDocumentRecoveries.delete(token);
            if (this.preparedDocumentRecoveryById.get(documentId) === token) {
                this.preparedDocumentRecoveryById.delete(documentId);
            }
        } finally {
            this.documentRecoveryFences.delete(documentId);
            this.committingPreparedDocumentRecoveries.delete(token);
        }
    }

    private snapshotDocumentSession(session: DurableDocumentSession): DocumentRecoverySnapshot {
        return {
            visibleContent: session.content,
            acknowledgedContent: session.acknowledgedContent,
            revision: session.revision,
            pendingOperationIds: [...session.pendingOperationIds],
            lastKnownPath: session.lastKnownPath,
        };
    }

    private async readDocumentRecoverySnapshot(
        documentId: string,
        waitForTransport: boolean,
    ): Promise<DocumentRecoverySnapshot> {
        const registered = this.documentSessions.get(documentId);
        if (registered) {
            const session = await registered;
            if (waitForTransport) {
                await session.waitForTransportIdle();
            }
            return this.snapshotDocumentSession(session);
        }
        const journal = await DocumentJournal.openExisting(
            this.documentJournalDirectory,
            documentId,
        );
        if (journal) {
            try {
                return {
                    visibleContent: journal.content,
                    acknowledgedContent: journal.acknowledgedContent,
                    revision: journal.revision,
                    pendingOperationIds: [...journal.pendingOperationIds],
                    lastKnownPath: journal.lastKnownPath,
                };
            } finally {
                await journal.close();
            }
        }
        const legacy = await this.legacyDocumentStateStore.load(documentId);
        if (legacy) {
            return {
                visibleContent: legacy.state.localContent,
                acknowledgedContent: legacy.state.serverContent,
                revision: legacy.state.revision,
                pendingOperationIds: [
                    ...(legacy.state.outstanding?.ids ?? []),
                    ...(legacy.state.pending?.ids ?? []),
                ],
            };
        }
        throw new Error(`No durable document recovery state exists for ${documentId}`);
    }

    private hashDocumentRecoverySnapshot(
        documentId: string,
        snapshot: DocumentRecoverySnapshot,
        epoch: number,
    ): string {
        return contentHash(new TextEncoder().encode(JSON.stringify({
            documentId,
            ...snapshot,
            epoch,
        })));
    }

    /** Enumerate every durable pending WAL before filesystem semantics are observed. */
    async inspectPendingDocumentRecoveries(): Promise<PendingDocumentRecovery[]> {
        await this.acquireDocumentJournalOwnership();
        const result: PendingDocumentRecovery[] = [];
        const journalDocumentIds = await DocumentJournal.listExisting(
            this.documentJournalDirectory,
        );
        for (const documentId of journalDocumentIds) {
            const snapshot = await this.readDocumentRecoverySnapshot(documentId, false);
            if (snapshot.pendingOperationIds.length === 0) {
                continue;
            }
            const epoch = this.documentJournalEpochs.get(documentId) ?? 0;
            result.push({
                documentId,
                ...snapshot,
                epoch,
                hash: this.hashDocumentRecoverySnapshot(documentId, snapshot, epoch),
            });
        }
        const journalIds = new Set(journalDocumentIds);
        for (const legacy of await this.legacyDocumentStateStore.listExisting()) {
            if (journalIds.has(legacy.documentId)) {
                // A migrated v2 journal supersedes the legacy checkpoint.
                continue;
            }
            const snapshot: DocumentRecoverySnapshot = {
                visibleContent: legacy.state.localContent,
                acknowledgedContent: legacy.state.serverContent,
                revision: legacy.state.revision,
                pendingOperationIds: [
                    ...(legacy.state.outstanding?.ids ?? []),
                    ...(legacy.state.pending?.ids ?? []),
                ],
            };
            if (snapshot.pendingOperationIds.length === 0) {
                continue;
            }
            const epoch = this.documentJournalEpochs.get(legacy.documentId) ?? 0;
            result.push({
                documentId: legacy.documentId,
                ...snapshot,
                epoch,
                hash: this.hashDocumentRecoverySnapshot(legacy.documentId, snapshot, epoch),
            });
        }
        result.sort((left, right) => left.documentId.localeCompare(right.documentId));
        return result;
    }

    private assertPreparedDocumentRecoveryCurrent(
        prepared: PreparedDocumentRecovery,
        snapshot?: DocumentRecoverySnapshot,
    ): void {
        const epoch = this.documentJournalEpochs.get(prepared.documentId) ?? 0;
        if (epoch !== prepared.epoch) {
            throw new DocumentRecoveryAdvancedError(
                `Document ${prepared.documentId} changed after recovery preparation`,
            );
        }
        if (
            snapshot
            && this.hashDocumentRecoverySnapshot(prepared.documentId, snapshot, epoch) !== prepared.hash
        ) {
            throw new DocumentRecoveryAdvancedError(
                `Document ${prepared.documentId} content changed after recovery preparation`,
            );
        }
    }

    /** Apply a document change against a proven ancestor, preserving newer OT operations. */
    async writeDocumentAgainstAncestor(
        uri: vscode.Uri,
        ancestor: string,
        target: string,
        expectedEntityId?: string,
        operationId?: string,
    ): Promise<string> {
        const accepted = await this.acceptDocumentSnapshotAgainstAncestor(
            uri,
            ancestor,
            target,
            true,
            expectedEntityId,
            operationId,
        );
        return accepted.acknowledgement;
    }

    /** Journal a document operation without waiting for its remote acknowledgement. */
    async acceptDocumentSnapshotAgainstAncestor(
        uri: vscode.Uri,
        ancestor: string,
        target: string,
        barrier = false,
        expectedEntityId?: string,
        operationId?: string,
    ): Promise<AcceptedDocumentOperation> {
        let fileEntity: DocumentEntity;
        let resolvedPath = uri.path;
        if (
            !this.connectionReady
            && expectedEntityId !== undefined
            && this.documentSessions.has(expectedEntityId)
        ) {
            fileEntity = {
                _id: expectedEntityId,
                name: uri.path.split('/').at(-1) ?? expectedEntityId,
            };
        } else if (expectedEntityId !== undefined && this.root) {
            const resolved = this._resolveById(expectedEntityId);
            if (resolved?.fileType !== 'doc') {
                throw new ConcurrentRemoteResourceError(uri.path);
            }
            fileEntity = resolved.fileEntity as DocumentEntity;
            resolvedPath = resolved.path;
        } else {
            const resolved = await this._resolveUri(uri);
            if (
                resolved.fileType !== 'doc'
                || !resolved.fileEntity
                || (
                    expectedEntityId !== undefined
                    && resolved.fileEntity._id !== expectedEntityId
                )
            ) {
                if (expectedEntityId !== undefined) {
                    throw new ConcurrentRemoteResourceError(uri.path);
                }
                throw new Error(`Expected an Overleaf document at ${uri.path}`);
            }
            fileEntity = resolved.fileEntity as DocumentEntity;
            resolvedPath = this._resolveById(fileEntity._id)?.path ?? fileEntity.name;
        }
        const finishAdmission = this.admitDocumentAccept(fileEntity._id);
        try {
            for (let attempt = 0; attempt < 2; attempt++) {
                const session = await this.getDocumentSession(
                    fileEntity,
                    resolvedPath,
                );
                try {
                    this.assertDocumentRecoveryAvailable(fileEntity._id);
                    const accepted = operationId === undefined
                        ? await session.acceptSnapshotAgainstAncestor(ancestor, target, barrier)
                        : await session.acceptSnapshotAgainstAncestor(
                            ancestor,
                            target,
                            barrier,
                            operationId,
                        );
                    if (!accepted.opId) {
                        return accepted;
                    }
                    this.documentJournalEpochs.set(
                        fileEntity._id,
                        (this.documentJournalEpochs.get(fileEntity._id) ?? 0) + 1,
                    );
                    return {
                        ...accepted,
                        acknowledgement: this.observeLogicalDocumentOperation(
                            fileEntity,
                            resolvedPath,
                            accepted.opId,
                            session,
                            accepted.acknowledgement,
                        ),
                    };
                } catch (error) {
                    if (!(error instanceof DocumentSessionRetiredError) || attempt > 0) {
                        throw error;
                    }
                    const registered = this.documentSessions.get(fileEntity._id);
                    if (registered && await registered.catch(() => undefined) === session) {
                        this.documentSessions.delete(fileEntity._id);
                        await this.trackDocumentSessionClose(registered);
                    }
                }
            }
            throw new DocumentSessionRetiredError('Document session could not recover after retirement');
        } finally {
            finishAdmission();
        }
    }

    /** Inspect one fixed logical operation without deriving a replacement operation. */
    async inspectDocumentOperation(
        uri: vscode.Uri,
        expectedEntityId: string,
        operationId: string,
    ): Promise<DocumentOperationInspection> {
        const resolved = this._resolveById(expectedEntityId);
        let document: DocumentEntity;
        let resolvedPath: string;
        if (resolved) {
            if (resolved.fileType !== 'doc') {
                throw new ConcurrentRemoteResourceError(uri.path);
            }
            document = resolved.fileEntity as DocumentEntity;
            resolvedPath = resolved.path;
        } else {
            const byPath = await this._resolveUri(uri);
            if (
                byPath.fileType !== 'doc'
                || !byPath.fileEntity
                || byPath.fileEntity._id !== expectedEntityId
            ) {
                throw new ConcurrentRemoteResourceError(uri.path);
            }
            document = byPath.fileEntity as DocumentEntity;
            resolvedPath = uri.path;
        }
        const session = await this.getDocumentSession(document, resolvedPath);
        return session.inspectOperation(operationId);
    }

    private admitDocumentAccept(documentId: string): () => void {
        this.assertDocumentRecoveryAvailable(documentId);
        let finish!: () => void;
        const admission = new Promise<void>(resolve => { finish = resolve; });
        const active = this.documentAcceptAdmissions.get(documentId) ?? new Set<Promise<void>>();
        active.add(admission);
        this.documentAcceptAdmissions.set(documentId, active);
        let finished = false;
        return () => {
            if (finished) {
                return;
            }
            finished = true;
            active.delete(admission);
            if (active.size === 0 && this.documentAcceptAdmissions.get(documentId) === active) {
                this.documentAcceptAdmissions.delete(documentId);
            }
            finish();
        };
    }

    private async drainDocumentAcceptAdmissions(documentId: string): Promise<void> {
        while (this.documentAcceptAdmissions.get(documentId)?.size) {
            await Promise.allSettled([...this.documentAcceptAdmissions.get(documentId)!]);
        }
    }

    /**
     * Follow one durable operation id across socket/session generations. Once
     * the receipt exists this method only observes or idempotently recovers
     * that operation; it never derives a second operation from the snapshot.
     */
    private observeLogicalDocumentOperation(
        doc: DocumentEntity,
        path: string,
        opId: string,
        initialSession: DurableDocumentSession,
        initialAcknowledgement: Promise<string>,
    ): Promise<string> {
        const following = this.followLogicalDocumentOperation(
            doc,
            path,
            opId,
            initialSession,
            initialAcknowledgement,
        );
        const active = this.logicalDocumentAcknowledgements.get(doc._id) ?? new Set<Promise<string>>();
        active.add(following);
        this.logicalDocumentAcknowledgements.set(doc._id, active);
        void following.finally(() => {
            active.delete(following);
            if (active.size === 0 && this.logicalDocumentAcknowledgements.get(doc._id) === active) {
                this.logicalDocumentAcknowledgements.delete(doc._id);
            }
        }).catch(() => {});
        return following;
    }

    private async followLogicalDocumentOperation(
        doc: DocumentEntity,
        path: string,
        opId: string,
        initialSession: DurableDocumentSession,
        initialAcknowledgement: Promise<string>,
    ): Promise<string> {
        this.logicalAcknowledgementCount += 1;
        let session = initialSession;
        let acknowledgement = initialAcknowledgement;
        try {
            for (;;) {
                try {
                    return await acknowledgement;
                } catch (error) {
                    if (!(error instanceof DocumentSessionRetiredError)) {
                        throw error;
                    }
                }

                this.assertDocumentRecoveryAvailable(doc._id);

                const registered = this.documentSessions.get(doc._id);
                if (registered && await registered.catch(() => undefined) === session) {
                    this.documentSessions.delete(doc._id);
                    await this.trackDocumentSessionClose(registered);
                }
                session = await this.getDocumentSession(doc, path);
                this.assertDocumentRecoveryAvailable(doc._id);
                acknowledgement = session.observeOperation(opId);
            }
        } finally {
            this.logicalAcknowledgementCount -= 1;
        }
    }

    private assertDocumentRecoveryAvailable(documentId: string): void {
        if (this.documentRecoveryIsolations?.has(documentId)) {
            throw this.documentRecoveryIsolationError(documentId);
        }
        if (this.documentRecoveryFences.has(documentId)) {
            throw new DocumentSessionRetiredError(
                `Document recovery is being discarded for ${documentId}`,
            );
        }
    }

    private async drainLogicalDocumentAcknowledgements(documentId: string): Promise<void> {
        while (this.logicalDocumentAcknowledgements.get(documentId)?.size) {
            await Promise.allSettled([
                ...this.logicalDocumentAcknowledgements.get(documentId)!,
            ]);
        }
    }

    private async waitForDetachedDocumentSessionClosures(): Promise<void> {
        for (;;) {
            const generation = this.documentSessionsClosePromise;
            if (generation) {
                await generation;
            }
            const closures = [...this.documentSessionClosures];
            if (closures.length > 0) {
                const results = await Promise.allSettled(closures);
                const failure = results.find(
                    (result): result is PromiseRejectedResult => result.status === 'rejected',
                );
                if (failure) {
                    throw failure.reason;
                }
            }
            if (!this.documentSessionsClosePromise && this.documentSessionClosures.size === 0) {
                break;
            }
        }
        if (this.documentSessionCloseError) {
            throw this.documentSessionCloseError;
        }
    }

    /** Read the current canonical visible text and OT revision for a document. */
    async readDocumentProjection(uri: vscode.Uri, expectedEntityId?: string): Promise<{
        content: string;
        revision: number;
        entityId: string;
        sequence: number;
    }> {
        let fileEntity: DocumentEntity;
        let resolvedPath = uri.path;
        if (
            !this.connectionReady
            && expectedEntityId !== undefined
            && this.documentSessions.has(expectedEntityId)
        ) {
            fileEntity = {
                _id: expectedEntityId,
                name: uri.path.split('/').at(-1) ?? expectedEntityId,
            };
        } else {
            const resolved = await this._resolveUri(uri);
            if (
                resolved.fileType !== 'doc'
                || !resolved.fileEntity
                || (
                    expectedEntityId !== undefined
                    && resolved.fileEntity._id !== expectedEntityId
                )
            ) {
                if (expectedEntityId !== undefined) {
                    throw new ConcurrentRemoteResourceError(uri.path);
                }
                throw new Error(`Expected an Overleaf document at ${uri.path}`);
            }
            fileEntity = resolved.fileEntity as DocumentEntity;
            resolvedPath = this._resolveById(fileEntity._id)?.path ?? fileEntity.name;
        }
        const session = await this.getDocumentSession(
            fileEntity,
            resolvedPath,
        );
        return {
            content: session.content,
            revision: session.revision,
            entityId: fileEntity._id,
            sequence: session.sequence,
        };
    }

    /** Approved E2E fault boundary: keep WAL admission live while socket sends stop. */
    async suspendDocumentTransportForE2E(uri: vscode.Uri): Promise<void> {
        const projection = await this.readDocumentProjection(uri);
        const registered = this.documentSessions.get(projection.entityId);
        if (!registered) {
            throw new Error(`Document session did not open for ${uri.path}`);
        }
        (await registered).suspendTransport();
    }

    async hasPendingDocumentRecoveryForE2E(uri: vscode.Uri): Promise<boolean> {
        const {fileType, fileEntity} = await this._resolveUri(uri);
        if (fileType !== 'doc' || !fileEntity) {
            return false;
        }
        const registered = this.documentSessions.get(fileEntity._id);
        return registered ? (await registered).hasPendingOperations : false;
    }

    /** Read the active WAL owner without opening a second journal or sending recovery traffic. */
    async inspectDocumentRecoveryForE2E(
        uri: vscode.Uri,
        operationId?: string,
    ): Promise<{
        entityId: string;
        revision: number;
        visibleContent: string;
        acknowledgedContent: string;
        pendingOperationIds: string[];
        lastKnownPath?: string;
        operationDisposition?: 'unknown' | 'pending' | 'settled';
    } | undefined> {
        const {fileType, fileEntity} = await this._resolveUri(uri);
        if (fileType !== 'doc' || !fileEntity) {
            return undefined;
        }
        const registered = this.documentSessions.get(fileEntity._id);
        if (!registered) {
            return undefined;
        }
        const session = await registered;
        return {
            entityId: fileEntity._id,
            revision: session.revision,
            visibleContent: session.content,
            acknowledgedContent: session.acknowledgedContent,
            pendingOperationIds: [...session.pendingOperationIds],
            ...(session.lastKnownPath === undefined
                ? {}
                : {lastKnownPath: session.lastKnownPath}),
            ...(operationId === undefined
                ? {}
                : {operationDisposition: session.inspectOperation(operationId).disposition}),
        };
    }

    /** Replace a resource only when its identity and bytes still match the durable baseline. */
    async writeResourceAgainstBaseline(
        uri: vscode.Uri,
        expectedEntityId: string,
        expectedHash: string,
        content: Uint8Array,
    ): Promise<string> {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const target = await this._resolveUri(uri);
        if (
            target.fileType !== 'file'
            || !target.fileEntity
            || target.fileEntity._id !== expectedEntityId
        ) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        const transaction = await this.createRemoteResourceTransaction(identity, 'history');
        const incoming = await this.uploadRemoteResource(
            identity,
            transaction.incoming,
            target.fileName,
            content,
        );
        const before = await this.readAuthoritativeFile(uri);
        if (
            before.fileType !== 'file'
            || before.entityId !== expectedEntityId
            || contentHash(before.content) !== expectedHash
        ) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }

        await this.moveRemoteEntity(
            identity,
            'file',
            target.fileEntity,
            target.parentFolder,
            transaction.previous,
        );
        const archived = await this.readRemoteResource(identity, expectedEntityId);
        const archivedHash = contentHash(archived);
        if (archivedHash !== expectedHash) {
            await this.rollbackRetainedEntity(
                identity,
                uri,
                'file',
                target.fileEntity,
                transaction.previous,
                target.parentFolder,
                archivedHash,
            );
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        await this.assertAuthoritativePathAbsent(identity, uri, true);
        await this.moveRemoteEntity(
            identity,
            'file',
            incoming,
            transaction.incoming,
            target.parentFolder,
        );
        const after = await this.readAuthoritativeFile(uri);
        if (
            after.fileType !== 'file'
            || after.entityId !== incoming._id
            || contentHash(after.content) !== contentHash(content)
        ) {
            throw new Error(`Remote resource read-back verification failed for ${uri.path}`);
        }
        this.notify([{type: vscode.FileChangeType.Changed, uri}]);
        return after.entityId;
    }

    /**
     * Logically delete a resource by moving the proven entity into retained
     * remote trash. No caller-observed delete can destroy a concurrent writer.
     */
    async removeResourceAgainstBaseline(
        uri: vscode.Uri,
        expectedEntityId: string,
        expectedHash: string,
    ): Promise<void> {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const target = await this._resolveUri(uri);
        if (
            target.fileType !== 'file'
            || !target.fileEntity
            || target.fileEntity._id !== expectedEntityId
        ) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        const before = await this.readAuthoritativeFile(uri);
        if (
            before.fileType !== 'file'
            || before.entityId !== expectedEntityId
            || contentHash(before.content) !== expectedHash
        ) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        const transaction = await this.createRemoteResourceTransaction(identity, 'trash');
        await this.moveRemoteEntity(
            identity,
            'file',
            target.fileEntity,
            target.parentFolder,
            transaction.previous,
        );
        const archived = await this.readRemoteResource(identity, expectedEntityId);
        const archivedHash = contentHash(archived);
        if (archivedHash !== expectedHash) {
            await this.rollbackRetainedEntity(
                identity,
                uri,
                'file',
                target.fileEntity,
                transaction.previous,
                target.parentFolder,
                archivedHash,
            );
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        await this.assertAuthoritativePathAbsent(identity, uri, true);
        this.notify([{type: vscode.FileChangeType.Deleted, uri}]);
    }

    /** Move a proven document into retained trash without discarding pending OT work. */
    async removeDocumentAgainstBaseline(
        uri: vscode.Uri,
        expectedEntityId: string,
        expectedHash: string,
    ): Promise<void> {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const target = await this._resolveUri(uri);
        if (
            target.fileType !== 'doc'
            || !target.fileEntity
            || target.fileEntity._id !== expectedEntityId
        ) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        await this.acquireDocumentJournalOwnership();
        const transaction = await this.createRemoteResourceTransaction(identity, 'trash');
        const before = await this.readAuthoritativeFile(uri);
        if (
            before.fileType !== 'doc'
            || before.entityId !== expectedEntityId
            || contentHash(before.content) !== expectedHash
        ) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }

        const documentId = expectedEntityId;
        if (this.documentRecoveryFences.has(documentId)) {
            throw new DocumentRecoveryPendingError(
                `Document ${target.fileName} already has an active recovery fence`,
            );
        }
        this.documentRecoveryFences.add(documentId);
        try {
            if (
                this.documentAcceptAdmissions.get(documentId)?.size
                || this.logicalDocumentAcknowledgements.get(documentId)?.size
                || this.documentPendingOperations.get(documentId)
            ) {
                throw new DocumentRecoveryPendingError(
                    `Document ${target.fileName} has a pending journaled operation or acknowledgement`,
                );
            }

            const registered = this.documentSessions.get(documentId);
            if (registered) {
                const session = await registered;
                if (session.hasPendingOperations) {
                    throw new DocumentRecoveryPendingError(
                        `Document ${target.fileName} has a pending journaled operation`,
                    );
                }
                if (this.documentSessions.get(documentId) === registered) {
                    this.documentSessions.delete(documentId);
                }
                await this.trackDocumentSessionClose(registered);
            }
            await this.waitForDetachedDocumentSessionClosures();

            const journal = await DocumentJournal.openExisting(
                this.documentJournalDirectory,
                documentId,
            );
            if (journal) {
                const hasPendingOperations = journal.hasPendingOperations;
                await journal.close();
                if (hasPendingOperations) {
                    throw new DocumentRecoveryPendingError(
                        `Document ${target.fileName} has durable operations without a proven server disposition`,
                    );
                }
            }

            await this.socket.leaveDoc(documentId).catch(() => {});
            const authoritative = await this.readRemoteDocument(documentId);
            if (contentHash(authoritative) !== expectedHash) {
                throw new ConcurrentRemoteResourceError(uri.path);
            }
            await this.socket.leaveDoc(documentId).catch(() => {});
            await this.moveRemoteEntity(
                identity,
                'doc',
                target.fileEntity,
                target.parentFolder,
                transaction.previous,
            );
            const archived = await this.readRemoteDocument(documentId);
            const archivedHash = contentHash(archived);
            if (archivedHash !== expectedHash) {
                await this.socket.leaveDoc(documentId).catch(() => {});
                await this.rollbackRetainedEntity(
                    identity,
                    uri,
                    'doc',
                    target.fileEntity,
                    transaction.previous,
                    target.parentFolder,
                    archivedHash,
                );
                throw new ConcurrentRemoteResourceError(uri.path);
            }
            await this.assertAuthoritativePathAbsent(identity, uri, true);
            await this.socket.leaveDoc(documentId).catch(() => {});
            await Promise.all([
                DocumentJournal.remove(this.documentJournalDirectory, documentId),
                this.legacyDocumentStateStore.remove(documentId),
            ]);
            this.documentPendingOperations.delete(documentId);
            this.documentJournalEpochs.delete(documentId);
            this.notify([{type: vscode.FileChangeType.Deleted, uri}]);
        } finally {
            this.documentRecoveryFences.delete(documentId);
        }
    }

    /** Move a structurally proven folder into retained trash without deleting its subtree. */
    async removeFolderAgainstBaseline(
        uri: vscode.Uri,
        expectedEntityId: string,
        expectedTreeHash = EMPTY_REMOTE_FOLDER_TREE_HASH,
    ): Promise<void> {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const target = await this._resolveUri(uri);
        if (
            target.fileType !== 'folder'
            || !target.fileEntity
            || target.fileEntity._id !== expectedEntityId
        ) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        const folder = target.fileEntity as FolderEntity;
        if (folder._id === target.parentFolder._id) {
            throw new Error('Safe-sync cannot move the remote project root');
        }
        const internalRoot = this.root?.rootFolder[0].folders.find(
            child => child.name === REMOTE_SAFE_SYNC_DIRECTORY,
        );
        if (internalRoot && this.folderContainsEntity(folder, internalRoot._id)) {
            throw new Error('Remote trash cannot be nested inside the folder being moved');
        }
        const transaction = await this.createRemoteResourceTransaction(identity, 'trash');
        if (this.folderContainsEntity(folder, transaction.previous._id)) {
            throw new Error('Remote trash transaction cannot be nested inside the folder being moved');
        }
        const originalPath = parseUri(uri).pathParts.filter(Boolean).join('/');
        const beforeHash = await this.readAuthoritativeFolderTreeHashAtPath(
            identity,
            originalPath,
            expectedTreeHash,
        );
        if (beforeHash !== expectedTreeHash) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }

        await this.moveRemoteEntity(
            identity,
            'folder',
            folder,
            target.parentFolder,
            transaction.previous,
        );
        const archivedPath = this._resolveById(folder._id)?.path;
        if (!archivedPath) {
            throw new RetainedRemoteCandidateError(
                uri,
                folder._id,
                beforeHash,
                'folder',
            );
        }
        const archivedHash = await this.readAuthoritativeFolderTreeHashAtPath(
            identity,
            archivedPath,
            expectedTreeHash,
        );
        if (archivedHash !== expectedTreeHash) {
            await this.rollbackRetainedEntity(
                identity,
                uri,
                'folder',
                folder,
                transaction.previous,
                target.parentFolder,
                archivedHash,
            );
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        await this.assertAuthoritativePathAbsent(identity, uri, true);
        this.notify([{type: vscode.FileChangeType.Deleted, uri}]);
    }

    /** Fetch a retained binary or document candidate so conflict artifacts can expose it. */
    async readRetainedRemoteCandidate(
        candidate: RetainedRemoteCandidateError | string,
        fileType?: 'doc' | 'file' | 'folder',
    ): Promise<Uint8Array> {
        const entityId = candidate instanceof RetainedRemoteCandidateError
            ? candidate.entityId
            : candidate;
        const resolvedType = candidate instanceof RetainedRemoteCandidateError
            ? candidate.fileType
            : fileType;
        if (resolvedType === 'doc') {
            try {
                return await this.readRemoteDocument(entityId);
            } finally {
                await this.socket.leaveDoc(entityId).catch(() => {});
            }
        }
        if (resolvedType === 'file') {
            const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
            return this.readRemoteResource(identity, entityId);
        }
        throw new Error(`Retained folder ${entityId} has no byte-content projection`);
    }

    /** Inspect only entities still resident in the private retained namespace. */
    async inspectRetainedRemoteCandidate(
        entityId: string,
    ): Promise<RetainedRemoteCandidateInspection | undefined> {
        await this.init();
        const resolved = this._resolveById(entityId);
        if (!resolved) {
            return undefined;
        }
        const retainedPath = resolved.path.replace(/^\/+|\/+$/g, '');
        const historyPrefix = `${REMOTE_SAFE_SYNC_DIRECTORY}/${REMOTE_SAFE_SYNC_HISTORY_DIRECTORY}/`;
        const trashPrefix = `${REMOTE_SAFE_SYNC_DIRECTORY}/${REMOTE_SAFE_SYNC_TRASH_DIRECTORY}/`;
        if (!retainedPath.startsWith(historyPrefix) && !retainedPath.startsWith(trashPrefix)) {
            return undefined;
        }
        if (resolved.fileType === 'doc') {
            let content: Uint8Array;
            try {
                content = await this.readRemoteDocument(entityId);
            } finally {
                await this.socket.leaveDoc(entityId).catch(() => {});
            }
            return {
                entityId,
                fileType: 'doc',
                hash: contentHash(content),
                content,
                retainedPath,
            };
        }
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        if (resolved.fileType === 'file') {
            const content = await this.readRemoteResource(identity, entityId);
            return {
                entityId,
                fileType: 'file',
                hash: contentHash(content),
                content,
                retainedPath,
            };
        }
        if (resolved.fileType === 'folder') {
            return {
                entityId,
                fileType: 'folder',
                hash: await this.readAuthoritativeFolderTreeHashAtPath(
                    identity,
                    retainedPath,
                ),
                retainedPath,
            };
        }
        return undefined;
    }

    async readRemoteFolderTreeHash(uri: vscode.Uri): Promise<string> {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        return this.readAuthoritativeFolderTreeHashAtPath(
            identity,
            parseUri(uri).pathParts.filter(Boolean).join('/'),
        );
    }

    /** Move one unchanged remote entity and prove its identity at the destination. */
    async moveResourceAgainstBaseline(
        oldUri: vscode.Uri,
        newUri: vscode.Uri,
        expectedEntityId: string,
        expectedFileType: 'doc' | 'file' | 'folder',
        expectedHash: string,
    ): Promise<void> {
        const source = await this._resolveUri(oldUri);
        const destination = await this._resolveUri(newUri);
        if (
            source.fileType !== expectedFileType
            || !source.fileEntity
            || source.fileEntity._id !== expectedEntityId
            || destination.fileEntity
        ) {
            throw new ConcurrentRemoteResourceError(oldUri.path);
        }
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        await this.assertAuthoritativePathAbsent(identity, newUri);
        if (expectedFileType === 'folder') {
            const sourceHash = await this.readAuthoritativeFolderTreeHashAtPath(
                identity,
                this.remoteRelativePath(oldUri),
            );
            if (sourceHash !== expectedHash) {
                throw new ConcurrentRemoteResourceError(oldUri.path);
            }
        } else {
            const snapshot = await this.readAuthoritativeFile(oldUri);
            if (
                snapshot.fileType !== expectedFileType
                || snapshot.entityId !== expectedEntityId
                || contentHash(snapshot.content) !== expectedHash
            ) {
                throw new ConcurrentRemoteResourceError(oldUri.path);
            }
        }

        const entity = source.fileEntity;
        const originalName = entity.name;
        const changesName = originalName !== destination.fileName;
        const changesParent = source.parentFolder !== destination.parentFolder;
        let renamed = false;
        if (changesName && changesParent) {
            const intermediatePath = path.posix.join(
                path.posix.dirname(this.remoteRelativePath(oldUri)),
                destination.fileName,
            );
            await this.assertAuthoritativeRelativePathAbsent(
                identity,
                intermediatePath,
                oldUri.path,
            );
        }
        if (changesName) {
            await this.renameRemoteEntity(
                identity,
                expectedFileType,
                entity,
                destination.fileName,
            );
            renamed = true;
        }
        if (changesParent) {
            try {
                await this.moveRemoteEntity(
                    identity,
                    expectedFileType,
                    entity,
                    source.parentFolder,
                    destination.parentFolder,
                );
            } catch (moveError) {
                if (renamed) {
                    try {
                        await this.renameRemoteEntity(
                            identity,
                            expectedFileType,
                            entity,
                            originalName,
                        );
                    } catch (rollbackError) {
                        const moveMessage = moveError instanceof Error
                            ? moveError.message
                            : String(moveError);
                        const rollbackMessage = rollbackError instanceof Error
                            ? rollbackError.message
                            : String(rollbackError);
                        throw new Error(
                            `Remote move failed (${moveMessage}) and rename rollback failed `
                            + `(${rollbackMessage})`,
                        );
                    }
                }
                throw moveError;
            }
        }

        const after = await this._resolveUri(newUri);
        const resolvedById = this._resolveById(expectedEntityId);
        if (
            after.fileType !== expectedFileType
            || after.fileEntity?._id !== expectedEntityId
            || !resolvedById
            || this.normalizeRemoteRelativePath(resolvedById.path)
                !== this.remoteRelativePath(newUri)
        ) {
            throw new Error(`Remote move identity verification failed for ${newUri.path}`);
        }
        await this.assertAuthoritativePathAbsent(identity, oldUri, true);
        await this.assertAuthoritativePathPresent(identity, newUri, expectedFileType, true);
        if (expectedFileType === 'folder') {
            const afterHash = await this.readAuthoritativeFolderTreeHashAtPath(
                identity,
                this.remoteRelativePath(newUri),
                expectedHash,
            );
            if (afterHash !== expectedHash) {
                throw new Error(`Remote folder move hash verification failed for ${newUri.path}`);
            }
        } else {
            const afterSnapshot = await this.readAuthoritativeFile(newUri);
            if (
                afterSnapshot.fileType !== expectedFileType
                || afterSnapshot.entityId !== expectedEntityId
                || contentHash(afterSnapshot.content) !== expectedHash
            ) {
                throw new Error(`Remote move hash verification failed for ${newUri.path}`);
            }
        }
        if (expectedFileType === 'doc') {
            const registered = this.documentSessions.get(expectedEntityId);
            if (registered) {
                await (await registered).bindPath(this.remoteRelativePath(newUri));
            } else {
                await this.acquireDocumentJournalOwnership();
                await this.waitForDetachedDocumentSessionClosures();
                const journal = await DocumentJournal.openExisting(
                    this.documentJournalDirectory,
                    expectedEntityId,
                );
                if (journal) {
                    try {
                        await journal.bindPath(this.remoteRelativePath(newUri), true);
                    } finally {
                        await journal.close();
                    }
                }
            }
        }
        this.notify([
            {type: vscode.FileChangeType.Deleted, uri: oldUri},
            {type: vscode.FileChangeType.Created, uri: newUri},
        ]);
    }

    /** Create a new entity using an explicit conservative doc/resource classification. */
    async createTypedFile(
        uri: vscode.Uri,
        content: Uint8Array,
        fileType: 'doc' | 'file',
    ): Promise<CreatedTypedFile> {
        const {parentFolder, fileName, fileEntity} = await this._resolveUri(uri);
        if (fileEntity) {
            throw vscode.FileSystemError.FileExists(uri);
        }
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        let entity: FileEntity | undefined;
        if (fileType === 'doc') {
            entity = await this.withReclaimedRemoteEntities(identity, async () => {
                const response = await this.api.addDoc(
                    identity, this.projectId, parentFolder._id, fileName,
                );
                if (response.type !== 'success') {
                    // The refusal reason has to survive: it is what tells a
                    // full project apart from every other creation failure.
                    throw new Error(
                        `Could not create remote doc ${uri.path}: `
                        + (response.message ?? 'unknown error'),
                    );
                }
                return response.entity;
            });
        } else {
            const transaction = await this.createRemoteResourceTransaction(identity, 'history');
            entity = await this.withReclaimedRemoteEntities(identity, () => (
                this.uploadRemoteResource(identity, transaction.incoming, fileName, content)
            ));
            await this.assertAuthoritativePathAbsent(identity, uri);
            await this.moveRemoteEntity(
                identity,
                'file',
                entity,
                transaction.incoming,
                parentFolder,
            );
        }
        if (!entity?._type) {
            throw new Error(`Overleaf did not create ${fileType} ${uri.path}`);
        }
        this.insertEntity(parentFolder, entity._type, entity);
        this.notify([{ type: vscode.FileChangeType.Created, uri }]);
        let authoritativeContent = content;
        if (fileType === 'doc' && content.byteLength > 0) {
            const target = new TextDecoder('utf-8', { fatal: true }).decode(content);
            const actual = await this.writeDocumentAgainstAncestor(uri, '', target, entity._id);
            authoritativeContent = new TextEncoder().encode(actual);
        }
        return { entityId: entity._id, content: authoritativeContent };
    }

    private async createRemoteResourceTransaction(
        identity: Identity,
        kind: 'history' | 'trash',
    ): Promise<{incoming: FolderEntity; previous: FolderEntity}> {
        const internal = await this.getOwnedRemoteSafeSyncNamespace(identity);
        const kindName = kind === 'history'
            ? REMOTE_SAFE_SYNC_HISTORY_DIRECTORY
            : REMOTE_SAFE_SYNC_TRASH_DIRECTORY;
        const kindFolder = await this.ensureRemoteInternalFolder(identity, internal, kindName);
        this.scheduleRetainedRemotePrune(identity, kindFolder);
        return this.withReclaimedRemoteEntities(
            identity,
            () => this.openRetainedRemoteTransaction(identity, kindFolder),
        );
    }

    /**
     * Reclaim in the background. Routine reclamation is maintenance for the
     * next operation, not for this one: awaiting it would put a queue of
     * deletes in front of every retained transaction, and a replica that is
     * busy tidying is a replica that has not converged yet.
     */
    private scheduleRetainedRemotePrune(identity: Identity, kindFolder: FolderEntity): void {
        if (this.retainedRemotePrune) {
            return;
        }
        const pruning = this.pruneRetainedRemoteTransactions(identity, kindFolder, 'aged')
            .then(() => undefined, () => undefined)
            .then(() => {
                if (this.retainedRemotePrune === pruning) {
                    this.retainedRemotePrune = undefined;
                }
            });
        this.retainedRemotePrune = pruning;
    }

    /**
     * Run a remote creation that the project may be too full to accept. The
     * entities retention is holding are the ones this replica can give back:
     * reclaiming them is the difference between a project that keeps
     * converging and one whose every delete fails from here on.
     */
    private async withReclaimedRemoteEntities<T>(
        identity: Identity,
        operation: () => Promise<T>,
    ): Promise<T> {
        try {
            return await operation();
        } catch (error) {
            if (
                !isRemoteEntityLimitFailure(error)
                || !await this.reclaimRetainedRemoteTransactions(identity)
            ) {
                throw error;
            }
            return await operation();
        }
    }

    private async reclaimRetainedRemoteTransactions(identity: Identity): Promise<boolean> {
        const internal = await this.getOwnedRemoteSafeSyncNamespace(identity);
        let reclaimed = false;
        for (const kindName of [
            REMOTE_SAFE_SYNC_TRASH_DIRECTORY,
            REMOTE_SAFE_SYNC_HISTORY_DIRECTORY,
        ]) {
            // Look the folder up instead of ensuring it: a project with no
            // entities left to give is the one case where creating the folder
            // that reclamation reads would itself fail.
            const kindFolder = (internal.folders ?? []).find(folder => folder.name === kindName);
            if (!kindFolder) {
                continue;
            }
            const freed = await this.pruneRetainedRemoteTransactions(
                identity,
                kindFolder,
                'pressured',
            );
            reclaimed = reclaimed || freed;
        }
        return reclaimed;
    }

    private async openRetainedRemoteTransaction(
        identity: Identity,
        kindFolder: FolderEntity,
    ): Promise<{incoming: FolderEntity; previous: FolderEntity}> {
        const transaction = await this.ensureRemoteInternalFolder(
            identity,
            kindFolder,
            `${new Date().toISOString().replace(/[^0-9]/g, '')}-${randomUUID()}`,
        );
        const incoming = await this.ensureRemoteInternalFolder(identity, transaction, 'incoming');
        const previous = await this.ensureRemoteInternalFolder(identity, transaction, 'previous');
        return {incoming, previous};
    }

    /**
     * Retention that only grows ends as a project at its entity limit, where
     * the server refuses exactly the deletes and replacements that retention
     * exists to make recoverable. Keep a bounded window of recent transactions
     * and reclaim the rest.
     *
     * Age is what protects correctness here, not the count: a transaction that
     * another endpoint is still writing into, or that a journaled action can
     * still be recovered from, is minutes old at most. Under project-limit
     * pressure the window shrinks to that in-flight horizon, never below it.
     */
    private async pruneRetainedRemoteTransactions(
        identity: Identity,
        kindFolder: FolderEntity,
        scope: 'aged' | 'pressured',
    ): Promise<boolean> {
        const ordered = [...kindFolder.folders ?? []]
            .sort((left, right) => left.name.localeCompare(right.name));
        const excess = ordered.length - MAX_RETAINED_REMOTE_TRANSACTIONS;
        if (excess <= 0) {
            return false;
        }
        const now = Date.now();
        const newestRetained = now - (
            scope === 'aged'
                ? RETAINED_REMOTE_TRANSACTION_AGE_MS
                : RETAINED_REMOTE_TRANSACTION_INFLIGHT_MS
        );
        const deadline = now + (
            scope === 'aged'
                ? RETAINED_REMOTE_PRUNE_BUDGET_MS
                : RETAINED_REMOTE_PRESSURED_PRUNE_BUDGET_MS
        );
        let reclaimed = 0;
        for (const folder of ordered.slice(0, excess)) {
            // Reclaim in small passes. A burst of deletes is a burst of
            // entity events for every endpoint joined to this project, and
            // maintenance must not be what disturbs their sync.
            if (Date.now() > deadline || reclaimed >= RETAINED_REMOTE_PRUNE_BATCH) {
                break;
            }
            const createdAt = retainedRemoteTransactionTime(folder.name);
            // An unrecognized name was not written by this retention scheme.
            // Nothing here is entitled to delete another owner's folder.
            if (createdAt === undefined || createdAt > newestRetained) {
                continue;
            }
            const response = await this.api.deleteEntity(
                identity,
                this.projectId,
                'folder',
                folder._id,
            );
            if (response.type !== 'success') {
                // A peer may have reclaimed the same folder. Pruning is
                // maintenance: it reports what it freed and never fails the
                // operation it is making room for.
                continue;
            }
            this.removeEntityById(kindFolder, 'folder', folder._id, true);
            reclaimed++;
        }
        return reclaimed > 0;
    }

    /** Verify ownership before any caller hides or mutates the reserved remote namespace. */
    async ensureSafeSyncRemoteNamespace(legacyNamespaceId?: string): Promise<void> {
        await this.verifyRemoteSafeSyncNamespace(undefined, legacyNamespaceId, false);
    }

    private async getOwnedRemoteSafeSyncNamespace(
        identity?: Identity,
        legacyNamespaceId?: string,
    ): Promise<FolderEntity> {
        if (this.remoteSafeSyncNamespacePromise) {
            return this.remoteSafeSyncNamespacePromise;
        }
        const verification = this.verifyRemoteSafeSyncNamespace(
            identity,
            legacyNamespaceId,
            true,
        ).then(namespace => {
            if (!namespace) {
                throw new Error('Remote safe-sync namespace was not created');
            }
            return namespace;
        });
        this.remoteSafeSyncNamespacePromise = verification;
        try {
            return await verification;
        } finally {
            if (this.remoteSafeSyncNamespacePromise === verification) {
                this.remoteSafeSyncNamespacePromise = undefined;
            }
        }
    }

    private async verifyRemoteSafeSyncNamespace(
        suppliedIdentity?: Identity,
        legacyNamespaceId?: string,
        createIfMissing = false,
    ): Promise<FolderEntity | undefined> {
        const project = await this.init();
        const root = project.rootFolder[0];
        const matchingFolders = root.folders.filter(
            folder => folder.name === REMOTE_SAFE_SYNC_DIRECTORY,
        );
        const stagingFolders = root.folders.filter(
            folder => folder.name.startsWith(REMOTE_SAFE_SYNC_STAGING_PREFIX),
        );
        if (matchingFolders.length > 1) {
            throw new Error('Remote safe-sync namespace has duplicate root folders');
        }
        if (matchingFolders.length > 0 && stagingFolders.length > 0) {
            throw new Error('Remote safe-sync namespace has an ambiguous leftover staging folder');
        }
        const blocked = root.docs.some(entity => entity.name === REMOTE_SAFE_SYNC_DIRECTORY)
            || root.fileRefs.some(entity => entity.name === REMOTE_SAFE_SYNC_DIRECTORY);
        if (blocked) {
            throw new Error(`Remote safe-sync namespace is blocked by ${REMOTE_SAFE_SYNC_DIRECTORY}`);
        }

        let internal = matchingFolders[0];
        if (!internal) {
            let identity = suppliedIdentity;
            if (stagingFolders.length > 0) {
                identity ??= await GlobalStateManager.authenticate(this.context, this.serverName);
                const recoverable = await this.findRecoverableRemoteSafeSyncStaging(identity, root);
                if (stagingFolders.length !== 1 || recoverable !== stagingFolders[0]) {
                    throw new Error(
                        'Refusing to hide an unowned or ambiguous remote safe-sync staging folder',
                    );
                }
            }
            if (!createIfMissing) {
                return undefined;
            }
            identity ??= await GlobalStateManager.authenticate(this.context, this.serverName);
            return this.createOrRecoverRemoteSafeSyncNamespace(identity, root);
        }

        const identity = suppliedIdentity
            ?? await GlobalStateManager.authenticate(this.context, this.serverName);
        internal.docs ??= [];
        internal.fileRefs ??= [];
        internal.folders ??= [];
        const markerFiles = internal.fileRefs.filter(
            entity => entity.name === REMOTE_SAFE_SYNC_OWNER_FILE,
        );
        const markerBlocked = internal.docs.some(
            entity => entity.name === REMOTE_SAFE_SYNC_OWNER_FILE,
        ) || internal.folders.some(
            entity => entity.name === REMOTE_SAFE_SYNC_OWNER_FILE,
        );
        const legacyFolders = internal.folders.map(folder => folder.name);
        const canExplicitlyAdoptLegacy = markerFiles.length === 0
            && !markerBlocked
            && internal.docs.length === 0
            && internal.fileRefs.length === 0
            && legacyFolders.length > 0
            && new Set(legacyFolders).size === legacyFolders.length
            && legacyFolders.every(name => (
                name === REMOTE_SAFE_SYNC_HISTORY_DIRECTORY
                || name === REMOTE_SAFE_SYNC_TRASH_DIRECTORY
            ))
            && typeof legacyNamespaceId === 'string'
            && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
                .test(legacyNamespaceId);
        if (canExplicitlyAdoptLegacy) {
            const marker: RemoteSafeSyncOwnerMarker = {
                schemaVersion: REMOTE_SAFE_SYNC_OWNER_SCHEMA_VERSION,
                projectId: this.projectId,
                extensionId: ROOT_NAME,
                extensionName: ELEGANT_NAME,
                namespaceId: legacyNamespaceId!,
            };
            await this.uploadRemoteResource(
                identity,
                internal,
                REMOTE_SAFE_SYNC_OWNER_FILE,
                new TextEncoder().encode(`${JSON.stringify(marker, undefined, 2)}\n`),
            );
            return internal;
        }
        if (markerBlocked || markerFiles.length !== 1) {
            throw new Error(
                'Refusing to reuse an unowned remote .hershel-safe-sync directory: '
                + 'the ownership marker is missing or ambiguous',
            );
        }
        let marker: unknown;
        try {
            const content = await this.readRemoteResource(identity, markerFiles[0]._id);
            marker = JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(content));
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            throw new Error(`Remote safe-sync ownership marker is unreadable: ${message}`);
        }
        if (!this.isOwnedRemoteSafeSyncMarker(marker)) {
            throw new Error(
                'Refusing to reuse an unowned remote .hershel-safe-sync directory: '
                + 'the ownership marker does not match this project and extension',
            );
        }
        return internal;
    }

    private async createOrRecoverRemoteSafeSyncNamespace(
        identity: Identity,
        root: FolderEntity,
    ): Promise<FolderEntity> {
        const staging = await this.findRecoverableRemoteSafeSyncStaging(identity, root)
            ?? await this.createRemoteSafeSyncStaging(identity, root);
        const response = await this.api.renameEntity(
            identity,
            this.projectId,
            'folder',
            staging._id,
            REMOTE_SAFE_SYNC_DIRECTORY,
        );
        if (response.type !== 'success') {
            throw new Error(
                'Could not finalize remote safe-sync namespace: '
                + (response.message ?? 'unknown error'),
            );
        }
        staging.name = REMOTE_SAFE_SYNC_DIRECTORY;
        return staging;
    }

    private async findRecoverableRemoteSafeSyncStaging(
        identity: Identity,
        root: FolderEntity,
    ): Promise<FolderEntity | undefined> {
        const candidates = root.folders
            .filter(folder => folder.name.startsWith(REMOTE_SAFE_SYNC_STAGING_PREFIX))
            .sort((left, right) => left.name.localeCompare(right.name));
        for (const candidate of candidates) {
            candidate.docs ??= [];
            candidate.fileRefs ??= [];
            candidate.folders ??= [];
            const markerFiles = candidate.fileRefs.filter(
                entity => entity.name === REMOTE_SAFE_SYNC_OWNER_FILE,
            );
            if (
                candidate.docs.length !== 0
                || candidate.folders.length !== 0
                || candidate.fileRefs.length !== 1
                || markerFiles.length !== 1
            ) {
                continue;
            }
            try {
                const content = await this.readRemoteResource(identity, markerFiles[0]._id);
                const marker = JSON.parse(
                    new TextDecoder('utf-8', {fatal: true}).decode(content),
                );
                if (
                    this.isOwnedRemoteSafeSyncMarker(marker)
                    && candidate.name === `${REMOTE_SAFE_SYNC_STAGING_PREFIX}${marker.namespaceId}`
                ) {
                    return candidate;
                }
            } catch {
                // An incomplete or foreign staging folder is never promoted.
            }
        }
        return undefined;
    }

    private async createRemoteSafeSyncStaging(
        identity: Identity,
        root: FolderEntity,
    ): Promise<FolderEntity> {
        const namespaceId = randomUUID();
        const staging = await this.ensureRemoteInternalFolder(
            identity,
            root,
            `${REMOTE_SAFE_SYNC_STAGING_PREFIX}${namespaceId}`,
        );
        const marker: RemoteSafeSyncOwnerMarker = {
            schemaVersion: REMOTE_SAFE_SYNC_OWNER_SCHEMA_VERSION,
            projectId: this.projectId,
            extensionId: ROOT_NAME,
            extensionName: ELEGANT_NAME,
            namespaceId,
        };
        await this.uploadRemoteResource(
            identity,
            staging,
            REMOTE_SAFE_SYNC_OWNER_FILE,
            new TextEncoder().encode(`${JSON.stringify(marker, undefined, 2)}\n`),
        );
        return staging;
    }

    private isOwnedRemoteSafeSyncMarker(marker: unknown): marker is RemoteSafeSyncOwnerMarker {
        if (!marker || typeof marker !== 'object') {
            return false;
        }
        const value = marker as Partial<RemoteSafeSyncOwnerMarker>;
        return value.schemaVersion === REMOTE_SAFE_SYNC_OWNER_SCHEMA_VERSION
            && value.projectId === this.projectId
            && value.extensionId === ROOT_NAME
            && value.extensionName === ELEGANT_NAME
            && typeof value.namespaceId === 'string'
            && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
                .test(value.namespaceId);
    }

    private async ensureRemoteInternalFolder(
        identity: Identity,
        parent: FolderEntity,
        name: string,
    ): Promise<FolderEntity> {
        const existing = parent.folders.find(folder => folder.name === name);
        if (existing) {
            return existing;
        }
        if (
            parent.docs.some(entity => entity.name === name)
            || parent.fileRefs.some(entity => entity.name === name)
        ) {
            throw new Error(`Remote safe-sync namespace is blocked by ${name}`);
        }
        const response = await this.api.addFolder(identity, this.projectId, name, parent._id);
        if (response.type !== 'success' || !response.entity) {
            throw new Error(
                `Could not create remote safe-sync folder ${name}: ${response.message ?? 'unknown error'}`,
            );
        }
        const folder = response.entity as FolderEntity;
        folder._type = 'folder';
        folder.docs ??= [];
        folder.fileRefs ??= [];
        folder.folders ??= [];
        this.insertEntity(parent, 'folder', folder);
        return folder;
    }

    private async uploadRemoteResource(
        identity: Identity,
        parent: FolderEntity,
        name: string,
        content: Uint8Array,
    ): Promise<FileEntity> {
        const response = await this.api.uploadFile(
            identity,
            this.projectId,
            parent._id,
            name,
            content,
            'application/octet-stream',
        );
        if (response.type !== 'success' || response.entity?._type !== 'file') {
            // The refusal reason has to survive: it is what tells a full
            // project apart from every other upload failure.
            throw new Error(
                `Overleaf did not create binary resource ${name}`
                + (response.message ? `: ${response.message}` : ''),
            );
        }
        const entity = response.entity;
        this.insertEntity(parent, 'file', entity);
        const uploaded = await this.readRemoteResource(identity, entity._id);
        if (contentHash(uploaded) !== contentHash(content)) {
            throw new Error(`Remote resource staging verification failed for ${name}`);
        }
        return entity;
    }

    private async readRemoteResource(identity: Identity, entityId: string): Promise<Uint8Array> {
        const response = await this.api.getFile(identity, this.projectId, entityId);
        if (response.type !== 'success' || response.content === undefined) {
            throw new Error(`Could not read remote resource ${entityId}`);
        }
        return response.content;
    }

    private async readRemoteDocument(entityId: string): Promise<Uint8Array> {
        const joined = await this.socket.joinDoc(entityId);
        if (joined.otType !== 'sharejs-text-ot') {
            throw new Error(`Unsupported Overleaf document OT type: ${joined.otType}`);
        }
        return new TextEncoder().encode(joined.docLines.join('\n'));
    }

    private async moveRemoteEntity(
        identity: Identity,
        fileType: 'doc' | 'file' | 'folder',
        entity: FileEntity,
        source: FolderEntity,
        destination: FolderEntity,
    ): Promise<void> {
        const response = await this.api.moveEntity(
            identity,
            this.projectId,
            fileType,
            entity._id,
            destination._id,
        );
        if (response.type !== 'success') {
            throw new Error(
                `Could not move remote resource ${entity.name}: ${response.message ?? 'unknown error'}`,
            );
        }
        this.removeEntity(source, fileType, entity);
        this.insertEntity(destination, fileType, entity);
    }

    private async renameRemoteEntity(
        identity: Identity,
        fileType: 'doc' | 'file' | 'folder',
        entity: FileEntity,
        name: string,
    ): Promise<void> {
        const response = await this.api.renameEntity(
            identity,
            this.projectId,
            fileType,
            entity._id,
            name,
        );
        if (response.type !== 'success') {
            throw new Error(
                `Could not rename remote resource ${entity.name}: `
                + (response.message ?? 'unknown error'),
            );
        }
        entity.name = name;
    }

    private async rollbackRetainedEntity(
        identity: Identity,
        uri: vscode.Uri,
        fileType: 'doc' | 'file' | 'folder',
        entity: FileEntity,
        retainedParent: FolderEntity,
        originalParent: FolderEntity,
        archivedHash: string,
    ): Promise<void> {
        try {
            await this.assertAuthoritativePathAbsent(identity, uri, true);
            await this.moveRemoteEntity(
                identity,
                fileType,
                entity,
                retainedParent,
                originalParent,
            );
        } catch {
            throw new RetainedRemoteCandidateError(
                uri,
                entity._id,
                archivedHash,
                fileType,
            );
        }
    }

    private folderContainsEntity(folder: FolderEntity, entityId: string): boolean {
        if (folder._id === entityId) {
            return true;
        }
        return folder.folders.some(child => this.folderContainsEntity(child, entityId));
    }

    /**
     * Overleaf's realtime project snapshot is the structural authority used by
     * the collaborative editor.  The legacy `/entities` REST projection can
     * retain a pre-move path long after the mutation acknowledgement, so it
     * must not act as a compare-and-swap oracle for safe-sync mutations.
     */
    private async readFreshProjectSnapshot(identity: Identity): Promise<ProjectEntity> {
        const serverUrl = GlobalStateManager.getServerUrl(this.context, this.serverName);
        if (!serverUrl) {
            throw new Error(`Server URL is unavailable for ${this.serverName}`);
        }
        const observer = new SocketIOAPI(serverUrl, this.api, identity, this.projectId);
        try {
            try {
                return await observer.joinProject(this.projectId);
            } catch (error) {
                if (!observer.needsReinit) {
                    throw error;
                }
                observer.init();
                return await observer.joinProject(this.projectId);
            }
        } finally {
            observer.disconnect();
        }
    }

    private resolveFreshProjectPath(
        project: ProjectEntity,
        relativePath: string,
    ): {fileType: 'doc' | 'file' | 'folder'; fileEntity: FileEntity} | undefined {
        const normalized = this.normalizeRemoteRelativePath(relativePath);
        const root = project.rootFolder[0];
        if (!root) {
            return undefined;
        }
        if (!normalized) {
            return {fileType: 'folder', fileEntity: root};
        }
        const parts = normalized.split('/');
        let current = root;
        for (let index = 0; index < parts.length; index++) {
            const name = parts[index];
            const final = index === parts.length - 1;
            const child = current.folders.find(folder => folder.name === name);
            if (!final) {
                if (!child) {
                    return undefined;
                }
                current = child;
                continue;
            }
            if (child) {
                return {fileType: 'folder', fileEntity: child};
            }
            const document = current.docs.find(entity => entity.name === name);
            if (document) {
                return {fileType: 'doc', fileEntity: document};
            }
            const file = current.fileRefs.find(entity => entity.name === name);
            return file ? {fileType: 'file', fileEntity: file} : undefined;
        }
        return undefined;
    }

    private resolveFreshDocumentPath(
        project: ProjectEntity,
        documentId: string,
    ): string | undefined {
        const root = project.rootFolder[0];
        if (!root) {
            return undefined;
        }
        const visit = (folder: FolderEntity, prefix: string): string | undefined => {
            const document = folder.docs.find(candidate => candidate._id === documentId);
            if (document) {
                return `${prefix}${document.name}`;
            }
            for (const child of folder.folders) {
                const resolved = visit(child, `${prefix}${child.name}/`);
                if (resolved !== undefined) {
                    return resolved;
                }
            }
            return undefined;
        };
        return visit(root, '');
    }

    private freshFolderTreeHash(folder: FolderEntity): string {
        const tree: Array<{path: string; type: 'doc' | 'file' | 'folder'}> = [];
        const visit = (current: FolderEntity, prefix: string): void => {
            for (const document of current.docs) {
                tree.push({path: `${prefix}${document.name}`, type: 'doc'});
            }
            for (const file of current.fileRefs) {
                tree.push({path: `${prefix}${file.name}`, type: 'file'});
            }
            for (const child of current.folders) {
                const childPath = `${prefix}${child.name}`;
                tree.push({path: childPath, type: 'folder'});
                visit(child, `${childPath}/`);
            }
        };
        visit(folder, '');
        tree.sort((left, right) => (
            left.path.localeCompare(right.path) || left.type.localeCompare(right.type)
        ));
        return contentHash(new TextEncoder().encode(JSON.stringify(tree)));
    }

    private async readAuthoritativeFolderTreeHashAtPath(
        identity: Identity,
        folderPath: string,
        expectedHash?: string,
    ): Promise<string> {
        const normalizedFolder = folderPath.replace(/^\/+|\/+$/g, '');
        const deadline = Date.now() + REMOTE_MUTATION_OBSERVATION_TIMEOUT_MS;
        let lastHash = MISSING_REMOTE_FOLDER_TREE_HASH;
        while (true) {
            const project = await this.readFreshProjectSnapshot(identity);
            const resolved = this.resolveFreshProjectPath(project, normalizedFolder);
            if (resolved?.fileType === 'folder') {
                lastHash = this.freshFolderTreeHash(resolved.fileEntity as FolderEntity);
                if (expectedHash === undefined || lastHash === expectedHash) {
                    return lastHash;
                }
            }
            if (expectedHash === undefined) {
                throw new ConcurrentRemoteResourceError(folderPath);
            }
            if (Date.now() >= deadline) {
                return lastHash;
            }
            await new Promise(resolve => setTimeout(
                resolve,
                REMOTE_MUTATION_OBSERVATION_INTERVAL_MS,
            ));
        }
    }

    private async assertAuthoritativePathAbsent(
        identity: Identity,
        uri: vscode.Uri,
        waitForMutation = false,
    ): Promise<void> {
        return this.assertAuthoritativeRelativePathAbsent(
            identity,
            this.remoteRelativePath(uri),
            uri.path,
            waitForMutation,
        );
    }

    private async assertAuthoritativeRelativePathAbsent(
        identity: Identity,
        relativePath: string,
        displayPath: string,
        waitForMutation = false,
    ): Promise<void> {
        const expected = this.normalizeRemoteRelativePath(relativePath);
        const deadline = Date.now() + REMOTE_MUTATION_OBSERVATION_TIMEOUT_MS;
        while (true) {
            const project = await this.readFreshProjectSnapshot(identity);
            const occupied = this.resolveFreshProjectPath(project, expected) !== undefined;
            if (!occupied) {
                return;
            }
            if (!waitForMutation || Date.now() >= deadline) {
                throw new ConcurrentRemoteResourceError(displayPath);
            }
            await new Promise(resolve => setTimeout(
                resolve,
                REMOTE_MUTATION_OBSERVATION_INTERVAL_MS,
            ));
        }
    }

    private async assertAuthoritativePathPresent(
        identity: Identity,
        uri: vscode.Uri,
        expectedFileType: 'doc' | 'file' | 'folder',
        waitForMutation = false,
    ): Promise<void> {
        const expected = this.remoteRelativePath(uri);
        const deadline = Date.now() + REMOTE_MUTATION_OBSERVATION_TIMEOUT_MS;
        while (true) {
            const project = await this.readFreshProjectSnapshot(identity);
            const resolved = this.resolveFreshProjectPath(project, expected);
            if (resolved?.fileType === expectedFileType) {
                return;
            }
            if (!waitForMutation || Date.now() >= deadline) {
                throw new Error(`Remote move path verification failed for ${uri.path}`);
            }
            await new Promise(resolve => setTimeout(
                resolve,
                REMOTE_MUTATION_OBSERVATION_INTERVAL_MS,
            ));
        }
    }

    private remoteRelativePath(uri: vscode.Uri): string {
        return this.normalizeRemoteRelativePath(
            parseUri(uri).pathParts.filter(Boolean).join('/'),
        );
    }

    private normalizeRemoteRelativePath(remotePath: string): string {
        return remotePath.replace(/^\/+|\/+$/g, '');
    }

    async resolve(uri: vscode.Uri): Promise<File> {
        const {fileName, fileEntity, fileType} = await this._resolveUri(uri);
        const readonly = fileEntity?.readonly ? vscode.FilePermission.Readonly : undefined;
        switch (fileType) {
            case undefined:
                throw vscode.FileSystemError.FileNotFound(uri);
            case 'folder':
                return new File(fileName, vscode.FileType.Directory, undefined, readonly);
            case 'file':
                if ((fileEntity as FileRefEntity).linkedFileData!==null) {
                    return new File(fileName, vscode.FileType.File | vscode.FileType.SymbolicLink, Date.parse((fileEntity as FileRefEntity).created), readonly);
                } else {
                    return new File(fileName, vscode.FileType.File, Date.parse((fileEntity as FileRefEntity).created), readonly);
                }
            default:
                return new File(fileName, vscode.FileType.File, undefined, readonly);
        }
    }

    async list(uri: vscode.Uri): Promise<[string, vscode.FileType][]> {
        const {fileEntity} = await this._resolveUri(uri);
        const folder = fileEntity as FolderEntity;
        let results:[string, vscode.FileType][] = [];
        if (folder) {
            Object.values(FolderKeys).forEach((key) => {
                const _type = key==='folders'? vscode.FileType.Directory : vscode.FileType.File;
                folder[key]?.forEach((entity) => {
                    results.push([entity.name, _type]);
                });
            });
        }
        return results;
    }

    async openFile(uri: vscode.Uri): Promise<Uint8Array> {
        const {fileType, fileEntity} = await this._resolveUri(uri);
        if (!fileEntity) {
            throw vscode.FileSystemError.FileNotFound();
        }

        if (fileType==='doc') {
            const doc = fileEntity as DocumentEntity;
            const resolved = this._resolveById(doc._id);
            const session = await this.getDocumentSession(doc, resolved?.path ?? fileEntity.name);
            EventBus.fire('fileWillOpenEvent', {uri});
            return new TextEncoder().encode(session.content);
        } else if (fileType==='outputs') {
            const {compileGroup, clsiServerId, pdfDownloadDomain} = this;
            return GlobalStateManager.authenticate(this.context, this.serverName)
            .then((identity) => {
                return this.api.getFileFromClsi(identity, (fileEntity as OutputFileEntity).url, compileGroup || 'standard', clsiServerId, pdfDownloadDomain)
                .then((res) => {
                    if (res.type==='success') {
                        EventBus.fire('fileWillOpenEvent', {uri});
                        return res.content;
                    } else {
                        return new Uint8Array(0);
                    }
                });
            });
        } else {
            const fileId = fileEntity._id;
            const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
            const res = await this.api.getFile(identity, this.projectId, fileId);
            if (res.type==='success' && res.content) {
                EventBus.fire('fileWillOpenEvent', {uri});
                return res.content;
            } else {
                return new Uint8Array(0);
            }
        }
    }

    async createFile(uri: vscode.Uri, content:Uint8Array, overwrite?:boolean) {
        const {parentFolder, fileName, fileEntity, fileType} = await this._resolveUri(uri);
        if (fileEntity && !overwrite) {
            throw vscode.FileSystemError.FileExists(uri);
        }

        let res = undefined;
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);

        if (content.length===0) {
            const _res = await this.api.addDoc(identity, this.projectId, parentFolder._id, fileName);
            if (_res.type==='success') {
                res = _res.entity;
            }
        } else {
            const parentFolderId = parentFolder._id;
            const _res = await this.api.uploadFile(identity, this.projectId, parentFolderId, fileName, content);
            if (_res.type==='success' && _res.entity!==undefined) {
                res = _res.entity;
            } else {
                if (_res.message!==undefined) {
                    vscode.window.showErrorMessage(_res.message);
                }
            }
        }
        if (res && res._type) {
            if (fileEntity && fileType) {
                this.removeEntity(parentFolder, fileType, fileEntity);
            }
            this.insertEntity(parentFolder, res._type, res);
            this.notify([
                {type: vscode.FileChangeType.Created, uri: uri},
            ]);
        }
    }

    async refreshLinkedFile(uri: vscode.Uri) {
        const {fileType, fileEntity} = await this._resolveUri(uri);
        if (fileType==='file' && fileEntity) {
            if ((fileEntity as FileRefEntity).linkedFileData===null) { return; }

            vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: `${vscode.l10n.t('Refreshing')} ${fileEntity.name}`,
                cancellable: true,
            }, async (progress, token) => {
                token.onCancellationRequested(() => {});
                
                const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
                const res = await (this.api as ExtendedBaseAPI).refreshLinkedFile(identity, this.projectId, fileEntity._id);

                if (res.type==='success' && res.message!==undefined) {
                    // refresh the entity id
                    fileEntity._id = res.message;
                    this.notify([
                        {type: vscode.FileChangeType.Changed, uri: uri},
                    ]);
                    progress.report({message: vscode.l10n.t('Done')});
                } else {
                    if (res.message!==undefined) {
                        throw new Error(res.message);
                    }
                }
            });
        }
    }

    async createLinkedFile(uri: vscode.Uri) {
        const res = await this._resolveUri(uri);
        const parentFolder = res.fileType==='folder' ? res.fileEntity as FolderEntity : res.parentFolder;

        const supportedProviders = [
            vscode.l10n.t('From Another Project'),
            vscode.l10n.t('From External URL'),
        ];
        const selection = await vscode.window.showQuickPick(supportedProviders, {
            placeHolder: vscode.l10n.t('Import file from...'),
        });

        let provider = undefined, entityId = undefined, fileName = undefined, data = undefined;
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        if (selection === vscode.l10n.t('From Another Project')) {
            provider = 'project_file';
            const allTags = (await this.api.getAllTags(identity)).tags || [];
            const projectId = await vscode.window.showQuickPick(
                (await this.api.userProjectsJson(identity)).projects!
                .filter(project => project.id!==this.projectId)
                .map(project => {
                    let detail = '';
                    for (const tag of allTags) {
                        if (tag.project_ids.includes(project.id)) {
                            detail += `$(tag) ${tag.name} `;
                        }
                    }
                    return {label: project.name, id: project.id, detail};
                }),
                {
                    title: vscode.l10n.t('Select a Project'),
                    ignoreFocusOut: true,
                }
            );
            const filePath = projectId && await vscode.window.showQuickPick(
                (await this.api.projectEntitiesJson(identity, projectId!.id)).entities!.map(entity => entity.path),
                {
                    title: vscode.l10n.t('Select a File'),
                    ignoreFocusOut: true,
                }
            );
            fileName = filePath && await vscode.window.showInputBox({
                title: vscode.l10n.t('File Name In This Project'),
                value: filePath?.split('/').pop(),
                ignoreFocusOut: true,
                validateInput: (value) => {
                    if (value==='' || value===undefined || value.match(/^[^\/?%*:|"<>]+$/g)===null) {
                        return vscode.l10n.t('File name is empty or contains invalid characters');
                    } else if (parentFolder.fileRefs.find((fileRef) => fileRef.name===value) !== undefined) {
                        return vscode.l10n.t('A file or folder with this name already exists');
                    }
                }
            });
            //
            data = {source_entity_path: filePath!, source_project_id: projectId!.id};
            const res = await (this.api as ExtendedBaseAPI).createLinkedFile(identity, this.projectId, parentFolder._id, fileName!, provider, data);
            if (res.type==='success' && res.message!==undefined) {
                entityId = res.message;
            }
        } else if (selection === vscode.l10n.t('From External URL')) {
            provider = 'url';
            const url = await vscode.window.showInputBox({
                title: vscode.l10n.t('URL to fetch the file from'),
                placeHolder: 'https://example.com/my-file.png',
                ignoreFocusOut: true,
            });
            fileName = url && await vscode.window.showInputBox({
                title: vscode.l10n.t('File Name In This Project'),
                value: url?.split('/').pop(),
                ignoreFocusOut: true,
                validateInput: (value) => {
                    if (value==='' || value===undefined || value.match(/^[^\/?%*:|"<>]+$/g)===null) {
                        return vscode.l10n.t('File name is empty or contains invalid characters');
                    } else if (parentFolder.fileRefs.find((fileRef) => fileRef.name===value) !== undefined) {
                        return vscode.l10n.t('A file or folder with this name already exists');
                    }
                }
            });
            //
            data = {url:url!};
            const res = await (this.api as ExtendedBaseAPI).createLinkedFile(identity, this.projectId, parentFolder._id, fileName!, provider, data);
            if (res.type==='success' && res.message!==undefined) {
                entityId = res.message;
            }
        } else {
            return;
        }

        // insert entity
        const entity = {
            _id: entityId!, name: fileName!, _type: 'file', readonly: false,
            linkedFileData: { provider, ...data! },
            created: new Date().toISOString(),
        } as FileRefEntity;
        this.insertEntity(parentFolder, 'file', entity);
        const {path} = this._resolveById(entityId!)!;
        this.notify([
            {type: vscode.FileChangeType.Created, uri: uri.with({path:`/${this.projectName}${path}`})},
        ]);
    }

    async writeFile(uri: vscode.Uri, content:Uint8Array, create:boolean, overwrite:boolean) {
        const {fileType, fileEntity} = await this._resolveUri(uri);

        // if non-exists --> create it
        if (!fileType && create) {
            return this.createFile(uri, content, true);
        }

        // if exists but not doc --> create new
        if (fileType && fileType!=='doc' && create) {
            return this.createFile(uri, content, overwrite);
        }

        // if exists and is doc --> update
        if (fileType && fileType==='doc' && fileEntity) {
            const decoded = new TextDecoder('utf-8', {fatal:true}).decode(content);
            const projection = await this.readDocumentProjection(uri);
            this.isDirty = decoded !== projection.content;
            await this.writeDocumentAgainstAncestor(
                uri,
                projection.content,
                decoded,
                projection.entityId,
            );
            this.isDirty = false;
        }
    }

    async mkdir(uri: vscode.Uri) {
        const {parentFolder, fileName} = await this._resolveUri(uri);
        const [folderName, parentFolderId] = [fileName, parentFolder._id];
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.addFolder(identity, this.projectId, folderName, parentFolderId);

        if (res.type==='success' && res.entity!==undefined) {
            this.insertEntity(parentFolder, 'folder', res.entity as FolderEntity);
            this.notify([
                {type: vscode.FileChangeType.Created, uri: uri},
            ]);
        } else {
            if (res.message!==undefined) {
                vscode.window.showErrorMessage(res.message);
            }
        }
    }

    async remove(uri: vscode.Uri, recursive: boolean) {
        if (
            this.socket
            && this.safeSyncCapability.writable
            && !this.remoteRelativePath(uri).startsWith(REMOTE_SAFE_SYNC_DIRECTORY)
        ) {
            await this.removeWithRecoverySafety(uri, recursive);
            return;
        }
        await this.removeDirectly(uri, recursive);
    }

    private async removeDirectly(uri: vscode.Uri, recursive: boolean): Promise<void> {
        const {parentFolder, fileType, fileEntity} = await this._resolveUri(uri);
        if (fileType && fileEntity) {
            const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
            const res = await this.api.deleteEntity(identity, this.projectId, fileType, fileEntity._id);
            if (res.type!=='success') {
                throw new Error(
                    `Could not delete remote ${fileType} ${uri.path}: ${res.message ?? 'unknown error'}`,
                );
            }
            this.removeEntityById(parentFolder, fileType, fileEntity._id, recursive);
            this.notify([
                {type: vscode.FileChangeType.Deleted, uri: uri},
            ]);
        }
    }

    private async removeWithRecoverySafety(uri: vscode.Uri, recursive: boolean): Promise<void> {
        const target = await this._resolveUri(uri);
        if (!target.fileType || !target.fileEntity) {
            return;
        }
        if (target.fileType === 'doc' || target.fileType === 'file') {
            await this.removeCurrentLeafWithRecoverySafety(uri, target.fileType, target.fileEntity._id);
            return;
        }
        if (target.fileType !== 'folder') {
            throw new Error(`Unsupported remote entity deletion: ${uri.path}`);
        }
        const folder = target.fileEntity as FolderEntity;
        const hasChildren = folder.docs.length > 0
            || folder.fileRefs.length > 0
            || folder.folders.length > 0;
        if (hasChildren && !recursive) {
            throw new Error(`Remote folder is not empty: ${uri.path}`);
        }
        const rootPath = this.remoteRelativePath(uri);
        const descendants = this.walk(entity => (
            entity._type === 'doc' || entity._type === 'file' || entity._type === 'folder'
        ))
            .filter(({path: entityPath}) => {
                const normalized = this.normalizeRemoteRelativePath(entityPath);
                return normalized === rootPath || normalized.startsWith(`${rootPath}/`);
            })
            .sort((left, right) => (
                this.normalizeRemoteRelativePath(right.path).split('/').length
                - this.normalizeRemoteRelativePath(left.path).split('/').length
            ));
        for (const candidate of descendants) {
            const resolved = this._resolveById(candidate.entity._id);
            if (!resolved) {
                continue;
            }
            const currentPath = this.normalizeRemoteRelativePath(resolved.path);
            const currentUri = this.pathToUri(...currentPath.split('/'));
            if (resolved.fileType === 'folder') {
                await this.removeFolderAgainstBaseline(
                    currentUri,
                    resolved.fileEntity._id,
                    EMPTY_REMOTE_FOLDER_TREE_HASH,
                );
            } else if (resolved.fileType === 'doc' || resolved.fileType === 'file') {
                await this.removeCurrentLeafWithRecoverySafety(
                    currentUri,
                    resolved.fileType,
                    resolved.fileEntity._id,
                );
            } else {
                throw new Error(`Unsupported remote entity deletion: ${resolved.path}`);
            }
        }
    }

    private async removeCurrentLeafWithRecoverySafety(
        uri: vscode.Uri,
        fileType: 'doc' | 'file',
        expectedEntityId: string,
    ): Promise<void> {
        const snapshot = await this.readAuthoritativeFileWithoutRecovery(uri);
        if (snapshot.fileType !== fileType || snapshot.entityId !== expectedEntityId) {
            throw new ConcurrentRemoteResourceError(uri.path);
        }
        const expectedHash = contentHash(snapshot.content);
        if (fileType === 'doc') {
            await this.removeDocumentAgainstBaseline(uri, expectedEntityId, expectedHash);
        } else {
            await this.removeResourceAgainstBaseline(uri, expectedEntityId, expectedHash);
        }
    }

    async rename(oldUri: vscode.Uri, newUri: vscode.Uri, force: boolean) {
        const oldPath = await this._resolveUri(oldUri);
        const newPath = await this._resolveUri(newUri);

        if (oldPath.fileType && oldPath.fileEntity && oldPath.fileEntity) {
            // delete existence firstly
            if (newPath.fileType && newPath.fileEntity) {
                if (!force) { return; }
                await this.remove(newUri, true);
                this.removeEntity(newPath.parentFolder, newPath.fileType, newPath.fileEntity);
            }
            // rename or move
            let res = undefined;
            const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
            if (oldPath.parentFolder===newPath.parentFolder) {
                const [entityType, entityId, newName] = [oldPath.fileType, oldPath.fileEntity._id, newPath.fileName];
                res = await this.api.renameEntity(identity, this.projectId, entityType, entityId, newName);
            } else {
                const [entityType, entityId, newParentFolderId] = [oldPath.fileType, oldPath.fileEntity._id, newPath.parentFolder._id];
                res = await this.api.moveEntity(identity, this.projectId, entityType, entityId, newParentFolderId);
            }
            // update local cache
            if (res?.type==='success') {
                const newEntity = Object.assign(oldPath.fileEntity);
                newEntity.name = newPath.fileName;
                this.removeEntity(oldPath.parentFolder, oldPath.fileType, oldPath.fileEntity);
                this.insertEntity(newPath.parentFolder, oldPath.fileType, newEntity);
                this.notify([
                    {type: vscode.FileChangeType.Deleted, uri: oldUri},
                    {type: vscode.FileChangeType.Created, uri: newUri},
                ]);
            } else {
                if (res?.message!==undefined) {
                    vscode.window.showErrorMessage(res.message);
                }
            }
        }
    }

    async compile(force:boolean=false, draft:boolean=false, stopOnFirstError:boolean=false, rootDocId?:string) {
        if (force || (this.root && this.isDirty)) {
            this.isDirty = false;
            let needCacheClearFirst = false;
            try{
                await this.resolve(this.pathToUri(OUTPUT_FOLDER_NAME, "output.log"));
            }
            catch (e) {
                needCacheClearFirst = true;
            }
            const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
            // clear cache if needed
            if (needCacheClearFirst) {
                await this.api.deleteAuxFiles(identity, this.projectId);
            }
            // compile project
            const resolvedRootDocId = rootDocId ?? this.root?.rootDoc_id ?? null;
            let rootResourcePath: string | null = null;
            if (resolvedRootDocId) {
                const rootEntry = this._resolveById(resolvedRootDocId);
                if (rootEntry?.path) {
                    rootResourcePath = rootEntry.path.replace(/^\//, '');
                } else {
                    console.warn(`Unable to resolve root document id '${resolvedRootDocId}' to a path; compiling without explicit rootResourcePath.`);
                }
            }
            const res = await this.api.compile(identity, this.projectId, rootResourcePath, draft, stopOnFirstError);
            if (res.type==='success' && res.compile?.status==='success') {
                // Store CDN download info from the response for subsequent output file requests
                this.compileGroup = res.compile.compileGroup;
                this.clsiServerId = res.compile.clsiServerId;
                this.pdfDownloadDomain = res.compile.pdfDownloadDomain;
                this.updateOutputs(res.compile.outputFiles);
                return true;
            } else {
                if (res.message!==undefined) {
                    console.error('Compile failure.', res.message);
                }
                return false;
            }
        }
        return Promise.resolve(undefined);
    }

    async stopCompile() {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.stopCompile(identity, this.projectId);
        if (res.type==='success') {
            return true;
        } else {
            if (res.message!==undefined) {
                vscode.window.showErrorMessage(res.message);
            }
            return false;
        }
    }

    async updateOutputs(outputs: Array<OutputFileEntity>) {
        if (this.root) {
            // update output buildId
            // '/project/65dbfff719ad65b54b9eaed4/user/65094b5fa537faaba0bec01f/build/19620231e54-5372f67292889500/output/output.aux' --> 19620231e54-5372f67292889500'
            this.outputBuildId = outputs[0].url.match(/\/build\/([^\/]+)/)?.[1];

            const rootFolder = this.root.rootFolder[0];
            if (this.removeEntityById(rootFolder, 'folder', __OUTPUTS_ID)) {
                this.notify([
                    {type:vscode.FileChangeType.Deleted, uri:this.pathToUri(OUTPUT_FOLDER_NAME)}
                ]);
            }

            this.insertEntity(rootFolder, 'folder', {
                _id: __OUTPUTS_ID,
                name: OUTPUT_FOLDER_NAME,
                readonly: true,
                docs: [], fileRefs: [], folders:[],
                outputs: outputs.map((file) => {
                    file._id = __OUTPUTS_ID;
                    file.name=file.path;
                    file.readonly=true;
                    return file;
                })
            } as FolderEntity);
            this.notify([
                {type:vscode.FileChangeType.Created, uri:this.pathToUri(OUTPUT_FOLDER_NAME)},
                ...(outputs.map((file) => {
                    return {type:vscode.FileChangeType.Changed, uri:this.pathToUri(OUTPUT_FOLDER_NAME, file.path)};
                }))
            ]);
        }
    }

    async syncCode(filePath: string, line:number, column:number) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.proxySyncCode(identity, this.projectId, filePath, line, column, this.outputBuildId ?? '');
        if (res.type==='success') {
            return res.syncCode;
        } else {
            if (res.message!==undefined) {
                vscode.window.showErrorMessage(res.message);
            }
            return undefined;
        }
    }

    async syncPdf(page:number, h:number, v:number) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.proxySyncPdf(identity, this.projectId, page, h, v, this.outputBuildId ?? '');
        if (res.type==='success') {
            return res.syncPdf;
        } else {
            if (res.message!==undefined) {
                vscode.window.showErrorMessage(res.message);
            }
            return undefined;
        }
    }

    async spellCheck(uri: vscode.Uri, words: string[]) {
        if (this.root?.spellCheckLanguage==='') { return []; }

        const {fileType} = await this._resolveUri(uri);
        if (fileType==='doc' || fileType==='file') {
            const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
            const res = this.root && await this.api.proxyRequestToSpellingApi(identity, this.root.spellCheckLanguage, this.userId, words);
            if (res?.type==='success') {
                return res.misspellings;
            }
        }
    }

    async spellLearn(word: string) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.spellingControllerLearn(identity, this.userId, word);
        if (res.type==='success') {
            this.root?.settings.learnedWords.push(word);
            return true;
        } else {
            return false;
        }
    }

    async spellUnlearn(word: string) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.spellingControllerUnlearn(identity, word);
        if (res.type==='success') {
            const index = this.root?.settings.learnedWords.findIndex((w) => w===word);
            if (index!==undefined && index>=0) {
                this.root?.settings.learnedWords.splice(index, 1);
            }
            return true;
        } else {
            return false;
        }
    }

    getSpellCheckLanguage() {
        const language = this.root?.spellCheckLanguage;
        if (language==='') {
            return {name:'Off', code:''};
        } else {
            return this.root?.settings.languages.find(item => item.code===language);
        }
    }

    getAllSpellCheckLanguages() {
        return this.root?.settings.languages;
    }

    getCompiler() {
        const compiler = this.root?.compiler;
        const compilerItem = this.root?.settings.compilers.find(item => item.code===compiler);
        return compilerItem;
    }

    getAllCompilers() {
        return this.root?.settings.compilers;
    }

    getDictionary() {
        return this.root?.settings.learnedWords;
    }

    getRootDocName() {
        return this._resolveById(this.root?.rootDoc_id!)?.path ?? '';
    }

    getValidMainDocs() {
        return this.walk((entity) => {
            return entity._type==='doc' && entity.name.match(/\.tex$/g)!==null;
        });
    }

    getProjectSCMPersist(scmKey: string) {
        const scmPersists = GlobalStateManager.getServerProjectSCMPersists(this.context, this.serverName, this.projectId);
        return scmPersists[scmKey];
    }

    setProjectSCMPersist(scmKey: string, persist: any) {
        GlobalStateManager.updateServerProjectSCMPersist(this.context, this.serverName, this.projectId, scmKey, persist);
    }

    async updateSettings(setting: any) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.updateProjectSettings(identity, this.projectId, setting);
        if (res.type==='success') {
            const keys = Object.keys(setting);
            if (keys.includes('spellCheckLanguage')) {
                this.root!.spellCheckLanguage = setting.spellCheckLanguage;
            }
            if (keys.includes('compiler')) {
                this.root!.compiler = setting.compiler;
            }
            if (keys.includes('rootDocId')) {
                this.root!.rootDoc_id = setting.rootDocId;
            }
        }
        return res.type==='success'? true : false;
    }

    async metadata() {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.getMetadata(identity, this.projectId);
        if (res.type==='success') {
            return res.meta?.projectMeta;
        } else {
            return undefined;
        }
    }

    async getUpdates(before?: number) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.proxyToHistoryApiAndGetUpdates(identity, this.projectId, before);
        if (res.type==='success') {
            return res.updates;
        } else {
            return undefined;
        }
    }

    async getFileDiff(pathname:string, from:number, to:number) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.proxyToHistoryApiAndGetFileDiff(identity, this.projectId, pathname, from, to);
        if (res.type==='success') {
            return res.diff;
        } else {
            return undefined;
        }
    }

    async getFileTreeDiff(from:number, to:number) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.proxyToHistoryApiAndGetFileTreeDiff(identity, this.projectId, from, to);
        if (res.type==='success') {
            return res.treeDiff;
        } else {
            return undefined;
        }
    }

    async getCurrentVersion() {
        const base = this.currentVersion ?? 0;
        let lb = base;
        let rb = base+2**4;
        // firstly try: a) no update `+1`, b) one update `+2`
        const res = await this.getFileTreeDiff(base+1, base+1);
        if (res===undefined) {
            this.currentVersion = base;
            return base;
        }
        const res2 = await this.getFileTreeDiff(base+2, base+2);
        if (res2===undefined) {
            this.currentVersion = base+1;
            return this.currentVersion;
        }
        // locate the actual upper bound
        do {
            const res = await this.getFileTreeDiff(rb, rb);
            if (res!==undefined) {
                rb = lb + (rb-lb)*2;
            } else {
                break;
            }
        } while (true);
        // binary search the current version
        while (lb<rb) {
            const mid = Math.floor((lb+rb)/2);
            const res = await this.getFileTreeDiff(mid, mid);
            if (res!==undefined) {
                lb = mid+1;
            } else {
                rb = mid;
            }
        }
        // update current version
        this.currentVersion = rb-1;
        return this.currentVersion;
    }

    async createLabel(comment: string, version: number) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.createLabel(identity, this.projectId, comment, version);
        if (res.type==='success') {
            return res.labels?.at(0);
        } else {
            return undefined;
        }
    }

    async deleteLabel(labelId: string) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.deleteLabel(identity, this.projectId, labelId);
        if (res.type==='success') {
            return true;
        } else {
            return false;
        }
    }

    async downloadProjectArchive(version: number) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.downloadZipOfVersion(identity, this.projectId, version);
        return res.content;
    }

    async getMessages() {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.getMessages(identity, this.projectId);
        if (res.type==='success') {
            return res.messages;
        } else {
            return undefined;
        }
    }

    async sendMessage(publicId:string, content: string) {
        const identity = await GlobalStateManager.authenticate(this.context, this.serverName);
        const res = await this.api.sendMessage(identity, this.projectId, publicId, content);
        if (res.type==='success') {
            return true;
        } else {
            return false;
        }
    }
}

export class RemoteFileSystemProvider implements vscode.FileSystemProvider {
    private _emitter = new vscode.EventEmitter<vscode.FileChangeEvent[]>();
    readonly onDidChangeFile: vscode.Event<vscode.FileChangeEvent[]> = this._emitter.event;

    private vfss: {[key:string]:VirtualFileSystem};

    constructor(private context: vscode.ExtensionContext) {
        this.context = context;
        this.vfss = {};
    }

    private getVFS(uri: vscode.Uri): Promise<VirtualFileSystem> {
        const vfs = this.vfss[ uri.query ];
        if (vfs) {
            return Promise.resolve(vfs);
        } else {
            const vfs = new VirtualFileSystem(this.context, uri, this.notify.bind(this));
            this.vfss[ uri.query ] = vfs;
            return Promise.resolve(vfs);
        }
    }

    prefetch(uri: vscode.Uri): Promise<VirtualFileSystem> {
        return this.getVFS(uri).then((vfs) => {return vfs;});
    }

    notify(events :vscode.FileChangeEvent[]) {
        this._emitter.fire(events);
    }

    stat(uri: vscode.Uri): Thenable<vscode.FileStat> {
        return this.getVFS(uri).then( vfs => vfs.resolve(uri) );
    }

    watch(uri: vscode.Uri, options: { recursive: boolean; excludes: string[]; }): vscode.Disposable {
        return new vscode.Disposable(() => {});
    }

    readDirectory(uri: vscode.Uri): Thenable<[string, vscode.FileType][]> {
        return this.getVFS(uri).then( vfs => vfs.list(uri) );
    }

    createDirectory(uri: vscode.Uri): Thenable<void> {
        return this.getVFS(uri).then( vfs => vfs.mkdir(uri) );
    }

    readFile(uri: vscode.Uri): Thenable<Uint8Array> {
        return this.getVFS(uri).then( vfs => vfs.openFile(uri) );
    }

    writeFile(uri: vscode.Uri, content: Uint8Array, options: { create: boolean; overwrite: boolean; }): Thenable<void> {
        return this.getVFS(uri).then( vfs => vfs.writeFile(uri, content, options.create, options.overwrite) );
    }

    delete(uri: vscode.Uri, options: { recursive: boolean; }): Thenable<void> {
        return this.getVFS(uri).then( vfs => vfs.remove(uri, options.recursive) );
    }

    rename(oldUri: vscode.Uri, newUri: vscode.Uri, options: { overwrite: boolean; }) {
        if (oldUri.authority !== newUri.authority) {
            vscode.window.showErrorMessage( vscode.l10n.t('Cannot rename across servers') );
            return;
        } else {
            return this.getVFS(oldUri).then( vfs => vfs.rename(oldUri, newUri, options.overwrite) );
        }
    }

    get triggers() {
        return [
            // register file system provider
            vscode.workspace.registerFileSystemProvider(URI_SCHEME, this, { isCaseSensitive: true }),
            // register commands
            vscode.commands.registerCommand(`${ROOT_NAME}.remoteFileSystem.refreshLinkedFile`, (uri: vscode.Uri) => {
                return this.prefetch(uri).then((vfs) => vfs.refreshLinkedFile(uri));
            }),
            vscode.commands.registerCommand(`${ROOT_NAME}.remoteFileSystem.createLinkedFile`, (uri?: vscode.Uri) => {
                uri = uri || vscode.workspace.workspaceFolders?.[0].uri;
                if (uri) {
                    return this.prefetch(uri).then((vfs) => vfs.createLinkedFile(uri!));
                }                
            }),
            vscode.commands.registerCommand(`${ROOT_NAME}.remoteFileSystem.prefetch`, (uri: vscode.Uri) => {
                return this.prefetch(uri);
            }),
        ];
    }
}
