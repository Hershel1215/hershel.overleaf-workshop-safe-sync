import * as vscode from 'vscode';
import { createHash, randomUUID } from 'crypto';
import { readFileSync } from 'fs';
import { rm as removeDisk, writeFile as writeDiskFile } from 'fs/promises';
import { BaseAPI, Identity, ProjectPersist } from '../api/base';
import { isSocketIOTimeout, SocketIOAPI, UpdateSchema } from '../api/socketio';
import { FolderEntity, ProjectEntity } from '../core/remoteFileSystemProvider';
import { ROOT_NAME } from '../consts';
import { GlobalStateManager } from '../utils/globalStateManager';
import { LocalReplicaSCMProvider } from '../scm/localReplicaSCM';
import { DurableDocumentSession } from './durableDocumentSession';
import { DocumentJournal } from './documentJournal';
import { sendAiGatewayRequest } from './aiGateway';
import { assertLocalReplicaBinding, LocalReplicaBinding } from './localBinding';
import { ReplicaStateStore } from './replicaStateStore';
import { TextOperation } from './textOperation';

const APPROVED_SERVER = '10.10.5.4:8888';
const APPROVED_PROJECT = 'hershel-safe-sync-e2e';
const APPROVED_PROJECT_ID = '6a6b673cee2d179ed3303782';
const REQUEST_FILE = 'safe-sync-e2e.request.json';
const RUNNING_FILE = 'safe-sync-e2e.running.json';
const RESULT_FILE = 'safe-sync-e2e.result.json';
const RELOAD_FILE = 'safe-sync-e2e.reload.json';
const HANDOFF_TARGET_VERSION = '1.0.0-hershel.60';
const HANDOFF_TARGET_BUILD_ID = '20260802T045315Z-safe-sync-v2-rc60';
export const E2E_RESULT_SCHEMA_VERSION = 5;
const BINARY_FIXTURE = Uint8Array.from(Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
    'base64',
));

interface E2ERequest {
    schemaVersion: 2;
    serverName: typeof APPROVED_SERVER;
    projectName: typeof APPROVED_PROJECT;
    nonce: string;
    expectedExtensionId: string;
    expectedExtensionVersion: string;
    expectedBuildId: string;
    expectedRunnerSha256: string;
}

interface E2ERunningMarker {
    schemaVersion: 1;
    request: E2ERequest;
    ownerPid: number;
    startedAt: string;
}

interface E2EReloadMarker {
    schemaVersion: 1;
    targetExtensionVersion: typeof HANDOFF_TARGET_VERSION;
    targetBuildId: typeof HANDOFF_TARGET_BUILD_ID;
}

interface E2EStep {
    name: string;
    ok: boolean;
    durationMs: number;
    detail?: string;
}

interface E2EEvidence {
    fixtureNamespace?: string;
    binaryReadback?: {entityType: 'file'; sha256: string};
    localTextRoundTrip?: {
        entityType: 'doc';
        createdSha256: string;
        remotelyEditedSha256: string;
        lateAttachRebased: boolean;
        remoteDeleteVerified: boolean;
    };
    localResources?: Array<{name: string; entityType: 'file'; sha256: string}>;
    binaryConflict?: {
        localSha256: string;
        remoteSha256: string;
        archivesVerified: boolean;
        resolvedUsing: 'remote';
    };
    textResourceConflictRecovery?: {
        resolvedUsing: 'remote';
        reattached: boolean;
        sha256: string;
    };
    localWriterOverlap?: {sha256: string; journalConverged: boolean};
    streamingWriters?: {
        path: string;
        batchedLocalEditCount: number;
        batchedLocalOperationCount: number;
        localEditCount: number;
        localSubmittedOperationCount: number;
        editorChangeEventCount: number;
        remoteSubmittedOperationCount: number;
        aiCommittedOperationCount: number;
        finalRevision: number;
        expectedUtf16Length: number;
        perWriterOrderPreserved: boolean;
        sha256: string;
        savedToDisk: boolean;
    };
    conflictArtifactsRemoteAbsent?: boolean;
    otCatchUp?: {baseRevision: number; finalRevision: number; operationCount: number; sha256: string};
    cleanup?: {
        remoteAbsent: boolean;
        localAbsent: boolean;
        pendingAction: boolean;
        activeConflicts: number;
        remainingFixtureEntries: number;
        remainingFixtureArtifacts: number;
    };
}

interface E2EResult {
    schemaVersion: typeof E2E_RESULT_SCHEMA_VERSION;
    status: 'passed' | 'failed';
    serverName: string;
    projectName: string;
    nonce: string;
    extensionId: string;
    extensionVersion: string;
    buildId: string;
    extensionPath: string;
    runnerSha256: string;
    projectId?: string;
    startedAt: string;
    finishedAt: string;
    steps: E2EStep[];
    evidence: E2EEvidence;
    error?: string;
}

interface ActiveRun {
    nonce: string;
    promise: Promise<E2EResult>;
}

let running: ActiveRun | undefined;

function isE2EConflictPath(filePath: string, namespace?: string): boolean {
    if (namespace !== undefined) {
        return filePath === namespace || filePath.startsWith(`${namespace}/`);
    }
    return /^hershel-e2e-[a-zA-Z0-9_-]+(?:\/|$)/.test(filePath);
}

export function safeMessage(error: unknown): string {
    const raw = error instanceof Error
        ? error.message
        : typeof error === 'string'
            ? error
            : (() => {
                try {
                    return JSON.stringify(error);
                } catch {
                    return String(error);
                }
            })();
    return raw
        .replace(/("(?:cookie|csrf(?:[-_]?token)?|access[-_]?token|token|authorization)"\s*:\s*)"[^"]*"/gi,
            '$1"[redacted]"')
        .replace(/((?:authorization)\s*:\s*)(?:bearer|basic)\s+[^\s,}]+/gi, '$1[redacted]')
        .replace(/((?:cookie)\s*:\s*)[^\r\n]+/gi, '$1[redacted]')
        .replace(/((?:cookie|csrf(?:[-_]?token)?|access[-_]?token|token)\s*=\s*)[^\s,;}]+/gi,
            '$1[redacted]')
        .slice(0, 2000);
}

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) {
        throw new Error(message);
    }
}

export function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
    return new Promise<T>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
        promise.then(
            value => {
                clearTimeout(timer);
                resolve(value);
            },
            error => {
                clearTimeout(timer);
                reject(error);
            },
        );
    });
}

async function waitForAsync(
    predicate: () => Promise<boolean>,
    timeoutMs: number,
    label: string,
    intervalMs = 500,
): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!await predicate()) {
        if (Date.now() >= deadline) {
            throw new Error(`${label} timed out`);
        }
        await new Promise(resolve => setTimeout(resolve, intervalMs));
    }
}

async function waitFor(
    predicate: () => boolean,
    timeoutMs: number,
    label: string,
): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
        if (Date.now() >= deadline) {
            throw new Error(`${label} timed out`);
        }
        await new Promise(resolve => setTimeout(resolve, 25));
    }
}

async function joinProjectWithProtocolFallback(
    socket: SocketIOAPI,
    projectId: string,
    label: string,
): Promise<ProjectEntity> {
    try {
        return await withTimeout(socket.joinProject(projectId), 10000, label);
    } catch (error) {
        if (!socket.needsReinit) {
            throw error;
        }
        // Match VirtualFileSystem's production path: a legacy join rejection
        // changes v1 -> v2, then the socket must be rebuilt before retrying.
        socket.init();
        return withTimeout(socket.joinProject(projectId), 10000, `${label} after protocol fallback`);
    }
}

function findEntity(
    folder: FolderEntity,
    entityId: string,
): {entity: { _id: string; name: string }; parentId: string} | undefined {
    for (const entity of [...folder.docs, ...folder.fileRefs]) {
        if (entity._id === entityId) {
            return {entity, parentId: folder._id};
        }
    }
    for (const child of folder.folders) {
        if (child._id === entityId) {
            return {entity: child, parentId: folder._id};
        }
        const nested = findEntity(child, entityId);
        if (nested) {
            return nested;
        }
    }
    return undefined;
}

function findFolder(folder: FolderEntity, folderId: string): FolderEntity | undefined {
    if (folder._id === folderId) {
        return folder;
    }
    for (const child of folder.folders) {
        const nested = findFolder(child, folderId);
        if (nested) {
            return nested;
        }
    }
    return undefined;
}

async function freshProject(
    serverUrl: string,
    api: BaseAPI,
    identity: Identity,
    projectId: string,
    label: string,
): Promise<ProjectEntity> {
    const socket = new SocketIOAPI(serverUrl, api, identity, projectId);
    try {
        return await joinProjectWithProtocolFallback(socket, projectId, label);
    } finally {
        socket.disconnect();
    }
}

async function freshDocument(
    serverUrl: string,
    api: BaseAPI,
    identity: Identity,
    projectId: string,
    parentFolderId: string,
    name: string,
): Promise<{entityId: string; content: string} | undefined> {
    const socket = new SocketIOAPI(serverUrl, api, identity, projectId);
    try {
        const project = await joinProjectWithProtocolFallback(socket, projectId, `locate ${name}`);
        const parent = findFolder(project.rootFolder[0], parentFolderId);
        const document = parent?.docs.find(item => item.name === name);
        if (!document) {
            return undefined;
        }
        try {
            const joined = await withTimeout(socket.joinDoc(document._id), 10000, `read ${name}`);
            return {entityId: document._id, content: joined.docLines.join('\n')};
        } catch (error) {
            const current = await freshProject(
                serverUrl,
                api,
                identity,
                projectId,
                `verify failed read of ${name}`,
            );
            const currentParent = findFolder(current.rootFolder[0], parentFolderId);
            if (!currentParent?.docs.some(item => item._id === document._id)) {
                return undefined;
            }
            throw error;
        }
    } catch (error) {
        // Never report absence from a timed-out document read. A second fresh
        // project snapshot must prove that the entity itself is gone.
        if (isSocketIOTimeout(error)) {
            const current = await freshProject(
                serverUrl,
                api,
                identity,
                projectId,
                `confirm timed-out read of ${name}`,
            );
            const currentParent = findFolder(current.rootFolder[0], parentFolderId);
            if (!currentParent?.docs.some(item => item.name === name)) {
                return undefined;
            }
        }
        throw error;
    } finally {
        socket.disconnect();
    }
}

async function readLocalBytes(uri: vscode.Uri): Promise<Uint8Array | undefined> {
    try {
        return await vscode.workspace.fs.readFile(uri);
    } catch (error) {
        if ((error as vscode.FileSystemError).code === 'FileNotFound') {
            return undefined;
        }
        throw error;
    }
}

async function findE2EConflictArtifactDirectories(
    localRoot: vscode.Uri,
    namespace?: string,
): Promise<vscode.Uri[]> {
    const result: vscode.Uri[] = [];
    for (const status of ['unresolved', 'resolved']) {
        const statusRoot = vscode.Uri.joinPath(localRoot, '.overleaf', 'conflicts', status);
        let artifacts: [string, vscode.FileType][];
        try {
            artifacts = await vscode.workspace.fs.readDirectory(statusRoot);
        } catch (error) {
            if ((error as vscode.FileSystemError).code === 'FileNotFound') {
                continue;
            }
            throw error;
        }
        for (const [artifactName, artifactType] of artifacts) {
            if ((artifactType & vscode.FileType.Directory) === 0) {
                continue;
            }
            const artifact = vscode.Uri.joinPath(statusRoot, artifactName);
            const queue = [artifact];
            let matches = false;
            while (queue.length > 0 && !matches) {
                const current = queue.shift()!;
                for (const [name, type] of await vscode.workspace.fs.readDirectory(current)) {
                    if (namespace ? name === namespace : name.startsWith('hershel-e2e-')) {
                        matches = true;
                        break;
                    }
                    if (
                        (type & vscode.FileType.Directory) !== 0
                        && (type & vscode.FileType.SymbolicLink) === 0
                    ) {
                        queue.push(vscode.Uri.joinPath(current, name));
                    }
                }
            }
            if (matches) {
                result.push(artifact);
            }
        }
    }
    return result;
}

async function cleanupE2EConflictArtifacts(
    localRoot: vscode.Uri,
    namespace?: string,
): Promise<void> {
    const artifacts = await findE2EConflictArtifactDirectories(localRoot, namespace);
    for (const artifact of artifacts) {
        await vscode.workspace.fs.delete(artifact, {recursive: true, useTrash: false});
    }
}

async function localPathExists(uri: vscode.Uri): Promise<boolean> {
    try {
        await vscode.workspace.fs.stat(uri);
        return true;
    } catch (error) {
        if ((error as vscode.FileSystemError).code === 'FileNotFound') {
            return false;
        }
        throw error;
    }
}

async function readJson<T>(uri: vscode.Uri): Promise<T | undefined> {
    try {
        const bytes = await vscode.workspace.fs.readFile(uri);
        return JSON.parse(Buffer.from(bytes).toString('utf8')) as T;
    } catch (error) {
        if ((error as vscode.FileSystemError).code === 'FileNotFound') {
            return undefined;
        }
        throw error;
    }
}

async function writeJsonAtomic(uri: vscode.Uri, value: unknown): Promise<void> {
    await vscode.workspace.fs.createDirectory(vscode.Uri.joinPath(uri, '..'));
    const temporary = uri.with({path: `${uri.path}.${process.pid}.${randomUUID()}.tmp`});
    try {
        await vscode.workspace.fs.writeFile(
            temporary,
            Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8'),
        );
        await vscode.workspace.fs.rename(temporary, uri, {overwrite: true});
    } finally {
        try {
            await vscode.workspace.fs.delete(temporary);
        } catch {
            // Rename already removed the temporary file in the normal path.
        }
    }
}

export function validateRequest(value: unknown): E2ERequest {
    const request = value as Partial<E2ERequest>;
    assert(request?.schemaVersion === 2, 'Unsupported E2E request schema');
    assert(request.serverName === APPROVED_SERVER, 'E2E request targets an unapproved server');
    assert(request.projectName === APPROVED_PROJECT, 'E2E request targets an unapproved project');
    assert(
        typeof request.nonce === 'string' && /^[a-zA-Z0-9_-]{8,80}$/.test(request.nonce),
        'Invalid E2E request nonce',
    );
    assert(request.expectedExtensionId === ROOT_NAME, 'E2E request expects another extension id');
    assert(
        typeof request.expectedExtensionVersion === 'string'
        && /^1\.0\.0-hershel\.\d+$/.test(request.expectedExtensionVersion),
        'Invalid expected extension version',
    );
    assert(
        typeof request.expectedBuildId === 'string'
        && /^[a-zA-Z0-9._-]{8,100}$/.test(request.expectedBuildId),
        'Invalid expected build id',
    );
    assert(
        typeof request.expectedRunnerSha256 === 'string'
        && /^[a-f0-9]{64}$/.test(request.expectedRunnerSha256),
        'Invalid expected runner digest',
    );
    return request as E2ERequest;
}

export function validateRunningMarker(value: unknown): E2ERunningMarker {
    const marker = value as Partial<E2ERunningMarker>;
    assert(marker?.schemaVersion === 1, 'Unsupported running-marker schema');
    assert(Number.isSafeInteger(marker.ownerPid) && marker.ownerPid! > 0,
        'Invalid running-marker owner pid');
    assert(typeof marker.startedAt === 'string' && !Number.isNaN(Date.parse(marker.startedAt)),
        'Invalid running-marker start time');
    return {
        schemaVersion: 1,
        request: validateRequest(marker.request),
        ownerPid: marker.ownerPid!,
        startedAt: marker.startedAt,
    };
}

export function isProcessAlive(pid: number): boolean {
    if (!Number.isSafeInteger(pid) || pid <= 0) {
        return false;
    }
    try {
        process.kill(pid, 0);
        return true;
    } catch (error) {
        return (error as NodeJS.ErrnoException).code === 'EPERM';
    }
}

export function buildE2EIdentity(context: vscode.ExtensionContext): Pick<
    E2EResult,
    'extensionId' | 'extensionVersion' | 'buildId' | 'extensionPath' | 'runnerSha256'
> {
    return {
        extensionId: context.extension.id,
        extensionVersion: String(context.extension.packageJSON.version ?? 'unknown'),
        buildId: String(context.extension.packageJSON.hershelBuildId ?? 'missing-hershel-build-id'),
        extensionPath: context.extension.extensionPath,
        runnerSha256: createHash('sha256').update(readFileSync(__filename)).digest('hex'),
    };
}

export function assertExpectedBuild(
    request: E2ERequest,
    identity: ReturnType<typeof buildE2EIdentity>,
): void {
    assert(request.expectedExtensionId === identity.extensionId,
        'Loaded extension id does not match the approved E2E request');
    assert(request.expectedExtensionVersion === identity.extensionVersion,
        'Loaded extension version does not match the approved E2E request');
    assert(request.expectedBuildId === identity.buildId,
        'Loaded extension build id does not match the approved E2E request');
    assert(request.expectedRunnerSha256 === identity.runnerSha256,
        'Loaded E2E runner digest does not match the approved E2E request');
}

async function workspaceTargetsApprovedReplica(): Promise<boolean> {
    const folders = vscode.workspace.workspaceFolders;
    if (folders?.length !== 1 || folders[0].uri.scheme !== 'file') {
        return false;
    }
    try {
        const raw = await vscode.workspace.fs.readFile(
            vscode.Uri.joinPath(folders[0].uri, '.overleaf/settings.json'),
        );
        const binding = JSON.parse(Buffer.from(raw).toString('utf8')) as Partial<LocalReplicaBinding>;
        const uri = typeof binding.uri === 'string' ? new URL(binding.uri) : undefined;
        return binding.serverName === APPROVED_SERVER
            && binding.projectName === APPROVED_PROJECT
            && uri?.protocol === 'overleaf-workshop:'
            && uri.host === APPROVED_SERVER;
    } catch {
        return false;
    }
}

async function consumeApprovedReloadHandoff(
    context: vscode.ExtensionContext,
    output: vscode.OutputChannel,
): Promise<boolean> {
    const markerUri = vscode.Uri.joinPath(context.globalStorageUri, RELOAD_FILE);
    const value = await readJson<unknown>(markerUri);
    if (value === undefined) {
        return false;
    }
    const marker = value as Partial<E2EReloadMarker>;
    if (
        marker.schemaVersion !== 1
        || marker.targetExtensionVersion !== HANDOFF_TARGET_VERSION
        || marker.targetBuildId !== HANDOFF_TARGET_BUILD_ID
        || !await workspaceTargetsApprovedReplica()
    ) {
        output.appendLine('[warn] Refused an invalid or out-of-scope E2E reload handoff.');
        return false;
    }
    const root = vscode.workspace.workspaceFolders![0].uri;
    const raw = await vscode.workspace.fs.readFile(vscode.Uri.joinPath(root, '.overleaf/settings.json'));
    const binding = JSON.parse(Buffer.from(raw).toString('utf8')) as LocalReplicaBinding;
    assertLocalReplicaBinding(binding, APPROVED_SERVER, APPROVED_PROJECT_ID);
    await vscode.workspace.fs.delete(markerUri);
    output.appendLine(
        `Reloading the approved E2E workspace to activate ${HANDOFF_TARGET_VERSION}`
        + ` (${HANDOFF_TARGET_BUILD_ID}).`,
    );
    await vscode.commands.executeCommand('workbench.action.reloadWindow');
    return true;
}

async function approvedLocalReplica(
    context: vscode.ExtensionContext,
    projectId: string,
): Promise<{root: vscode.Uri; stateStore: ReplicaStateStore}> {
    const folders = vscode.workspace.workspaceFolders;
    assert(folders?.length === 1 && folders[0].uri.scheme === 'file',
        'Run the full E2E from the single-folder local replica workspace');
    const root = folders[0].uri;
    const raw = await vscode.workspace.fs.readFile(vscode.Uri.joinPath(root, '.overleaf/settings.json'));
    const binding = JSON.parse(Buffer.from(raw).toString('utf8')) as LocalReplicaBinding;
    assertLocalReplicaBinding(binding, APPROVED_SERVER, projectId);
    assert(binding.projectName === APPROVED_PROJECT,
        'Local replica project name does not match the approved E2E project');
    const stateStore = new ReplicaStateStore(
        context.globalStorageUri.fsPath,
        APPROVED_SERVER,
        projectId,
        root.fsPath,
    );
    await waitForAsync(async () => {
        const runtime = LocalReplicaSCMProvider.getRuntimeStatus(root.fsPath);
        if (runtime?.paused) {
            throw new Error('The approved Local Replica provider is paused');
        }
        return runtime?.settled === true
            && runtime.pendingAction === false;
    }, 60000, 'wait for the Local Replica watcher, lock, and settled reconciliation queue');
    let state = await stateStore.reload();
    const staleConflictPaths = Object.keys(state.conflicts).filter(filePath => (
        isE2EConflictPath(filePath)
    ));
    for (const filePath of staleConflictPaths) {
        await LocalReplicaSCMProvider.resolveE2EConflictUsingRemote(root.fsPath, filePath);
    }
    await waitForAsync(async () => (
        LocalReplicaSCMProvider.getRuntimeStatus(root.fsPath)?.settled === true
    ), 60000, 'settle the approved Local Replica after E2E conflict recovery');
    const runtime = LocalReplicaSCMProvider.getRuntimeStatus(root.fsPath);
    assert(runtime && !runtime.paused, 'The approved Local Replica provider is paused');
    assert(runtime.initialized, 'The active Local Replica baseline is not initialized');
    assert(!runtime.pendingAction, 'The active Local Replica has a pending action');
    state = await stateStore.reload();
    assert(state.initialized, 'The approved local replica baseline is not initialized');
    assert(!state.pendingAction, 'The approved local replica has a pending action');
    assert(
        !Object.keys(state.conflicts).some(filePath => isE2EConflictPath(filePath)),
        'The approved local replica has stale E2E conflicts',
    );
    return {root, stateStore};
}

async function selectProject(
    context: vscode.ExtensionContext,
    api: BaseAPI,
    identity: Identity,
): Promise<{projectId: string; project: ProjectPersist | undefined}> {
    const projects = await GlobalStateManager.fetchServerProjects(context, api, APPROVED_SERVER);
    let project = projects.find(item => item.name === APPROVED_PROJECT && !item.trashed);
    if (!project) {
        const trashed = projects.find(item => item.name === APPROVED_PROJECT && item.trashed);
        if (trashed) {
            const restored = await api.untrashProject(identity, trashed.id);
            assert(restored.type === 'success', `Could not restore E2E project: ${restored.message ?? 'unknown error'}`);
            project = {...trashed, trashed: false};
        }
    }
    if (project) {
        return {projectId: project.id, project};
    }
    const created = await api.newProject(identity, APPROVED_PROJECT, 'none');
    assert(created.type === 'success' && created.message, `Could not create E2E project: ${created.message ?? 'unknown error'}`);
    return {projectId: created.message, project: undefined};
}

async function runRemoteE2E(
    context: vscode.ExtensionContext,
    request: E2ERequest,
    output: vscode.OutputChannel,
): Promise<E2EResult> {
    const startedAt = new Date().toISOString();
    const extension = buildE2EIdentity(context);
    const steps: E2EStep[] = [];
    const evidence: E2EEvidence = {};
    let projectId: string | undefined;
    let runFolderId: string | undefined;
    let api: BaseAPI | undefined;
    let identity: Identity | undefined;
    let serverUrl: string | undefined;
    let localRoot: vscode.Uri | undefined;
    let replicaStateStore: ReplicaStateStore | undefined;
    let runName: string | undefined;
    const sockets: SocketIOAPI[] = [];
    const sessions: DurableDocumentSession[] = [];
    const e2eJournalDirectory = vscode.Uri.joinPath(
        context.globalStorageUri,
        'safe-sync-e2e-journals',
        request.nonce,
    ).fsPath;

    const step = async <T>(
        name: string,
        action: () => Promise<T>,
    ): Promise<T> => {
        const started = Date.now();
        try {
            // Composite mutation steps must settle before cleanup begins. Leaf
            // socket reads and bounded polling carry their own timeouts.
            const value = await action();
            const entry = {name, ok: true, durationMs: Date.now() - started};
            steps.push(entry);
            output.appendLine(`[pass] ${name} (${entry.durationMs} ms)`);
            return value;
        } catch (error) {
            const entry = {
                name,
                ok: false,
                durationMs: Date.now() - started,
                detail: safeMessage(error),
            };
            steps.push(entry);
            output.appendLine(`[fail] ${name}: ${entry.detail}`);
            throw error;
        }
    };

    const cleanupFixtures = async (
        recordEvidence = true,
        namespace = runName,
    ): Promise<void> => {
        assert(api && identity && projectId && serverUrl && namespace && localRoot,
            'Fixture cleanup was requested before fixture creation');
        await LocalReplicaSCMProvider.cleanupNamespace(localRoot.fsPath, namespace);
        await waitForAsync(async () => {
            const current = await freshProject(
                serverUrl!,
                api!,
                identity!,
                projectId!,
                'verify E2E fixture cleanup',
            );
            return !current.rootFolder[0].folders.some(folder => folder.name === namespace);
        }, 30000, 'verify remote fixture cleanup', 750);
        if (localRoot) {
            const localFolder = vscode.Uri.joinPath(localRoot, namespace);
            await waitForAsync(
                async () => !await localPathExists(localFolder),
                30000,
                'verify local fixture cleanup',
            );
        }
        let cleanupPendingAction = false;
        let cleanupConflictCount = 0;
        let cleanupEntryCount = 0;
        if (replicaStateStore) {
            await waitForAsync(async () => {
                const state = await replicaStateStore!.reload();
                cleanupPendingAction = state.pendingAction !== undefined;
                cleanupConflictCount = Object.keys(state.conflicts)
                    .filter(filePath => isE2EConflictPath(filePath, namespace)).length;
                cleanupEntryCount = Object.keys(state.entries)
                    .filter(path => path === namespace || path.startsWith(`${namespace}/`)).length;
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return !cleanupPendingAction
                    && cleanupConflictCount === 0
                    && cleanupEntryCount === 0
                    && runtime?.settled === true;
            }, 60000, 'verify clean replica state after fixture cleanup');
        }
        await cleanupE2EConflictArtifacts(localRoot, namespace);
        const remainingFixtureArtifacts = (
            await findE2EConflictArtifactDirectories(localRoot, namespace)
        ).length;
        assert(remainingFixtureArtifacts === 0,
            `E2E conflict artifacts remain after cleanup: ${namespace}`);
        if (recordEvidence) {
            evidence.cleanup = {
                remoteAbsent: true,
                localAbsent: localRoot !== undefined,
                pendingAction: cleanupPendingAction,
                activeConflicts: cleanupConflictCount,
                remainingFixtureEntries: cleanupEntryCount,
                remainingFixtureArtifacts,
            };
        }
        if (namespace === runName) {
            runFolderId = undefined;
        }
    };

    try {
        await step('exact loaded extension build identity', async () => {
            assertExpectedBuild(request, extension);
        });
        const server = await step('approved target and SecretStorage login', async () => {
            const match = GlobalStateManager.getServers(context)
                .find(item => item.server.name === APPROVED_SERVER);
            assert(match, `Server ${APPROVED_SERVER} is not configured`);
            const url = new URL(match.server.url);
            assert(url.hostname === '10.10.5.4' && url.port === '8888', 'Configured server URL is outside the approved target');
            identity = await GlobalStateManager.authenticate(context, APPROVED_SERVER);
            api = match.api;
            serverUrl = match.server.url;
            return match;
        });

        await step('create or reuse retained E2E project', async () => {
            const selected = await selectProject(context, api!, identity!);
            projectId = selected.projectId;
            assert(/^[a-zA-Z0-9_-]+$/.test(projectId), 'Server returned an invalid project id');
            assert(projectId === APPROVED_PROJECT_ID,
                'The retained E2E project id differs from the approved target');
        });

        await step('approved Local Replica binding and clean baseline', async () => {
            const approved = await approvedLocalReplica(context, projectId!);
            localRoot = approved.root;
            replicaStateStore = approved.stateStore;
        });

        let runFolder!: FolderEntity;
        let documentId!: string;
        let streamingDocumentId!: string;
        let binaryId!: string;
        let folderA!: FolderEntity;
        let folderB!: FolderEntity;
        let movedDocumentId!: string;
        const isolatedRunName = `hershel-e2e-${request.nonce}`;
        runName = isolatedRunName;
        evidence.fixtureNamespace = isolatedRunName;

        await step('remove stale E2E namespaces from interrupted runs', async () => {
            const remote = await freshProject(
                server.server.url,
                api!,
                identity!,
                projectId!,
                'locate stale E2E namespaces',
            );
            const localEntries = await vscode.workspace.fs.readDirectory(localRoot!);
            const staleNames = new Set([
                ...remote.rootFolder[0].folders.map(folder => folder.name),
                ...localEntries
                    .filter(([, type]) => (type & vscode.FileType.Directory) !== 0)
                    .map(([name]) => name),
            ].filter(name => name.startsWith('hershel-e2e-') && name !== isolatedRunName));
            for (const staleName of staleNames) {
                await cleanupFixtures(false, staleName);
            }
            await cleanupE2EConflictArtifacts(localRoot!);
            assert((await findE2EConflictArtifactDirectories(localRoot!)).length === 0,
                'Stale E2E conflict artifacts remain after cleanup');
        });

        await step('remove stale fixture namespace for this nonce', async () => {
            await cleanupFixtures(false);
        });

        await step('create isolated text, binary, and move fixtures', async () => {
            const bootstrap = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(bootstrap);
            const project = await joinProjectWithProtocolFallback(
                bootstrap,
                projectId!,
                'join E2E project',
            );
            assert(project.name === APPROVED_PROJECT, 'Joined project name does not match the approved target');
            const root = project.rootFolder[0];
            assert(root?._id, 'E2E project has no root folder');

            const folderResponse = await api!.addFolder(identity!, projectId!, isolatedRunName, root._id);
            assert(folderResponse.type === 'success' && folderResponse.entity, 'Could not create E2E run folder');
            runFolder = folderResponse.entity as FolderEntity;
            runFolderId = runFolder._id;

            const document = await api!.addDoc(identity!, projectId!, runFolder._id, 'concurrent.tex');
            assert(document.type === 'success' && document.entity, 'Could not create E2E document');
            documentId = document.entity._id;

            const streaming = await api!.addDoc(
                identity!, projectId!, runFolder._id, 'streaming.tex',
            );
            assert(
                streaming.type === 'success' && streaming.entity,
                'Could not create streaming editor document',
            );
            streamingDocumentId = streaming.entity._id;

            const uploaded = await api!.uploadFile(
                identity!,
                projectId!,
                runFolder._id,
                'fixture.png',
                BINARY_FIXTURE,
            );
            assert(
                uploaded.type === 'success' && uploaded.entity,
                `Could not upload E2E binary fixture: ${uploaded.message ?? uploaded.type}`,
            );
            binaryId = uploaded.entity._id;

            const createdA = await api!.addFolder(identity!, projectId!, 'move-a', runFolder._id);
            const createdB = await api!.addFolder(identity!, projectId!, 'move-b', runFolder._id);
            assert(createdA.type === 'success' && createdA.entity, 'Could not create move source folder');
            assert(createdB.type === 'success' && createdB.entity, 'Could not create move target folder');
            folderA = createdA.entity as FolderEntity;
            folderB = createdB.entity as FolderEntity;
            const moved = await api!.addDoc(identity!, projectId!, folderA._id, 'before-move.tex');
            assert(moved.type === 'success' && moved.entity, 'Could not create move fixture');
            movedDocumentId = moved.entity._id;
        });

        await step('remote text and binary fixtures reach Local Replica', async () => {
            const localDocument = vscode.Uri.joinPath(localRoot!, isolatedRunName, 'concurrent.tex');
            const localStreamingDocument = vscode.Uri.joinPath(
                localRoot!, isolatedRunName, 'streaming.tex',
            );
            const localBinary = vscode.Uri.joinPath(localRoot!, isolatedRunName, 'fixture.png');
            await waitForAsync(async () => {
                const [text, streaming, binary] = await Promise.all([
                    readLocalBytes(localDocument),
                    readLocalBytes(localStreamingDocument),
                    readLocalBytes(localBinary),
                ]);
                return text !== undefined
                    && Buffer.from(text).toString('utf8') === ''
                    && streaming !== undefined
                    && Buffer.from(streaming).toString('utf8') === ''
                    && binary !== undefined
                    && Buffer.from(binary).equals(Buffer.from(BINARY_FIXTURE));
            }, 30000, 'pull remote fixtures into Local Replica');
        });

        await step('binary upload and authoritative readback', async () => {
            const downloaded = await api!.getFile(identity!, projectId!, binaryId);
            assert(downloaded.type === 'success' && downloaded.content, 'Could not read binary fixture');
            assert(
                Buffer.from(downloaded.content).equals(Buffer.from(BINARY_FIXTURE)),
                'Binary readback differs byte-for-byte',
            );
            evidence.binaryReadback = {
                entityType: 'file',
                sha256: createHash('sha256').update(BINARY_FIXTURE).digest('hex'),
            };
        });

        await step('Local Replica watcher concurrent text overlap and delete', async () => {
            const fileName = 'watcher-roundtrip.tex';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);
            const localContent = `LOCAL-${request.nonce}\n`;
            const localHash = createHash('sha256').update(localContent).digest('hex');
            await vscode.workspace.fs.writeFile(localUri, Buffer.from(localContent, 'utf8'));

            let remoteDocument: {entityId: string; content: string} | undefined;
            await waitForAsync(async () => {
                remoteDocument = await freshDocument(
                    server.server.url,
                    api!,
                    identity!,
                    projectId!,
                    runFolderId!,
                    fileName,
                );
                return remoteDocument !== undefined
                    && createHash('sha256').update(remoteDocument.content).digest('hex') === localHash;
            }, 30000, 'push local text document to authoritative remote');
            assert(remoteDocument, 'Local text document did not appear as a remote doc');

            const editor = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(editor);
            let remoteSession: DurableDocumentSession | undefined;
            const receiveErrors: Error[] = [];
            editor.updateEventHandlers({
                onFileChanged: update => {
                    remoteSession?.receiveUpdate(update).catch(error => receiveErrors.push(error));
                },
            });
            await joinProjectWithProtocolFallback(editor, projectId!, 'join Local Replica remote editor');
            await waitFor(() => editor.sourceId !== undefined, 5000, 'receive remote editor source id');
            const joined = await withTimeout(
                editor.joinDoc(remoteDocument.entityId),
                10000,
                'join Local Replica round-trip document',
            );
            assert(joined.docLines.join('\n') === localContent,
                'Remote editor joined an unexpected Local Replica snapshot');
            const sessionErrors: Error[] = [];
            const remoteJournal = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${remoteDocument.entityId}-roundtrip`,
                initialRevision: joined.version,
                initialContent: localContent,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            remoteSession = await DurableDocumentSession.create({
                documentId: remoteDocument.entityId,
                authoritativeContent: localContent,
                authoritativeRevision: joined.version,
                journal: remoteJournal,
                sendUpdate: update => editor.applyOtUpdate(remoteDocument!.entityId, update as UpdateSchema),
                getSourceId: () => editor.sourceId,
                createOperationId: () => `e2e:${request.nonce}:${randomUUID()}`,
                onChange: () => {},
                onError: error => sessionErrors.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            sessions.push(remoteSession);
            const localToken = `LOCAL-OVERLAP-${request.nonce}\n`;
            const remoteToken = `REMOTE-OVERLAP-${request.nonce}\n`;
            const localOverlap = `${localToken}${localContent}`;
            const remoteOverlap = `${remoteToken}${localContent}`;
            await Promise.all([
                vscode.workspace.fs.writeFile(localUri, Buffer.from(localOverlap, 'utf8')),
                remoteSession.submitSnapshot(remoteOverlap),
            ]);
            let convergedContent = '';
            await waitForAsync(async () => {
                const local = await readLocalBytes(localUri);
                if (!local) {
                    return false;
                }
                convergedContent = Buffer.from(local).toString('utf8');
                return convergedContent === remoteSession!.content
                    && convergedContent.includes(localToken)
                    && convergedContent.includes(remoteToken);
            }, 45000, 'OT-converge overlapping Local Replica and remote edits');
            const authoritative = await freshDocument(
                server.server.url,
                api!,
                identity!,
                projectId!,
                runFolderId!,
                fileName,
            );
            assert(authoritative?.content === convergedContent,
                'Fresh remote observer differs from the Local Replica overlap result');
            assert(sessionErrors.length === 0 && receiveErrors.length === 0,
                'Remote round-trip editor entered fail-closed state');

            await waitForAsync(async () => {
                const state = await replicaStateStore!.reload();
                return state.entries[relativePath]?.hash
                    === createHash('sha256').update(convergedContent).digest('hex');
            }, 30000, 'persist the round-trip common base before late attachment', 250);
            await LocalReplicaSCMProvider.detachE2ETextReplica(
                localRoot!.fsPath,
                relativePath,
            );
            const lateBase = convergedContent;
            const lateLocalToken = `LATE-LOCAL-${request.nonce}\n`;
            const lateRemoteToken = `LATE-REMOTE-${request.nonce}\n`;
            await Promise.all([
                writeDiskFile(
                    localUri.fsPath,
                    Buffer.from(`${lateBase}${lateLocalToken}`, 'utf8'),
                ),
                remoteSession.submitSnapshotAgainstAncestor(
                    lateBase,
                    `${lateBase}${lateRemoteToken}`,
                ),
            ]);
            await waitForAsync(async () => {
                const [local, remote, state] = await Promise.all([
                    readLocalBytes(localUri),
                    freshDocument(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        runFolderId!,
                        fileName,
                    ),
                    replicaStateStore!.reload(),
                ]);
                if (!local || !remote) {
                    return false;
                }
                convergedContent = Buffer.from(local).toString('utf8');
                return convergedContent === remote.content
                    && convergedContent === remoteSession!.content
                    && convergedContent.includes(lateLocalToken)
                    && convergedContent.includes(lateRemoteToken)
                    && !state.pendingAction
                    && !state.conflicts[relativePath];
            }, 45000, 'OT-rebase local and remote changes during late text attachment', 500);

            await vscode.workspace.fs.delete(localUri);
            await waitForAsync(async () => {
                const current = await freshProject(
                    server.server.url,
                    api!,
                    identity!,
                    projectId!,
                    'verify Local Replica remote delete',
                );
                const parent = findFolder(current.rootFolder[0], runFolderId!);
                return parent !== undefined && !parent.docs.some(item => item.name === fileName);
            }, 30000, 'push Local Replica delete to authoritative remote', 750);
            await waitForAsync(async () => {
                const state = await replicaStateStore!.reload();
                return !state.pendingAction
                    && !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName))
                    && state.entries[relativePath] === undefined;
            }, 15000, 'settle clean Local Replica journal after round-trip');
            evidence.localTextRoundTrip = {
                entityType: 'doc',
                createdSha256: localHash,
                remotelyEditedSha256: createHash('sha256').update(convergedContent).digest('hex'),
                lateAttachRebased: true,
                remoteDeleteVerified: true,
            };
        });

        await step('dirty editor and external disk writer OT-converge', async () => {
            const fileName = 'writer-overlap.tex';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);
            const base = `BASE-${request.nonce}\n`;
            await vscode.workspace.fs.writeFile(localUri, Buffer.from(base, 'utf8'));
            await waitForAsync(async () => {
                const state = await replicaStateStore!.reload();
                // The persisted baseline is produced only after the Local
                // Replica has scanned and accepted the authoritative remote
                // snapshot. Avoid requiring a second fresh project-directory
                // socket here: Overleaf's directory listing can lag the OT
                // stream even after the document itself is acknowledged.
                return state.entries[relativePath]?.fileType === 'doc'
                    && state.entries[relativePath]?.hash
                        === createHash('sha256').update(base).digest('hex')
                    && !state.pendingAction;
            }, 30000, 'establish writer-overlap common baseline', 750);

            const document = await vscode.workspace.openTextDocument(localUri);
            const editorToken = `EDITOR-${request.nonce}\n`;
            const diskToken = `DISK-${request.nonce}\n`;
            const edit = new vscode.WorkspaceEdit();
            edit.insert(localUri, new vscode.Position(0, 0), editorToken);
            assert(await vscode.workspace.applyEdit(edit), 'Could not create the dirty editor fixture');
            assert(document.isDirty, 'Editor fixture was unexpectedly saved before the disk write');
            await writeDiskFile(localUri.fsPath, Buffer.from(`${base}${diskToken}`, 'utf8'));

            let converged = '';
            await waitForAsync(async () => {
                const [local, remote, state] = await Promise.all([
                    readLocalBytes(localUri),
                    freshDocument(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        runFolderId!,
                        fileName,
                    ),
                    replicaStateStore!.reload(),
                ]);
                if (!local || !remote) {
                    return false;
                }
                converged = Buffer.from(local).toString('utf8');
                return document.getText() === converged
                    && remote.content === converged
                    && converged.includes(editorToken)
                    && converged.includes(diskToken)
                    && !state.pendingAction
                    && !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName));
            }, 45000, 'converge dirty editor, disk, and remote snapshots', 750);
            evidence.localWriterOverlap = {
                sha256: createHash('sha256').update(converged).digest('hex'),
                journalConverged: true,
            };

            // The overlap assertion above intentionally leaves the editor
            // dirty. Cleanup must first make that verified snapshot durable;
            // deleting the disk file while an unsaved buffer still exists is
            // a new local-writer overlap, not an unambiguous fixture delete.
            assert(await document.save(), 'Could not save the converged writer-overlap fixture');
            assert(!document.isDirty, 'Writer-overlap fixture remained dirty before cleanup');
            await vscode.workspace.fs.delete(localUri);
            await waitForAsync(async () => {
                const remote = await freshDocument(
                    server.server.url,
                    api!,
                    identity!,
                    projectId!,
                    runFolderId!,
                    fileName,
                );
                const state = await replicaStateStore!.reload();
                return remote === undefined
                    && state.entries[relativePath] === undefined
                    && !state.pendingAction;
            }, 30000, 'delete writer-overlap fixture on both endpoints', 750);
        });

        await step('streaming VS Code, remote, and AI writers converge', async () => {
            const fileName = 'streaming.tex';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);
            const gatewayUri = vscode.Uri.joinPath(
                localRoot!, '.overleaf', 'safe-sync-gateway.json',
            );
            const document = await vscode.workspace.openTextDocument(localUri);
            assert(document.getText() === '', 'Streaming fixture did not start from an empty document');
            let streamingEditor = await vscode.window.showTextDocument(document, {
                preview: false,
                preserveFocus: false,
            });

            const socket = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(socket);
            let remoteSession: DurableDocumentSession | undefined;
            const receiveErrors: Error[] = [];
            socket.updateEventHandlers({
                onFileChanged: update => {
                    remoteSession?.receiveUpdate(update).catch(error => receiveErrors.push(error));
                },
            });
            await joinProjectWithProtocolFallback(socket, projectId!, 'join streaming remote writer');
            await waitFor(
                () => socket.sourceId !== undefined,
                5000,
                'receive streaming remote writer source id',
            );
            const joined = await withTimeout(
                socket.joinDoc(streamingDocumentId),
                10000,
                'join streaming document',
            );
            const base = joined.docLines.join('\n');
            assert(base === '', 'Streaming remote writer joined an unexpected baseline');
            const sessionErrors: Error[] = [];
            const journal = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${streamingDocumentId}-streaming-remote`,
                initialRevision: joined.version,
                initialContent: base,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            remoteSession = await DurableDocumentSession.create({
                documentId: streamingDocumentId,
                authoritativeContent: base,
                authoritativeRevision: joined.version,
                journal,
                sendUpdate: update => socket.applyOtUpdate(
                    streamingDocumentId,
                    update as UpdateSchema,
                ),
                getSourceId: () => socket.sourceId,
                createOperationId: () => `e2e:${request.nonce}:stream:${randomUUID()}`,
                onChange: () => {},
                onError: error => sessionErrors.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            sessions.push(remoteSession);

            await waitForAsync(
                () => localPathExists(gatewayUri),
                15000,
                'wait for the active AI gateway metadata',
                250,
            );
            const gatewayPath = gatewayUri.fsPath;
            // Keep the batching probe inside the extension host so its edit
            // spacing is controlled. `executeCommand('type')` crosses the
            // remote workbench IPC boundary, where one requested burst can be
            // delivered in arbitrarily delayed waves; the concurrent stream
            // below still exercises that real workbench path.
            const batchedLocalTokens = ['~', '!', '@', '#'];
            // A workbench `type` payload is not an atomic OT operation: the
            // client may emit one change event per character, and a valid
            // remote insert may land between two such events. Use unique
            // single-key tokens so the test proves exact-once retention and
            // causal order without imposing false transaction semantics on a
            // multi-character command.
            const localTokens = Array.from(
                {length: 48},
                (_, index) => String.fromCharCode(0xe000 + index),
            );
            const remoteTokens = Array.from(
                {length: 8},
                (_, index) => `REMOTE-STREAM-${request.nonce}-${index}\n`,
            );
            const aiTokens = Array.from(
                {length: 6},
                (_, index) => `AI-STREAM-${request.nonce}-${index}\n`,
            );
            const expectedTokens = [
                ...batchedLocalTokens,
                ...localTokens,
                ...remoteTokens,
                ...aiTokens,
            ];
            const expectedUtf16Length = expectedTokens.reduce(
                (total, token) => total + token.length,
                0,
            );
            let localEditCount = 0;
            let editorChangeEventCount = 0;
            let remoteSubmittedOperationCount = 0;
            let aiCommittedOperationCount = 0;
            const editorEvents = vscode.workspace.onDidChangeTextDocument(event => {
                if (event.document.uri.toString() === document.uri.toString()) {
                    editorChangeEventCount += event.contentChanges.length;
                }
            });
            const pause = (milliseconds: number): Promise<void> => new Promise(resolve => {
                setTimeout(resolve, milliseconds);
            });
            const applyEditorToken = async (token: string): Promise<void> => {
                if (vscode.window.activeTextEditor?.document.uri.toString()
                    !== document.uri.toString()) {
                    streamingEditor = await vscode.window.showTextDocument(document, {
                        preview: false,
                        preserveFocus: false,
                    });
                }
                const end = document.positionAt(document.getText().length);
                streamingEditor.selection = new vscode.Selection(end, end);
                await vscode.commands.executeCommand('type', {text: token});
                assert(document.getText().includes(token), 'VS Code type command lost a streaming edit');
                localEditCount += 1;
            };
            const applyEditorBurst = async (tokens: string[]): Promise<void> => {
                for (const token of tokens) {
                    const end = document.positionAt(document.getText().length);
                    const applied = await streamingEditor.edit(builder => {
                        builder.insert(end, token);
                    }, {
                        undoStopBefore: false,
                        undoStopAfter: false,
                    });
                    assert(applied, 'VS Code rejected a rapid editor edit');
                }
                assert(
                    tokens.every(token => document.getText().includes(token)),
                    'VS Code lost part of the rapid editor burst',
                );
                localEditCount += tokens.length;
            };
            const isRetryableAiError = (error: unknown): boolean => (
                /lease is already held|recovery is still in progress|baseline is not initialized/i
                    .test(safeMessage(error))
            );

            interface AiBeginResult {
                leaseId: string;
                fencingToken: number;
                content: string;
            }
            interface AiCommitResult {
                journaled: boolean;
            }

            const commitAiToken = async (token: string): Promise<void> => {
                let lastError: unknown;
                for (let attempt = 0; attempt < 80; attempt++) {
                    let begin: AiBeginResult | undefined;
                    try {
                        begin = await sendAiGatewayRequest<AiBeginResult>(gatewayPath, {
                            method: 'begin',
                            path: relativePath,
                            holder: `e2e-ai-${request.nonce}`,
                        });
                        const committed = await sendAiGatewayRequest<AiCommitResult>(gatewayPath, {
                            method: 'commit',
                            path: relativePath,
                            leaseId: begin.leaseId,
                            fencingToken: begin.fencingToken,
                            content: `${begin.content}${token}`,
                        });
                        assert(committed.journaled, 'AI stream commit was not journaled');
                        aiCommittedOperationCount += 1;
                        return;
                    } catch (error) {
                        lastError = error;
                        if (begin) {
                            await sendAiGatewayRequest(gatewayPath, {
                                method: 'abort',
                                path: relativePath,
                                leaseId: begin.leaseId,
                                fencingToken: begin.fencingToken,
                            }).catch(() => {});
                        }
                        if (!isRetryableAiError(error) || attempt === 79) {
                            throw error;
                        }
                        await pause(50);
                    }
                }
                throw lastError instanceof Error ? lastError : new Error('AI stream did not commit');
            };

            try {
                await applyEditorBurst(batchedLocalTokens);
                await waitForAsync(() => Promise.resolve(
                    remoteSession!.revision >= joined.version + 1
                    && batchedLocalTokens.every(token => remoteSession!.content.includes(token)),
                ), 30000, 'journal one batched operation for rapid VS Code input', 100);
                const batchedLocalOperationCount = remoteSession.revision - joined.version;
                assert(
                    batchedLocalOperationCount === 1,
                    `${batchedLocalTokens.length} rapid edits created `
                    + `${batchedLocalOperationCount} remote operations instead of one batch`,
                );

                const outcomes = await Promise.allSettled([
                    (async () => {
                        for (const token of localTokens) {
                            await applyEditorToken(token);
                            await pause(18);
                        }
                    })(),
                    (async () => {
                        for (const token of remoteTokens) {
                            const ancestor = remoteSession!.content;
                            await remoteSession!.submitSnapshotAgainstAncestor(
                                ancestor,
                                `${ancestor}${token}`,
                            );
                            remoteSubmittedOperationCount += 1;
                            await pause(23);
                        }
                    })(),
                    (async () => {
                        for (const token of aiTokens) {
                            await commitAiToken(token);
                            await pause(31);
                        }
                    })(),
                ]);
                const failed = outcomes.find(
                    (outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected',
                );
                if (failed) {
                    throw failed.reason;
                }
            } finally {
                editorEvents.dispose();
            }
            assert(
                localEditCount === batchedLocalTokens.length + localTokens.length,
                'Streaming editor did not issue every edit',
            );
            assert(
                editorChangeEventCount >= localEditCount,
                `Streaming editor emitted ${editorChangeEventCount} change events for ${localEditCount} edits`,
            );
            assert(
                remoteSubmittedOperationCount === remoteTokens.length,
                'Remote streaming writer did not acknowledge every operation',
            );
            assert(
                aiCommittedOperationCount === aiTokens.length,
                'AI streaming writer did not journal every operation',
            );
            const containsAllTokens = (content: string): boolean => (
                expectedTokens.every(token => content.includes(token))
            );
            const containsInOrder = (content: string, tokens: string[]): boolean => {
                let cursor = 0;
                for (const token of tokens) {
                    const index = content.indexOf(token, cursor);
                    if (index < 0) {
                        return false;
                    }
                    cursor = index + token.length;
                }
                return true;
            };
            const preservesWriterOrder = (content: string): boolean => (
                containsInOrder(content, localTokens)
                && containsInOrder(content, remoteTokens)
                && containsInOrder(content, aiTokens)
            );
            let converged = '';
            await waitForAsync(async () => {
                const [remote, state] = await Promise.all([
                    freshDocument(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        runFolderId!,
                        fileName,
                    ),
                    replicaStateStore!.reload(),
                ]);
                if (!remote || !containsAllTokens(document.getText()) || !containsAllTokens(remote.content)) {
                    return false;
                }
                converged = remote.content;
                return document.getText() === converged
                    && converged.length === expectedUtf16Length
                    && preservesWriterOrder(converged)
                    && !state.pendingAction
                    && !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName));
            }, 60000, 'converge all streaming writer snapshots', 500);
            assert(await document.save(), 'Could not save the converged streaming document');
            await waitForAsync(async () => {
                const [local, remote, state] = await Promise.all([
                    readLocalBytes(localUri),
                    freshDocument(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        runFolderId!,
                        fileName,
                    ),
                    replicaStateStore!.reload(),
                ]);
                if (!local || !remote) {
                    return false;
                }
                converged = remote.content;
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return Buffer.from(local).toString('utf8') === converged
                    && document.getText() === converged
                    && containsAllTokens(converged)
                    && converged.length === expectedUtf16Length
                    && preservesWriterOrder(converged)
                    && !state.pendingAction
                    && !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName))
                    && runtime?.settled === true;
            }, 60000, 'persist the converged streaming document to disk', 500);
            assert(sessionErrors.length === 0 && receiveErrors.length === 0,
                'A streaming remote session entered fail-closed state');
            const localSubmittedOperationCount = remoteSession.revision
                - joined.version
                - remoteSubmittedOperationCount
                - aiCommittedOperationCount;
            assert(
                localSubmittedOperationCount > 0
                && localSubmittedOperationCount < localEditCount,
                `${localEditCount} streaming editor edits were not durably batched: `
                + `${localSubmittedOperationCount} local operations`,
            );
            evidence.streamingWriters = {
                path: relativePath,
                batchedLocalEditCount: batchedLocalTokens.length,
                batchedLocalOperationCount: 1,
                localEditCount,
                localSubmittedOperationCount,
                editorChangeEventCount,
                remoteSubmittedOperationCount,
                aiCommittedOperationCount,
                finalRevision: remoteSession.revision,
                expectedUtf16Length,
                perWriterOrderPreserved: true,
                sha256: createHash('sha256').update(converged).digest('hex'),
                savedToDisk: true,
            };
        });

        await step('Local Replica watcher uploads SVG and stable binary resources', async () => {
            const fixtures = [
                {
                    name: 'watcher-resource.svg',
                    bytes: Buffer.from(
                        `<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1">`
                        + `<title>${request.nonce}</title><rect width="1" height="1"/></svg>`,
                        'utf8',
                    ),
                },
                {name: 'watcher-binary.png', bytes: Buffer.from(BINARY_FIXTURE)},
            ];
            const verifiedResources: NonNullable<E2EEvidence['localResources']> = [];
            for (const fixture of fixtures) {
                await vscode.workspace.fs.writeFile(
                    vscode.Uri.joinPath(localRoot!, isolatedRunName, fixture.name),
                    fixture.bytes,
                );
                await waitForAsync(async () => {
                    const current = await freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        `locate ${fixture.name}`,
                    );
                    const parent = findFolder(current.rootFolder[0], runFolderId!);
                    const resource = parent?.fileRefs.find(item => item.name === fixture.name);
                    if (!resource) {
                        return false;
                    }
                    const downloaded = await api!.getFile(identity!, projectId!, resource._id);
                    return downloaded.type === 'success'
                        && downloaded.content !== undefined
                        && Buffer.from(downloaded.content).equals(fixture.bytes);
                }, 30000, `upload ${fixture.name} as byte-exact remote resource`, 750);
                verifiedResources.push({
                    name: fixture.name,
                    entityType: 'file',
                    sha256: createHash('sha256').update(fixture.bytes).digest('hex'),
                });
            }
            evidence.localResources = verifiedResources;
        });

        await step('concurrent binary changes archive both variants and resolve explicitly', async () => {
            const fileName = 'watcher-binary.png';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);
            const localBytes = Buffer.concat([
                Buffer.from(BINARY_FIXTURE),
                Buffer.from(`LOCAL-BINARY-${request.nonce}`, 'utf8'),
            ]);
            const remoteBytes = Buffer.concat([
                Buffer.from(BINARY_FIXTURE),
                Buffer.from(`REMOTE-BINARY-${request.nonce}`, 'utf8'),
            ]);
            const localHash = createHash('sha256').update(localBytes).digest('hex');
            const remoteHash = createHash('sha256').update(remoteBytes).digest('hex');

            await Promise.all([
                vscode.workspace.fs.writeFile(localUri, localBytes),
                (async () => {
                    const current = await freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        'locate binary conflict baseline',
                    );
                    const parent = findFolder(current.rootFolder[0], runFolderId!);
                    const resource = parent?.fileRefs.find(item => item.name === fileName);
                    assert(resource, 'Binary conflict baseline is not a remote resource');
                    const removed = await api!.deleteEntity(
                        identity!, projectId!, 'file', resource._id,
                    );
                    assert(removed.type === 'success', 'Could not replace remote binary baseline');
                    const uploaded = await api!.uploadFile(
                        identity!, projectId!, runFolderId!, fileName, remoteBytes,
                    );
                    assert(uploaded.type === 'success' && uploaded.entity,
                        'Could not upload concurrent remote binary variant');
                })(),
            ]);

            let artifactDirectory = '';
            await waitForAsync(async () => {
                const state = await replicaStateStore!.reload();
                const record = state.conflicts[relativePath];
                if (!record?.localFile || !record.remoteFile || state.pendingAction) {
                    return false;
                }
                const [archivedLocal, archivedRemote, currentLocal] = await Promise.all([
                    readLocalBytes(vscode.Uri.joinPath(localRoot!, ...record.localFile.split('/'))),
                    readLocalBytes(vscode.Uri.joinPath(localRoot!, ...record.remoteFile.split('/'))),
                    readLocalBytes(localUri),
                ]);
                if (!archivedLocal || !archivedRemote || !currentLocal) {
                    return false;
                }
                artifactDirectory = record.artifactDirectory;
                return record.localHash === localHash
                    && record.remoteHash === remoteHash
                    && Buffer.from(archivedLocal).equals(localBytes)
                    && Buffer.from(archivedRemote).equals(remoteBytes)
                    && Buffer.from(currentLocal).equals(remoteBytes);
            }, 45000, 'archive both concurrent binary variants', 750);

            const resolvedArtifactDirectory = await LocalReplicaSCMProvider
                .resolveE2EConflictUsingRemote(localRoot!.fsPath, relativePath);
            assert(
                resolvedArtifactDirectory === artifactDirectory.replace(
                    '.overleaf/conflicts/unresolved/',
                    '.overleaf/conflicts/resolved/',
                ),
                'Binary conflict artifacts were not moved to the resolved archive',
            );
            const resolvedArtifactUri = vscode.Uri.joinPath(
                localRoot!, ...resolvedArtifactDirectory.split('/'),
            );
            assert(await localPathExists(resolvedArtifactUri),
                'Resolved binary conflict archive is missing');

            await waitForAsync(async () => {
                const [state, currentLocal, current] = await Promise.all([
                    replicaStateStore!.reload(),
                    readLocalBytes(localUri),
                    freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        'verify resolved binary conflict',
                    ),
                ]);
                const parent = findFolder(current.rootFolder[0], runFolderId!);
                const resource = parent?.fileRefs.find(item => item.name === fileName);
                if (!resource || !currentLocal) {
                    return false;
                }
                const downloaded = await api!.getFile(identity!, projectId!, resource._id);
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return downloaded.type === 'success'
                    && downloaded.content !== undefined
                    && Buffer.from(downloaded.content).equals(remoteBytes)
                    && Buffer.from(currentLocal).equals(remoteBytes)
                    && !state.conflicts[relativePath]
                    && !state.pendingAction
                    && runtime?.settled === true;
            }, 30000, 'finish explicit binary conflict resolution', 750);

            const current = await freshProject(
                server.server.url,
                api!,
                identity!,
                projectId!,
                'verify binary conflict artifacts remain local',
            );
            const root = current.rootFolder[0];
            assert(!root.folders.some(item => item.name === '.overleaf')
                && !root.docs.some(item => item.name === '.overleaf')
                && !root.fileRefs.some(item => item.name === '.overleaf'),
            'Binary conflict artifacts reached the remote project tree');
            evidence.binaryConflict = {
                localSha256: localHash,
                remoteSha256: remoteHash,
                archivesVerified: true,
                resolvedUsing: 'remote',
            };
            await vscode.workspace.fs.delete(
                resolvedArtifactUri,
                {recursive: true, useTrash: false},
            );
        });

        await step('resource conflict resolution attaches bidirectional text sync', async () => {
            const fileName = 'reattach.bin';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);
            const fileBase = Buffer.from(`FILE-BASE-${request.nonce}\n`, 'utf8');
            const uploaded = await api!.uploadFile(
                identity!,
                projectId!,
                runFolderId!,
                fileName,
                fileBase,
            );
            assert(uploaded.type === 'success' && uploaded.entity,
                'Could not create text conflict file baseline');
            const fileEntityId = uploaded.entity._id;
            const fileBaseHash = createHash('sha256').update(fileBase).digest('hex');
            await waitForAsync(async () => {
                const [local, state] = await Promise.all([
                    readLocalBytes(localUri),
                    replicaStateStore!.reload(),
                ]);
                return local !== undefined
                    && Buffer.from(local).equals(fileBase)
                    && state.entries[relativePath]?.fileType === 'file'
                    && state.entries[relativePath]?.hash === fileBaseHash
                    && !state.pendingAction;
            }, 30000, 'establish text conflict file baseline', 750);

            const localVariant = Buffer.from(`LOCAL-FILE-${request.nonce}\n`, 'utf8');
            await vscode.workspace.fs.writeFile(localUri, localVariant);
            const removed = await api!.deleteEntity(
                identity!, projectId!, 'file', fileEntityId,
            );
            assert(removed.type === 'success', 'Could not remove text conflict file baseline');
            const created = await api!.addDoc(
                identity!, projectId!, runFolderId!, fileName,
            );
            assert(created.type === 'success' && created.entity,
                'Could not replace text conflict baseline with a document');
            const documentId = created.entity._id;

            let artifactDirectory = '';
            await waitForAsync(async () => {
                const state = await replicaStateStore!.reload();
                const record = state.conflicts[relativePath];
                if (!record || state.pendingAction) {
                    return false;
                }
                artifactDirectory = record.artifactDirectory;
                return record.localFile !== undefined
                    && record.remoteFile !== undefined
                    && record.remoteEntityId === documentId;
            }, 45000, 'isolate text resource replacement conflict', 750);

            const resolvedArtifactDirectory = await LocalReplicaSCMProvider
                .resolveE2EConflictUsingRemote(localRoot!.fsPath, relativePath);
            assert(
                resolvedArtifactDirectory === artifactDirectory.replace(
                    '.overleaf/conflicts/unresolved/',
                    '.overleaf/conflicts/resolved/',
                ),
                'Text conflict artifacts were not moved to the resolved archive',
            );
            await waitForAsync(async () => {
                const [local, state] = await Promise.all([
                    readLocalBytes(localUri),
                    replicaStateStore!.reload(),
                ]);
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return local !== undefined
                    && local.byteLength === 0
                    && state.entries[relativePath]?.fileType === 'doc'
                    && state.entries[relativePath]?.entityId === documentId
                    && !state.conflicts[relativePath]
                    && !state.pendingAction
                    && runtime?.settled === true;
            }, 30000, 'finish remote text conflict resolution', 750);

            const document = await vscode.workspace.openTextDocument(localUri);
            const localToken = `LOCAL-AFTER-RESOLVE-${request.nonce}\n`;
            const localEdit = new vscode.WorkspaceEdit();
            localEdit.insert(localUri, new vscode.Position(0, 0), localToken);
            assert(await vscode.workspace.applyEdit(localEdit),
                'Could not edit the reattached local document');
            await waitForAsync(async () => (
                (await freshDocument(
                    server.server.url,
                    api!,
                    identity!,
                    projectId!,
                    runFolderId!,
                    fileName,
                ))?.content === localToken
            ), 30000, 'push local edit after text conflict resolution', 750);
            assert(await document.save(), 'Could not save the reattached local document');

            const remoteEditor = new SocketIOAPI(
                server.server.url, api!, identity!, projectId!,
            );
            sockets.push(remoteEditor);
            let remoteSession: DurableDocumentSession | undefined;
            const receiveErrors: Error[] = [];
            remoteEditor.updateEventHandlers({
                onFileChanged: update => {
                    remoteSession?.receiveUpdate(update).catch(error => receiveErrors.push(error));
                },
            });
            await joinProjectWithProtocolFallback(
                remoteEditor, projectId!, 'join post-resolution remote editor',
            );
            await waitFor(
                () => remoteEditor.sourceId !== undefined,
                5000,
                'receive post-resolution remote editor source id',
            );
            const joined = await withTimeout(
                remoteEditor.joinDoc(documentId),
                10000,
                'join post-resolution document',
            );
            const remoteBase = joined.docLines.join('\n');
            assert(remoteBase === localToken,
                'Post-resolution remote editor joined an unexpected snapshot');
            const sessionErrors: Error[] = [];
            const remoteJournal = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${documentId}-reattach`,
                initialRevision: joined.version,
                initialContent: remoteBase,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            remoteSession = await DurableDocumentSession.create({
                documentId,
                authoritativeContent: remoteBase,
                authoritativeRevision: joined.version,
                journal: remoteJournal,
                sendUpdate: update => remoteEditor.applyOtUpdate(documentId, update as UpdateSchema),
                getSourceId: () => remoteEditor.sourceId,
                createOperationId: () => `e2e:${request.nonce}:reattach:${randomUUID()}`,
                onChange: () => {},
                onError: error => sessionErrors.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            sessions.push(remoteSession);
            const remoteToken = `REMOTE-AFTER-RESOLVE-${request.nonce}\n`;
            await withTimeout(
                remoteSession.submitSnapshot(`${remoteToken}${remoteBase}`),
                20000,
                'acknowledge remote edit after text conflict resolution',
            );

            let converged = '';
            await waitForAsync(async () => {
                const [local, remote, state] = await Promise.all([
                    readLocalBytes(localUri),
                    freshDocument(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        runFolderId!,
                        fileName,
                    ),
                    replicaStateStore!.reload(),
                ]);
                if (!local || !remote) {
                    return false;
                }
                converged = Buffer.from(local).toString('utf8');
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return document.getText() === converged
                    && remote.content === converged
                    && converged.includes(localToken)
                    && converged.includes(remoteToken)
                    && !state.conflicts[relativePath]
                    && !state.pendingAction
                    && runtime?.settled === true;
            }, 45000, 'converge reattached editor, disk, and remote document', 750);
            assert(sessionErrors.length === 0 && receiveErrors.length === 0,
                'Post-resolution remote editor entered fail-closed state');
            evidence.textResourceConflictRecovery = {
                resolvedUsing: 'remote',
                reattached: true,
                sha256: createHash('sha256').update(converged).digest('hex'),
            };
        });

        await step('conflict artifact watcher events remain local-only', async () => {
            const artifactDirectory = vscode.Uri.joinPath(
                localRoot!,
                '.overleaf',
                'conflicts',
                'unresolved',
                `e2e-${request.nonce}`,
            );
            try {
                await vscode.workspace.fs.createDirectory(artifactDirectory);
                await vscode.workspace.fs.writeFile(
                    vscode.Uri.joinPath(artifactDirectory, 'ignored.tex'),
                    Buffer.from(`ignored-${request.nonce}\n`, 'utf8'),
                );
                const deadline = Date.now() + 3000;
                do {
                    const current = await freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        'verify conflict artifacts stay local',
                    );
                    const root = current.rootFolder[0];
                    assert(!root.folders.some(item => item.name === '.overleaf')
                        && !root.docs.some(item => item.name === '.overleaf')
                        && !root.fileRefs.some(item => item.name === '.overleaf'),
                    'A local .overleaf conflict artifact reached the remote project tree');
                    if (Date.now() < deadline) {
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }
                } while (Date.now() < deadline);
                const state = await replicaStateStore!.reload();
                assert(!state.pendingAction && !Object.keys(state.conflicts)
                    .some(filePath => isE2EConflictPath(filePath, isolatedRunName)),
                    'Ignored conflict watcher event changed the active replica journal');
                evidence.conflictArtifactsRemoteAbsent = true;
            } finally {
                try {
                    await vscode.workspace.fs.delete(artifactDirectory, {recursive: true, useTrash: false});
                } catch (error) {
                    if ((error as vscode.FileSystemError).code !== 'FileNotFound') {
                        throw error;
                    }
                }
            }
        });

        await step('stable entity id across remote and local move/rename', async () => {
            const moved = await api!.moveEntity(identity!, projectId!, 'doc', movedDocumentId, folderB._id);
            assert(moved.type === 'success', `Move failed: ${moved.message ?? 'unknown error'}`);
            const renamed = await api!.renameEntity(identity!, projectId!, 'doc', movedDocumentId, 'after-move.tex');
            assert(renamed.type === 'success', `Rename failed: ${renamed.message ?? 'unknown error'}`);

            const observer = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(observer);
            const current = await joinProjectWithProtocolFallback(
                observer,
                projectId!,
                'refresh moved entity',
            );
            const located = findEntity(current.rootFolder[0], movedDocumentId);
            assert(located, 'Moved entity disappeared from the project tree');
            assert(located.parentId === folderB._id, 'Moved entity has the wrong parent id');
            assert(located.entity.name === 'after-move.tex', 'Renamed entity has the wrong name');

            const remoteMovedLocalUri = vscode.Uri.joinPath(
                localRoot!, isolatedRunName, 'move-b', 'after-move.tex',
            );
            const oldLocalUri = vscode.Uri.joinPath(
                localRoot!, isolatedRunName, 'move-a', 'before-move.tex',
            );
            await waitForAsync(async () => (
                await localPathExists(remoteMovedLocalUri)
                && !await localPathExists(oldLocalUri)
            ), 30000, 'apply the remote move and rename to Local Replica');

            const localRenamedUri = vscode.Uri.joinPath(
                localRoot!, isolatedRunName, 'move-b', 'local-renamed.tex',
            );
            await vscode.workspace.fs.rename(
                remoteMovedLocalUri,
                localRenamedUri,
                {overwrite: false},
            );
            const finalPath = `${isolatedRunName}/move-b/local-renamed.tex`;
            await waitForAsync(async () => {
                const [refreshed, state] = await Promise.all([
                    freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        'verify Local Replica rename on remote',
                    ),
                    replicaStateStore!.reload(),
                ]);
                const remoteEntity = findEntity(refreshed.rootFolder[0], movedDocumentId);
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return remoteEntity?.parentId === folderB._id
                    && remoteEntity.entity.name === 'local-renamed.tex'
                    && state.entries[finalPath]?.entityId === movedDocumentId
                    && !state.pendingAction
                    && !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName))
                    && runtime?.settled === true;
            }, 45000, 'apply and settle the Local Replica rename on the remote entity', 750);
            const state = await replicaStateStore!.reload();
            assert(!state.pendingAction && !Object.keys(state.conflicts)
                .some(filePath => isE2EConflictPath(filePath, isolatedRunName)),
                'Move/rename left a pending action or conflict');
            assert(state.entries[finalPath]?.entityId === movedDocumentId,
                'Move/rename lost the stable remote entity id in the replica baseline');
        });

        await step('two-client concurrent OT convergence', async () => {
            const socketA = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            const socketB = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(socketA, socketB);

            let sessionA: DurableDocumentSession | undefined;
            let sessionB: DurableDocumentSession | undefined;
            const receiveErrors: Error[] = [];
            socketA.updateEventHandlers({
                onFileChanged: update => {
                    sessionA?.receiveUpdate(update).catch(error => receiveErrors.push(error));
                },
            });
            socketB.updateEventHandlers({
                onFileChanged: update => {
                    sessionB?.receiveUpdate(update).catch(error => receiveErrors.push(error));
                },
            });

            await withTimeout(Promise.all([
                joinProjectWithProtocolFallback(socketA, projectId!, 'join E2E client A'),
                joinProjectWithProtocolFallback(socketB, projectId!, 'join E2E client B'),
            ]), 22000, 'join two E2E clients');
            await waitFor(
                () => socketA.sourceId !== undefined && socketB.sourceId !== undefined,
                5000,
                'receive socket source ids',
            );
            assert(socketA.sourceId !== socketB.sourceId, 'Concurrent sockets received the same source id');

            const [joinedA, joinedB] = await withTimeout(Promise.all([
                socketA.joinDoc(documentId),
                socketB.joinDoc(documentId),
            ]), 12000, 'join concurrent document');
            assert(joinedA.otType === 'sharejs-text-ot' && joinedB.otType === 'sharejs-text-ot', 'Server advertised an unsupported OT type');
            assert(joinedA.version === joinedB.version, 'Clients joined different document revisions');
            const baseA = joinedA.docLines.join('\n');
            const baseB = joinedB.docLines.join('\n');
            assert(baseA === baseB, 'Clients joined different document snapshots');
            const baseRevision = joinedA.version;

            const failures: Error[] = [];
            const journalA = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${documentId}-client-a`,
                initialRevision: baseRevision,
                initialContent: baseA,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            const journalB = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${documentId}-client-b`,
                initialRevision: baseRevision,
                initialContent: baseB,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            sessionA = await DurableDocumentSession.create({
                documentId,
                authoritativeContent: baseA,
                authoritativeRevision: baseRevision,
                journal: journalA,
                sendUpdate: update => socketA.applyOtUpdate(documentId, update as UpdateSchema),
                getSourceId: () => socketA.sourceId,
                createOperationId: () => `e2e:${request.nonce}:a:${randomUUID()}`,
                onChange: () => {},
                onError: error => failures.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            sessionB = await DurableDocumentSession.create({
                documentId,
                authoritativeContent: baseB,
                authoritativeRevision: baseRevision,
                journal: journalB,
                sendUpdate: update => socketB.applyOtUpdate(documentId, update as UpdateSchema),
                getSourceId: () => socketB.sourceId,
                createOperationId: () => `e2e:${request.nonce}:b:${randomUUID()}`,
                onChange: () => {},
                onError: error => failures.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            sessions.push(sessionA, sessionB);

            const left = `LEFT-${request.nonce}`;
            const right = `RIGHT-${request.nonce}`;
            await withTimeout(Promise.all([
                sessionA.submitSnapshot(left),
                sessionB.submitSnapshot(right),
            ]), 20000, 'acknowledge concurrent OT edits');
            await waitFor(
                () => sessionA!.content === sessionB!.content,
                5000,
                'converge both clients',
            );
            assert(failures.length === 0 && receiveErrors.length === 0, 'A client entered fail-closed state during valid OT');
            const converged = sessionA.content;
            assert(converged.includes(left) && converged.includes(right), 'Converged document lost one concurrent insertion');

            const catchUp = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(catchUp);
            await joinProjectWithProtocolFallback(
                catchUp,
                projectId!,
                'join catch-up observer',
            );
            const authoritative = await withTimeout(
                catchUp.joinDoc(documentId, baseRevision),
                12000,
                'fetch exact OT catch-up',
            );
            assert(authoritative.version === baseRevision + 2, 'Concurrent edits did not create exactly two revisions');
            assert(authoritative.updates.length === 2, 'Server did not return the exact two-operation catch-up history');
            const authoritativeContent = authoritative.docLines.join('\n');
            assert(authoritativeContent === converged, 'Fresh observer did not converge with both editing clients');

            let replay = baseA;
            for (let index = 0; index < authoritative.updates.length; index++) {
                const update = authoritative.updates[index];
                assert(update.v === baseRevision + index, 'Catch-up revision sequence is discontinuous');
                assert(update.op, 'Catch-up history omitted an operation');
                const operation = TextOperation.fromLegacyEdits(
                    replay,
                    update.op,
                    value => Buffer.from(value, 'latin1').toString('utf8'),
                );
                replay = operation.apply(replay);
            }
            assert(replay === authoritativeContent, 'Catch-up operations do not reproduce the authoritative snapshot');
            evidence.otCatchUp = {
                baseRevision,
                finalRevision: authoritative.version,
                operationCount: authoritative.updates.length,
                sha256: createHash('sha256').update(authoritativeContent).digest('hex'),
            };
        });

        await step('cleanup isolated fixtures while retaining project', async () => {
            await cleanupFixtures();
        });

        return {
            schemaVersion: E2E_RESULT_SCHEMA_VERSION,
            status: 'passed',
            serverName: request.serverName,
            projectName: request.projectName,
            nonce: request.nonce,
            ...extension,
            projectId,
            startedAt,
            finishedAt: new Date().toISOString(),
            steps,
            evidence,
        };
    } catch (error) {
        if (api && identity && projectId && serverUrl && runName) {
            try {
                await step('cleanup isolated fixtures after failure', cleanupFixtures);
            } catch (cleanupError) {
                output.appendLine(`[warn] fixture cleanup could not be proven: ${safeMessage(cleanupError)}`);
            }
        }
        return {
            schemaVersion: E2E_RESULT_SCHEMA_VERSION,
            status: 'failed',
            serverName: request.serverName,
            projectName: request.projectName,
            nonce: request.nonce,
            ...extension,
            projectId,
            startedAt,
            finishedAt: new Date().toISOString(),
            steps,
            evidence,
            error: safeMessage(error),
        };
    } finally {
        for (const session of sessions) {
            await session.close().catch(error => {
                output.appendLine(`[warn] E2E journal close failed: ${safeMessage(error)}`);
            });
        }
        await removeDisk(e2eJournalDirectory, {recursive: true, force: true}).catch(error => {
            output.appendLine(`[warn] E2E journal cleanup failed: ${safeMessage(error)}`);
        });
        for (const socket of sockets) {
            try {
                socket.disconnect();
            } catch {
                // Best-effort connection cleanup.
            }
        }
    }
}

async function execute(
    context: vscode.ExtensionContext,
    requestValue: unknown,
    output: vscode.OutputChannel,
): Promise<E2EResult> {
    const request = validateRequest(requestValue);
    while (running) {
        if (running.nonce === request.nonce) {
            return running.promise;
        }
        await running.promise;
    }
    const promise = runRemoteE2E(context, request, output);
    running = {nonce: request.nonce, promise};
    try {
        return await promise;
    } finally {
        if (running?.promise === promise) {
            running = undefined;
        }
    }
}

async function runRequestedE2E(
    context: vscode.ExtensionContext,
    output: vscode.OutputChannel,
): Promise<void> {
    const requestUri = vscode.Uri.joinPath(context.globalStorageUri, REQUEST_FILE);
    const runningUri = vscode.Uri.joinPath(context.globalStorageUri, RUNNING_FILE);
    const resultUri = vscode.Uri.joinPath(context.globalStorageUri, RESULT_FILE);
    let requestValue = await readJson<unknown>(requestUri);
    let recovering = false;
    if (requestValue === undefined) {
        const runningValue = await readJson<unknown>(runningUri);
        if (runningValue === undefined) {
            return;
        }
        let marker: E2ERunningMarker;
        try {
            marker = validateRunningMarker(runningValue);
        } catch (error) {
            output.appendLine(`[warn] stale E2E marker requires manual inspection: ${safeMessage(error)}`);
            return;
        }
        if (isProcessAlive(marker.ownerPid)) {
            return;
        }
        requestValue = marker.request;
        recovering = true;
    }
    if (!await workspaceTargetsApprovedReplica()) {
        output.appendLine('Approved E2E request left unclaimed: open the hershel-safe-sync-e2e Local Replica workspace.');
        return;
    }

    await vscode.workspace.fs.createDirectory(context.globalStorageUri);
    if (!recovering) {
        await vscode.workspace.fs.rename(requestUri, runningUri, {overwrite: true});
    }
    try {
        const request = validateRequest(requestValue);
        await writeJsonAtomic(runningUri, {
            schemaVersion: 1,
            request,
            ownerPid: process.pid,
            startedAt: new Date().toISOString(),
        } satisfies E2ERunningMarker);
    } catch {
        // execute() below records malformed requests as a redacted failed result.
    }
    output.show(true);
    const extension = buildE2EIdentity(context);
    output.appendLine(
        `${recovering ? 'Resuming' : 'Starting'} approved one-time safe-sync E2E run with ${extension.extensionId}`
        + `@${extension.extensionVersion} (${extension.buildId}). Credentials remain in VS Code SecretStorage.`,
    );
    let result: E2EResult;
    try {
        result = await execute(context, requestValue, output);
    } catch (error) {
        const now = new Date().toISOString();
        const request = requestValue as Partial<E2ERequest>;
        result = {
            schemaVersion: E2E_RESULT_SCHEMA_VERSION,
            status: 'failed',
            serverName: APPROVED_SERVER,
            projectName: APPROVED_PROJECT,
            nonce: typeof request.nonce === 'string' ? request.nonce : 'invalid-request',
            ...extension,
            startedAt: now,
            finishedAt: now,
            steps: [],
            evidence: {},
            error: safeMessage(error),
        };
    }
    await writeJsonAtomic(resultUri, result);
    try {
        await vscode.workspace.fs.delete(runningUri);
    } catch {
        // The result is authoritative; a stale marker can be removed manually.
    }
    const summary = result.status === 'passed'
        ? `Safe-sync E2E passed for ${APPROVED_PROJECT}.`
        : `Safe-sync E2E failed: ${result.error ?? 'unknown error'}`;
    output.appendLine(summary);
    if (result.status === 'passed') {
        vscode.window.showInformationMessage(summary);
    } else {
        vscode.window.showErrorMessage(summary);
    }
}

export function registerSafeSyncE2E(context: vscode.ExtensionContext): vscode.Disposable[] {
    const output = vscode.window.createOutputChannel('Hershel Safe Sync E2E');
    const command = vscode.commands.registerCommand(`${ROOT_NAME}.safeSync.runE2E`, async () => {
        const extension = buildE2EIdentity(context);
        const request: E2ERequest = {
            schemaVersion: 2,
            serverName: APPROVED_SERVER,
            projectName: APPROVED_PROJECT,
            nonce: `${Date.now()}-${process.pid}`,
            expectedExtensionId: extension.extensionId,
            expectedExtensionVersion: extension.extensionVersion,
            expectedBuildId: extension.buildId,
            expectedRunnerSha256: extension.runnerSha256,
        };
        output.show(true);
        output.appendLine(
            `Starting approved safe-sync E2E with ${extension.extensionId}`
            + `@${extension.extensionVersion} (${extension.buildId}).`,
        );
        const result = await execute(context, request, output);
        await writeJsonAtomic(vscode.Uri.joinPath(context.globalStorageUri, RESULT_FILE), result);
        return result;
    });
    (async () => {
        if (await consumeApprovedReloadHandoff(context, output)) {
            return;
        }
        await runRequestedE2E(context, output);
    })().catch(error => {
        output.appendLine(`[fail] could not process E2E startup request: ${safeMessage(error)}`);
    });
    return [output, command];
}
