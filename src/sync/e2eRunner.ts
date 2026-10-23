import * as vscode from 'vscode';
import { createHash, randomUUID } from 'crypto';
import { readFileSync, statSync } from 'fs';
import { open, rm as removeDisk, writeFile as writeDiskFile } from 'fs/promises';
import * as path from 'path';
import { BaseAPI, Identity, ResponseSchema } from '../api/base';
import { isSocketIOTimeout, SocketIOAPI, UpdateSchema } from '../api/socketio';
import { FolderEntity, ProjectEntity } from '../core/remoteFileSystemProvider';
import { ROOT_NAME } from '../consts';
import { GlobalStateManager } from '../utils/globalStateManager';
import { LocalReplicaSCMProvider } from '../scm/localReplicaSCM';
import { DurableDocumentSession } from './durableDocumentSession';
import { DocumentJournal } from './documentJournal';
import { DocumentStateStore } from './documentStateStore';
import { sendAiGatewayRequest } from './aiGateway';
import { assertLocalReplicaBinding, LocalReplicaBinding, projectIdFromUri } from './localBinding';
import { DetachedDocumentRecovery, ReplicaStateStore } from './replicaStateStore';
import { TextOperation } from './textOperation';
import { OtClientState } from './otClientState';
import { syncDirectoryDurably, writeFileDurably } from './durableFile';

const REQUEST_FILE = 'safe-sync-e2e.request.json';
const RUNNING_FILE = 'safe-sync-e2e.running.json';
const RESULT_FILE = 'safe-sync-e2e.result.json';
const RELOAD_FILE = 'safe-sync-e2e.reload.json';
const RESTART_RECOVERY_FILE = 'safe-sync-e2e.restart-recovery.json';
export const HANDOFF_TARGET_VERSION = '1.0.0-hershel.196';
export const HANDOFF_TARGET_BUILD_ID = '20261023T035901Z-safe-sync-v2-rc196';
export const E2E_RESULT_SCHEMA_VERSION = 13;
const BINARY_FIXTURE = Uint8Array.from(Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
    'base64',
));

interface E2ERequest {
    schemaVersion: 3;
    serverName: string;
    projectName: string;
    projectId: string;
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

interface E2ERestartRecoveryMarker {
    schemaVersion: 2;
    requestNonce: string;
    preReloadPid: number;
    namespace: string;
    runFolderId: string;
    documentId: string;
    relativePath: string;
    baseRevision: number;
    baseSha256: string;
    targetSha256: string;
    token: string;
    pendingOperationId: string;
    detachedLegacyDocumentId: string;
    detachedLegacyOperationId: string;
    detachedLegacyBaseSha256: string;
    detachedLegacyVisibleSha256: string;
    createdAt: string;
}

interface E2EStep {
    name: string;
    ok: boolean;
    durationMs: number;
    detail?: string;
}

interface E2EEvidence {
    fixtureNamespace?: string;
    /** Baseline entry the orphan-WAL manifest took its base side from. */
    orphanWalBaselineAtIsolation?: unknown;
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
    binaryMove?: {
        entityId: string;
        finalPath: string;
        sha256: string;
    };
    textResourceConflictRecovery?: {
        resolvedUsing: 'remote';
        reattached: boolean;
        sha256: string;
    };
    moveWithConcurrentEdit?: {
        entityId: string;
        finalPath: string;
        sha256: string;
    };
    orphanDocumentWalRecovery?: {
        path: string;
        oldDocumentId: string;
        replacementDocumentId: string;
        reason: string;
        oldRevision: number;
        replacementRevision: number;
        pendingOperationCount: number;
        baseSha256: string;
        acknowledgedSha256: string;
        visibleSha256: string;
        replacementSha256: string;
        localToRemoteSha256: string;
        remoteToLocalSha256: string;
        resolvedArtifactDirectory: string;
    };
    localWriterOverlap?: {sha256: string; journalConverged: boolean};
    restartRecovery?: {
        path: string;
        entityId: string;
        operationId: string;
        preReloadPid: number;
        postReloadPid: number;
        baseRevision: number;
        finalRevision: number;
        sha256: string;
    };
    detachedLegacyRecovery?: {
        documentId: string;
        operationId: string;
        artifactDirectory: string;
        baseSha256: string;
        visibleSha256: string;
        walRetired: boolean;
        projectPauseEvents: number;
    };
    singleWriterStreams?: {
        path: string;
        phases: Array<{
            writer: 'remote' | 'vscode' | 'ai';
            editCount: number;
            observedBeforeWriterStopped: boolean;
            convergenceAfterWriterMs: number;
            finalRevision: number;
            sha256: string;
        }>;
    };
    streamingWriters?: {
        path: string;
        batchedLocalEditCount: number;
        batchedLocalOperationCount: number;
        localEditCount: number;
        editorChangeEventCount: number;
        remoteJournaledEditCount: number;
        aiCommittedOperationCount: number;
        rawFilesystemEditCount: number;
        finalRevision: number;
        expectedUtf16Length: number;
        perWriterOrderPreserved: boolean;
        textIsolationEvents: number;
        projectPauseEvents: number;
        cleanRemotePullCount: number;
        remoteObservedBeforeLocalStop: boolean;
        remoteAdmissionToObservationLatenciesMs: number[];
        remotePullLatenciesMs: number[];
        maxRemotePullLatencyMs: number;
        p95RemotePullLatencyMs: number;
        sha256: string;
        savedToDisk: boolean;
        convergedBeforeExplicitSave: boolean;
        dirtyBeforeExplicitSave: boolean;
        saveEventsBeforeExplicitSave: number;
        autoSaveMode: string;
        aiOverlappedLocalAndRemote: boolean;
        rawFilesystemOverlappedLocalAndRemote: boolean;
        rangeReplaceVerified: boolean;
        rangeDeleteVerified: boolean;
        convergenceAfterWritersMs: number;
    };
    disjointParagraphEdits?: {
        path: string;
        commonBaseSha256: string;
        expectedSha256: string;
        finalRevision: number;
        aiBeganBeforeUserEdit: boolean;
        candidatesSharedBase: boolean;
        aiCommitJournaled: boolean;
        convergedBeforeExplicitSave: boolean;
        editorEqualsDisk: boolean;
        diskEqualsRemote: boolean;
        userParagraphOccurrences: number;
        aiParagraphOccurrences: number;
        textIsolationEvents: number;
        projectPauseEvents: number;
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
    cachedExtensionVersion: string;
    cachedBuildId: string;
    cachedMetadataMatches: boolean;
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

export function countExactOccurrences(content: string, fragment: string): number {
    if (fragment.length === 0) {
        throw new Error('Cannot count an empty E2E fragment');
    }
    let count = 0;
    let cursor = 0;
    while (cursor <= content.length - fragment.length) {
        const index = content.indexOf(fragment, cursor);
        if (index < 0) {
            break;
        }
        count++;
        cursor = index + fragment.length;
    }
    return count;
}

export function isExactComposition(
    content: string,
    fragments: readonly string[],
): boolean {
    return content.length === fragments.reduce((total, fragment) => total + fragment.length, 0)
        && fragments.every(fragment => countExactOccurrences(content, fragment) === 1);
}

interface OrphanWalManifestExpectation {
    path: string;
    oldDocumentId: string;
    replacementDocumentId: string;
    baseSha256: string;
    acknowledgedSha256: string;
    visibleSha256: string;
    replacementSha256: string;
}

interface VerifiedOrphanWalManifest {
    baseFile: string;
    localFile: string;
    visibleFile: string;
    remoteFile: string;
    acknowledgedFile: string;
    reason: string;
    oldRevision: number;
    replacementRevision: number;
    pendingOperationCount: number;
}

/** Verify the durable evidence contract before an E2E run reads archived bytes. */
export function assertResolvedOrphanWalManifest(
    value: unknown,
    expected: OrphanWalManifestExpectation,
): VerifiedOrphanWalManifest {
    assert(value !== null && typeof value === 'object', 'Resolved WAL manifest is not an object');
    const manifest = value as Record<string, unknown>;
    assert(manifest.schemaVersion === 3, 'Resolved WAL manifest schema changed');
    assert(manifest.path === expected.path, 'Resolved WAL manifest path changed');
    assert(manifest.baselineEntityId === expected.oldDocumentId,
        'Resolved WAL manifest lost the old immutable document id');
    assert(manifest.remoteEntityId === expected.replacementDocumentId,
        'Resolved WAL manifest lost the replacement document id');
    assert(manifest.baseType === 'doc' && manifest.localType === 'doc' && manifest.remoteType === 'doc',
        'Resolved WAL manifest lost document type provenance');
    // Name what the hash actually is. A manifest whose local side was
    // overwritten carries a hash this run already knows by another name, and
    // saying which one turns a bare mismatch into the writer's identity.
    const namedHash = (value: unknown): string => {
        for (const [label, candidate] of [
            ['base', expected.baseSha256],
            ['acknowledged', expected.acknowledgedSha256],
            ['visible', expected.visibleSha256],
            ['replacement', expected.replacementSha256],
        ] as const) {
            if (value === candidate) {
                return `${label} (${String(value).slice(0, 12)})`;
            }
        }
        return `unknown (${String(value).slice(0, 12)})`;
    };
    assert(
        manifest.baseHash === expected.baseSha256,
        `Resolved WAL manifest base hash changed: base side holds ${namedHash(manifest.baseHash)}`,
    );
    assert(
        manifest.localHash === expected.visibleSha256,
        'Resolved WAL manifest visible hash changed: local side holds '
        + `${namedHash(manifest.localHash)}, last written by `
        + `${String(manifest.latestLocalSource ?? 'isolation')} at `
        + `${String(manifest.latestLocalRevisionAt ?? 'isolation time')}`,
    );
    assert(
        manifest.remoteHash === expected.replacementSha256,
        'Resolved WAL manifest replacement hash changed: remote side holds '
        + `${namedHash(manifest.remoteHash)}`,
    );
    assert(typeof manifest.reason === 'string' && manifest.reason.length > 0,
        'Resolved WAL manifest omitted the conflict reason');
    assert(Number.isInteger(manifest.remoteRevision) && Number(manifest.remoteRevision) >= 0,
        'Resolved WAL manifest omitted the replacement revision');
    assert(typeof manifest.resolvedAt === 'string' && manifest.resolvedAt.length > 0,
        'Resolved WAL manifest omitted resolution time');

    const recoveries = manifest.documentRecoveries;
    assert(Array.isArray(recoveries), 'Resolved WAL manifest omitted document recoveries');
    const recovery = recoveries.find(candidate => (
        candidate !== null
        && typeof candidate === 'object'
        && (candidate as Record<string, unknown>).entityId === expected.oldDocumentId
    )) as Record<string, unknown> | undefined;
    assert(recovery, 'Resolved WAL manifest omitted the old document recovery');
    assert(recovery.visibleHash === expected.visibleSha256,
        'Resolved WAL manifest visible recovery hash changed');
    assert(recovery.acknowledgedHash === expected.acknowledgedSha256,
        'Resolved WAL manifest acknowledged recovery hash changed');
    assert(Number.isInteger(recovery.revision) && Number(recovery.revision) >= 0,
        'Resolved WAL manifest omitted the old document revision');
    assert(Number.isInteger(recovery.epoch) && Number(recovery.epoch) >= 0,
        'Resolved WAL manifest omitted the recovery epoch');
    assert(typeof recovery.snapshotHash === 'string' && /^[a-f0-9]{64}$/.test(recovery.snapshotHash),
        'Resolved WAL manifest omitted the recovery snapshot hash');
    assert(Array.isArray(recovery.pendingOperationIds) && recovery.pendingOperationIds.length > 0,
        'Resolved WAL manifest omitted pending operation ids');

    const baseFile = manifest.baseFile;
    const localFile = manifest.localFile;
    const remoteFile = manifest.remoteFile;
    const visibleFile = recovery.visibleFile;
    const acknowledgedFile = recovery.acknowledgedFile;
    assert(typeof baseFile === 'string' && baseFile.length > 0,
        'Resolved WAL manifest omitted the common-base file');
    assert(typeof localFile === 'string' && localFile.length > 0,
        'Resolved WAL manifest omitted the current local candidate');
    assert(typeof visibleFile === 'string' && visibleFile.length > 0,
        'Resolved WAL manifest omitted the visible WAL candidate');
    assert(typeof remoteFile === 'string' && remoteFile.length > 0,
        'Resolved WAL manifest omitted the replacement remote file');
    assert(typeof acknowledgedFile === 'string' && acknowledgedFile.length > 0,
        'Resolved WAL manifest omitted the acknowledged WAL file');
    for (const artifact of [baseFile, localFile, visibleFile, remoteFile, acknowledgedFile]) {
        assert(artifact.startsWith('.overleaf/conflicts/resolved/'),
            'Resolved WAL manifest retained an unresolved artifact path');
    }
    return {
        baseFile,
        localFile,
        visibleFile,
        remoteFile,
        acknowledgedFile,
        reason: manifest.reason,
        oldRevision: Number(recovery.revision),
        replacementRevision: Number(manifest.remoteRevision),
        pendingOperationCount: recovery.pendingOperationIds.length,
    };
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
    // A label can describe why the wait was still unsatisfied, which is only
    // known once it has failed, so resolve it at that point.
    label: string | (() => string),
    intervalMs = 500,
): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!await predicate()) {
        if (Date.now() >= deadline) {
            throw new Error(`${typeof label === 'string' ? label : label()} timed out`);
        }
        await new Promise(resolve => setTimeout(resolve, intervalMs));
    }
}

/**
 * Leave a document saved. VS Code's auto-save can already be writing the same
 * buffer, which makes an explicit `save()` a no-op that reports false, so the
 * invariant to assert is the saved state itself rather than which writer
 * reached it.
 */
async function saveDocument(
    document: vscode.TextDocument,
    label: string,
): Promise<void> {
    if (!document.isDirty) {
        return;
    }
    await document.save();
    if (document.isDirty) {
        await waitForAsync(async () => !document.isDirty, 5000, label, 25);
    }
}

/**
 * Close every editor showing a path, so a fixture delete cannot be resurrected
 * by the host writing a held buffer back out.
 */
async function closeEditorsFor(uri: vscode.Uri): Promise<void> {
    for (const editor of vscode.window.visibleTextEditors) {
        if (editor.document.uri.fsPath !== uri.fsPath) {
            continue;
        }
        await vscode.window.showTextDocument(editor.document, {
            preview: false,
            preserveFocus: false,
        });
        await vscode.commands.executeCommand('workbench.action.closeActiveEditor');
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
    // Match VirtualFileSystem's production path: a legacy join rejection
    // changes v1 -> v2, then the socket must be rebuilt before retrying. A join
    // whose acknowledgement never arrives needs the same rebuild, because the
    // stalled socket stays useless afterwards and retrying on it would only
    // wait a second time. Stalls come in runs when the deployment is loaded, so
    // a single retry is not enough to tell a busy server from a broken one.
    const attempts = 5;
    for (let attempt = 1; ; attempt++) {
        try {
            return await withTimeout(
                socket.joinProject(projectId),
                10000,
                attempt === 1 ? label : `${label} (attempt ${attempt})`,
            );
        } catch (error) {
            if (attempt >= attempts || (!socket.needsReinit && !isSocketIOTimeout(error))) {
                throw error;
            }
            socket.init();
            // Rebuilding takes microseconds, so retrying immediately spends
            // the whole budget inside one stall: four attempts back to back
            // ask a busy deployment the same question four times in the time
            // it needed to answer once. Wait between attempts so the budget
            // covers a real interval instead.
            await new Promise(resolve => setTimeout(resolve, attempt * 1000));
        }
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

/**
 * A timed-out create can have committed remotely even though its response was
 * lost. Before repeating the mutation, look up the named fixture in a fresh
 * project snapshot. This keeps E2E provisioning idempotent without weakening
 * any synchronization assertion that follows it.
 */
export async function createOrRecoverE2EEntity<T>(options: {
    label: string;
    create: () => Promise<Pick<ResponseSchema, 'type' | 'entity' | 'message'>>;
    recover: () => Promise<T | undefined>;
    attempts?: number;
    delay?: (milliseconds: number) => Promise<void>;
}): Promise<T> {
    const attempts = options.attempts ?? 3;
    const delay = options.delay ?? (milliseconds => new Promise(resolve => {
        setTimeout(resolve, milliseconds);
    }));
    let lastDetail = 'unknown error';

    for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
            const response = await options.create();
            if (response.type === 'success' && response.entity !== undefined) {
                return response.entity as T;
            }
            lastDetail = response.message ?? `${response.type} response omitted the entity`;
        } catch (error) {
            lastDetail = safeMessage(error);
        }

        await delay(attempt * 500);
        try {
            const recovered = await options.recover();
            if (recovered !== undefined) {
                return recovered;
            }
        } catch (error) {
            lastDetail = `${lastDetail}; verification failed: ${safeMessage(error)}`;
        }
    }

    throw new Error(`${options.label}: ${lastDetail}`);
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
): Promise<{entityId: string; content: string; revision: number} | undefined> {
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
            return {
                entityId: document._id,
                content: joined.docLines.join('\n'),
                revision: joined.version,
            };
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

/**
 * Model an uncoordinated filesystem editor with one writable inode ownership
 * window. LocalProjectionCas detects this handle and cannot project across it.
 */
async function appendThroughWritableHandle(
    filePath: string,
    suffix: Uint8Array,
): Promise<boolean> {
    let handle;
    try {
        handle = await open(filePath, 'r+');
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
            return false;
        }
        throw error;
    }
    try {
        const current = await handle.readFile();
        const target = Buffer.concat([current, Buffer.from(suffix)]);
        await handle.truncate(0);
        let offset = 0;
        while (offset < target.byteLength) {
            const {bytesWritten} = await handle.write(
                target,
                offset,
                target.byteLength - offset,
                offset,
            );
            assert(bytesWritten > 0, 'Raw filesystem writer made no forward progress');
            offset += bytesWritten;
        }
        await handle.sync();
        return true;
    } finally {
        await handle.close();
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

function validateServerAuthority(value: unknown): asserts value is string {
    assert(
        typeof value === 'string'
        && value.length > 0
        && value.length <= 512
        && !/[\s/?#@]/.test(value),
        'Invalid E2E server authority',
    );
    let parsed: URL;
    try {
        parsed = new URL(`http://${value}/`);
    } catch {
        throw new Error('Invalid E2E server authority');
    }
    assert(
        parsed.hostname.length > 0
        && parsed.username === ''
        && parsed.password === ''
        && parsed.pathname === '/'
        && parsed.search === ''
        && parsed.hash === ''
        && parsed.port !== '0',
        'Invalid E2E server authority',
    );
}

function validateE2ETarget<T extends {
    serverName?: unknown;
    projectName?: unknown;
    projectId?: unknown;
}>(value: T): asserts value is T & {serverName: string; projectName: string; projectId: string} {
    validateServerAuthority(value.serverName);
    assert(
        typeof value.projectName === 'string'
        && /^hershel-safe-sync-e2e(?:[-_][a-zA-Z0-9_-]+)?$/.test(value.projectName),
        'E2E request must target a dedicated E2E project',
    );
    assert(
        typeof value.projectId === 'string'
        && /^[a-zA-Z0-9_-]{1,128}$/.test(value.projectId),
        'Invalid E2E project id',
    );
}

export function e2eTargetFromBinding(value: unknown): {
    serverName: string;
    projectName: string;
    projectId: string;
} {
    assert(value !== null && typeof value === 'object',
        'E2E Local Replica binding is not an object');
    const binding = value as Partial<LocalReplicaBinding>;
    assert(typeof binding.uri === 'string' && typeof binding.serverName === 'string',
        'E2E Local Replica binding is missing its URI or server');
    const uriTarget = projectIdFromUri(binding.uri);
    const target = {
        serverName: binding.serverName,
        projectName: binding.projectName,
        projectId: uriTarget.projectId,
    };
    validateE2ETarget(target);
    assert(uriTarget.serverName === target.serverName,
        'E2E Local Replica URI does not match its bound server');
    return target;
}

export function validateRequest(value: unknown): E2ERequest {
    const request = value as Partial<E2ERequest>;
    assert(request?.schemaVersion === 3, 'Unsupported E2E request schema');
    validateE2ETarget(request);
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

export function validateRestartRecoveryMarker(
    value: unknown,
    expectedNonce?: string,
): E2ERestartRecoveryMarker {
    const marker = value as Partial<E2ERestartRecoveryMarker>;
    assert(marker?.schemaVersion === 2, 'Unsupported restart-recovery marker schema');
    assert(
        typeof marker.requestNonce === 'string'
        && /^[a-zA-Z0-9_-]{8,80}$/.test(marker.requestNonce),
        'Invalid restart-recovery nonce',
    );
    if (expectedNonce !== undefined) {
        assert(marker.requestNonce === expectedNonce,
            'Restart-recovery marker belongs to another E2E request');
    }
    assert(Number.isSafeInteger(marker.preReloadPid) && marker.preReloadPid! > 0,
        'Invalid restart-recovery owner pid');
    assert(marker.namespace === `hershel-e2e-${marker.requestNonce}`,
        'Restart-recovery namespace does not match its request');
    assert(typeof marker.runFolderId === 'string' && /^[a-zA-Z0-9_-]+$/.test(marker.runFolderId),
        'Invalid restart-recovery folder id');
    assert(typeof marker.documentId === 'string' && /^[a-zA-Z0-9_-]+$/.test(marker.documentId),
        'Invalid restart-recovery document id');
    assert(marker.relativePath === `${marker.namespace}/restart-recovery.tex`,
        'Restart-recovery path does not match its namespace');
    assert(Number.isSafeInteger(marker.baseRevision) && marker.baseRevision! >= 0,
        'Invalid restart-recovery base revision');
    for (const [label, hash] of [
        ['base', marker.baseSha256],
        ['target', marker.targetSha256],
    ] as const) {
        assert(typeof hash === 'string' && /^[a-f0-9]{64}$/.test(hash),
            `Invalid restart-recovery ${label} hash`);
    }
    assert(
        typeof marker.token === 'string'
        && marker.token.length > 0
        && marker.token.length <= 512,
        'Invalid restart-recovery token',
    );
    assert(
        typeof marker.pendingOperationId === 'string'
        && marker.pendingOperationId.length > 0
        && marker.pendingOperationId.length <= 512,
        'Invalid restart-recovery operation id',
    );
    assert(
        marker.detachedLegacyDocumentId === `hershel-e2e-detached-${marker.requestNonce}`,
        'Invalid detached legacy recovery document id',
    );
    assert(
        typeof marker.detachedLegacyOperationId === 'string'
        && marker.detachedLegacyOperationId.length > 0
        && marker.detachedLegacyOperationId.length <= 512,
        'Invalid detached legacy recovery operation id',
    );
    for (const [label, hash] of [
        ['detached base', marker.detachedLegacyBaseSha256],
        ['detached visible', marker.detachedLegacyVisibleSha256],
    ] as const) {
        assert(typeof hash === 'string' && /^[a-f0-9]{64}$/.test(hash),
            `Invalid restart-recovery ${label} hash`);
    }
    assert(typeof marker.createdAt === 'string' && !Number.isNaN(Date.parse(marker.createdAt)),
        'Invalid restart-recovery creation time');
    return marker as E2ERestartRecoveryMarker;
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
    | 'extensionId'
    | 'extensionVersion'
    | 'buildId'
    | 'extensionPath'
    | 'runnerSha256'
    | 'cachedExtensionVersion'
    | 'cachedBuildId'
    | 'cachedMetadataMatches'
> {
    const manifest = JSON.parse(readFileSync(
        path.join(context.extension.extensionPath, 'package.json'),
        'utf8',
    )) as {publisher?: unknown; name?: unknown; version?: unknown; hershelBuildId?: unknown};
    const extensionId = `${String(manifest.publisher ?? '')}.${String(manifest.name ?? '')}`;
    const extensionVersion = String(manifest.version ?? 'unknown');
    const buildId = String(manifest.hershelBuildId ?? 'missing-hershel-build-id');
    const cachedExtensionVersion = String(context.extension.packageJSON.version ?? 'unknown');
    const cachedBuildId = String(
        context.extension.packageJSON.hershelBuildId ?? 'missing-hershel-build-id',
    );
    return {
        extensionId,
        extensionVersion,
        buildId,
        extensionPath: context.extension.extensionPath,
        runnerSha256: createHash('sha256').update(readFileSync(__filename)).digest('hex'),
        cachedExtensionVersion,
        cachedBuildId,
        cachedMetadataMatches: context.extension.id === extensionId
            && cachedExtensionVersion === extensionVersion
            && cachedBuildId === buildId,
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

async function currentWorkspaceE2ETarget(): Promise<{
    serverName: string;
    projectName: string;
    projectId: string;
}> {
    const folders = vscode.workspace.workspaceFolders;
    assert(folders?.length === 1 && folders[0].uri.scheme === 'file',
        'Run E2E from one dedicated Local Replica workspace');
    const raw = await vscode.workspace.fs.readFile(
        vscode.Uri.joinPath(folders[0].uri, '.overleaf/settings.json'),
    );
    return e2eTargetFromBinding(JSON.parse(Buffer.from(raw).toString('utf8')));
}

async function workspaceTargetsApprovedReplica(request: E2ERequest): Promise<boolean> {
    try {
        const target = await currentWorkspaceE2ETarget();
        return target.serverName === request.serverName
            && target.projectName === request.projectName
            && target.projectId === request.projectId;
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
    const requestUri = vscode.Uri.joinPath(context.globalStorageUri, REQUEST_FILE);
    const runningUri = vscode.Uri.joinPath(context.globalStorageUri, RUNNING_FILE);
    const queued = await readJson<unknown>(requestUri);
    const runningValue = await readJson<unknown>(runningUri);
    let request: E2ERequest | undefined;
    try {
        request = queued === undefined
            ? validateRunningMarker(runningValue).request
            : validateRequest(queued);
    } catch {
        // The marker is refused below without mutating either request file.
    }
    if (
        marker.schemaVersion !== 1
        || marker.targetExtensionVersion !== HANDOFF_TARGET_VERSION
        || marker.targetBuildId !== HANDOFF_TARGET_BUILD_ID
        || !request
        || !await workspaceTargetsApprovedReplica(request)
    ) {
        output.appendLine('[warn] Refused an invalid or out-of-scope E2E reload handoff.');
        return false;
    }
    const root = vscode.workspace.workspaceFolders![0].uri;
    const raw = await vscode.workspace.fs.readFile(vscode.Uri.joinPath(root, '.overleaf/settings.json'));
    const binding = JSON.parse(Buffer.from(raw).toString('utf8')) as LocalReplicaBinding;
    assertLocalReplicaBinding(binding, request.serverName, request.projectId);
    assert(binding.projectName === request.projectName,
        'E2E reload binding project name does not match its request');
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
    request: E2ERequest,
    preservedNamespace?: string,
): Promise<{root: vscode.Uri; stateStore: ReplicaStateStore}> {
    const folders = vscode.workspace.workspaceFolders;
    assert(folders?.length === 1 && folders[0].uri.scheme === 'file',
        'Run the full E2E from the single-folder local replica workspace');
    const root = folders[0].uri;
    const raw = await vscode.workspace.fs.readFile(vscode.Uri.joinPath(root, '.overleaf/settings.json'));
    const binding = JSON.parse(Buffer.from(raw).toString('utf8')) as LocalReplicaBinding;
    assertLocalReplicaBinding(binding, request.serverName, request.projectId);
    assert(binding.projectName === request.projectName,
        'Local replica project name does not match the approved E2E project');
    const stateStore = new ReplicaStateStore(
        context.globalStorageUri.fsPath,
        request.serverName,
        request.projectId,
        root.fsPath,
    );
    // The provider already names every condition it is still waiting on. Carry
    // the last set into the timeout: "did not settle" alone cannot tell a lost
    // lock from a reconcile queue that never drains.
    let lastSettledBlockers: string[] = ['no runtime status'];
    await waitForAsync(async () => {
        const runtime = LocalReplicaSCMProvider.getRuntimeStatus(root.fsPath);
        if (runtime?.paused) {
            throw new Error('The approved Local Replica provider is paused');
        }
        if (runtime) {
            const reconcile = runtime.activeReconcile;
            lastSettledBlockers = [
                ...runtime.settledBlockers,
                ...runtime.pendingAction ? ['pending-action'] : [],
                ...reconcile
                    ? [`reconcile(${reconcile.reason}/${reconcile.phase}#${reconcile.iteration})`]
                    : [],
            ];
        }
        return runtime?.settled === true
            && runtime.pendingAction === false;
    }, 60000, () => 'wait for the Local Replica watcher, lock, and settled reconciliation queue'
        + ` (blocked on ${lastSettledBlockers.join(', ') || 'nothing'})`);
    let state = await stateStore.reload();
    const staleConflictPaths = Object.keys(state.conflicts).filter(filePath => (
        isE2EConflictPath(filePath)
        && (
            preservedNamespace === undefined
            || (filePath !== preservedNamespace && !filePath.startsWith(`${preservedNamespace}/`))
        )
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
        !Object.keys(state.conflicts).some(filePath => (
            isE2EConflictPath(filePath)
            && (
                preservedNamespace === undefined
                || (filePath !== preservedNamespace && !filePath.startsWith(`${preservedNamespace}/`))
            )
        )),
        'The approved local replica has stale E2E conflicts',
    );
    return {root, stateStore};
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
    let projectId: string | undefined = request.projectId;
    let runFolderId: string | undefined;
    let api: BaseAPI | undefined;
    let identity: Identity | undefined;
    let serverUrl: string | undefined;
    let localRoot: vscode.Uri | undefined;
    let replicaStateStore: ReplicaStateStore | undefined;
    let runName: string | undefined;
    let restartRecoveryMarker: E2ERestartRecoveryMarker | undefined;
    const restartRecoveryMarkerUri = vscode.Uri.joinPath(
        context.globalStorageUri,
        RESTART_RECOVERY_FILE,
    );
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
        // A connection belongs to the step that opened it: no step reads
        // another step's socket or journal. Overleaf counts live joins per
        // project, and a run this long would otherwise reach its final steps
        // holding every connection it ever opened, at which point the server
        // stops acknowledging new joins at all.
        const socketsBefore = sockets.length;
        const sessionsBefore = sessions.length;
        const releaseStepConnections = async (): Promise<void> => {
            for (const session of sessions.splice(sessionsBefore)) {
                await session.close().catch(error => {
                    output.appendLine(`[warn] E2E journal close failed: ${safeMessage(error)}`);
                });
            }
            for (const socket of sockets.splice(socketsBefore)) {
                try {
                    socket.retire();
                } catch {
                    // Best-effort connection cleanup.
                }
            }
        };
        try {
            // Composite mutation steps must settle before cleanup begins. Leaf
            // socket reads and bounded polling carry their own timeouts.
            const value = await action();
            const entry = {name, ok: true, durationMs: Date.now() - started};
            steps.push(entry);
            output.appendLine(`[pass] ${name} (${entry.durationMs} ms)`);
            await releaseStepConnections();
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
            await releaseStepConnections();
            throw error;
        }
    };

    const cleanupFixtures = async (
        recordEvidence = true,
        namespace = runName,
    ): Promise<void> => {
        assert(api && identity && projectId && serverUrl && namespace && localRoot,
            'Fixture cleanup was requested before fixture creation');
        // A failed streaming assertion can leave its isolated editor dirty.
        // Revert and close only documents under the approved E2E namespace;
        // otherwise deleting the directory lets the open buffer recreate it.
        const namespacePrefix = `${namespace}${path.sep}`;
        const openFixtures = vscode.workspace.textDocuments.filter(document => {
            if (document.uri.scheme !== 'file') {
                return false;
            }
            const relative = path.relative(localRoot!.fsPath, document.uri.fsPath);
            return relative === namespace || relative.startsWith(namespacePrefix);
        });
        for (const document of openFixtures) {
            await vscode.window.showTextDocument(document, {preview: false, preserveFocus: false});
            if (document.isDirty) {
                await vscode.commands.executeCommand('workbench.action.revertAndCloseActiveEditor');
            } else {
                await vscode.commands.executeCommand('workbench.action.closeActiveEditor');
            }
        }
        assert(
            !vscode.workspace.textDocuments.some(document => {
                const relative = document.uri.scheme === 'file'
                    ? path.relative(localRoot!.fsPath, document.uri.fsPath)
                    : '';
                return document.isDirty
                    && (relative === namespace || relative.startsWith(namespacePrefix));
            }),
            `Dirty E2E editor remained open during cleanup: ${namespace}`,
        );
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

    const commitRemoteSnapshot = async (
        remoteDocumentId: string,
        target: string,
        label: string,
    ): Promise<void> => {
        assert(serverUrl && api && identity && projectId,
            'Remote E2E edit was requested before authentication');
        const socket = new SocketIOAPI(serverUrl, api, identity, projectId);
        let session: DurableDocumentSession | undefined;
        const receiveErrors: Error[] = [];
        const sessionErrors: Error[] = [];
        socket.updateEventHandlers({
            onFileChanged: update => {
                session?.receiveUpdate(update).catch(error => receiveErrors.push(error));
            },
        });
        try {
            await joinProjectWithProtocolFallback(socket, projectId, `join ${label}`);
            await waitFor(
                () => socket.sourceId !== undefined,
                5000,
                `receive ${label} source id`,
            );
            const joined = await withTimeout(
                socket.joinDoc(remoteDocumentId),
                10000,
                `join ${label} document`,
            );
            const base = joined.docLines.join('\n');
            const journal = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${remoteDocumentId}-${label}-${randomUUID()}`,
                initialRevision: joined.version,
                initialContent: base,
                initialPath: `e2e/${remoteDocumentId}.tex`,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            session = await DurableDocumentSession.create({
                documentId: remoteDocumentId,
                authoritativeContent: base,
                authoritativeRevision: joined.version,
                journal,
                sendUpdate: update => socket.applyOtUpdate(
                    remoteDocumentId,
                    update as UpdateSchema,
                ),
                getSourceId: () => socket.sourceId,
                createOperationId: () => `e2e:${request.nonce}:${label}:${randomUUID()}`,
                onChange: () => {},
                onError: error => sessionErrors.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            await withTimeout(
                session.submitSnapshot(target),
                20000,
                `acknowledge ${label} snapshot`,
            );
            assert(sessionErrors.length === 0 && receiveErrors.length === 0,
                `${label} remote editor entered fail-closed state`);
            await session.close();
            session = undefined;
            await socket.leaveDoc(remoteDocumentId);
        } finally {
            session?.dispose();
            socket.disconnect();
        }
    };

    try {
        await step('exact loaded extension build identity', async () => {
            assertExpectedBuild(request, extension);
            assert(
                extension.cachedMetadataMatches,
                'VS Code contribution cache does not match the loaded extension manifest',
            );
        });
        const server = await step('approved target and SecretStorage login', async () => {
            const match = GlobalStateManager.getServers(context)
                .find(item => item.server.name === request.serverName);
            assert(match, `Server ${request.serverName} is not configured`);
            const url = new URL(match.server.url);
            assert(url.host === request.serverName,
                'Configured server URL does not match the requested E2E target');
            identity = await GlobalStateManager.authenticate(context, request.serverName);
            api = match.api;
            serverUrl = match.server.url;
            return match;
        });

        await step('verify retained E2E project binding', async () => {
            const projects = await GlobalStateManager.fetchServerProjects(
                context,
                api!,
                request.serverName,
            );
            const project = projects.find(item => item.id === request.projectId);
            assert(project && !project.trashed,
                'The bound E2E project is absent or trashed on the requested server');
            assert(project.name === request.projectName,
                'The bound E2E project name differs from the requested target');
        });

        await step('approved Local Replica binding and clean baseline', async () => {
            const markerValue = await readJson<unknown>(restartRecoveryMarkerUri);
            restartRecoveryMarker = markerValue === undefined
                ? undefined
                : validateRestartRecoveryMarker(markerValue, request.nonce);
            const approved = await approvedLocalReplica(
                context,
                request,
                restartRecoveryMarker?.namespace,
            );
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
        let restartDocumentId!: string;
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
                // An interrupted run can leave a paused conflict inside its
                // namespace, and a conflicted path is not cleanable. The remote
                // variant is the one this harness created, so adopt it and let
                // the namespace be removed with everything else.
                const stale = await replicaStateStore!.reload();
                for (const conflicted of Object.keys(stale.conflicts)) {
                    if (
                        conflicted === staleName
                        || conflicted.startsWith(`${staleName}/`)
                    ) {
                        await LocalReplicaSCMProvider.resolveE2EConflictUsingRemote(
                            localRoot!.fsPath,
                            conflicted,
                        );
                    }
                }
                await cleanupFixtures(false, staleName);
            }
            const staleState = await replicaStateStore!.reload();
            for (const documentId of Object.keys(staleState.detachedDocumentRecoveries ?? {})) {
                if (
                    documentId.startsWith('hershel-e2e-detached-')
                    && documentId !== restartRecoveryMarker?.detachedLegacyDocumentId
                ) {
                    await LocalReplicaSCMProvider.cleanupE2EDetachedRecovery(
                        localRoot!.fsPath,
                        documentId,
                    );
                }
            }
            if (!restartRecoveryMarker) {
                await cleanupE2EConflictArtifacts(localRoot!);
                assert((await findE2EConflictArtifactDirectories(localRoot!)).length === 0,
                    'Stale E2E conflict artifacts remain after cleanup');
            } else {
                assert(
                    (await findE2EConflictArtifactDirectories(
                        localRoot!, restartRecoveryMarker.namespace,
                    )).length === 0,
                    'Restart-recovery fixture entered conflict isolation before resume proof',
                );
            }
            const reclaimed = await LocalReplicaSCMProvider
                .reclaimE2ERetainedRemoteTransactions(
                    localRoot!.fsPath,
                    projectId!,
                    request.projectName,
                );
            if (reclaimed) {
                output.appendLine(
                    '[maintenance] reclaimed old safe-sync retention transactions for E2E capacity',
                );
            }
        });

        await step('remove stale fixture namespace for this nonce', async () => {
            if (!restartRecoveryMarker) {
                await cleanupFixtures(false);
            }
        });

        await step('create or recover isolated text, binary, and move fixtures', async () => {
            const bootstrap = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(bootstrap);
            const project = await joinProjectWithProtocolFallback(
                bootstrap,
                projectId!,
                'join E2E project',
            );
            assert(project.name === request.projectName,
                'Joined project name does not match the approved target');
            const root = project.rootFolder[0];
            assert(root?._id, 'E2E project has no root folder');

            const createFolder = async (
                parentFolderId: string,
                name: string,
                label: string,
            ): Promise<FolderEntity> => createOrRecoverE2EEntity({
                label,
                create: () => api!.addFolder(identity!, projectId!, name, parentFolderId),
                recover: async () => {
                    const current = await freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        `recover ${label}`,
                    );
                    return findFolder(current.rootFolder[0], parentFolderId)
                        ?.folders.find(folder => folder.name === name);
                },
            });
            const createDocument = async (
                parentFolderId: string,
                name: string,
                label: string,
            ) => createOrRecoverE2EEntity({
                label,
                create: () => api!.addDoc(identity!, projectId!, parentFolderId, name),
                recover: async () => {
                    const current = await freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        `recover ${label}`,
                    );
                    return findFolder(current.rootFolder[0], parentFolderId)
                        ?.docs.find(document => document.name === name);
                },
            });
            const uploadBinary = async (
                parentFolderId: string,
                name: string,
                content: Uint8Array,
                label: string,
            ) => createOrRecoverE2EEntity({
                label,
                create: () => api!.uploadFile(
                    identity!, projectId!, parentFolderId, name, content,
                ),
                recover: async () => {
                    const current = await freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        `recover ${label}`,
                    );
                    return findFolder(current.rootFolder[0], parentFolderId)
                        ?.fileRefs.find(file => file.name === name);
                },
            });

            if (restartRecoveryMarker) {
                const existing = root.folders.find(folder => folder.name === isolatedRunName);
                assert(existing, 'Restart-recovery fixture namespace disappeared across reload');
                assert(existing._id === restartRecoveryMarker.runFolderId,
                    'Restart-recovery fixture namespace changed identity across reload');
                runFolder = existing;
                runFolderId = existing._id;
                const concurrent = existing.docs.find(item => item.name === 'concurrent.tex');
                const streaming = existing.docs.find(item => item.name === 'streaming.tex');
                const restart = existing.docs.find(item => item.name === 'restart-recovery.tex');
                const binary = existing.fileRefs.find(item => item.name === 'fixture.png');
                folderA = existing.folders.find(item => item.name === 'move-a')!;
                folderB = existing.folders.find(item => item.name === 'move-b')!;
                const moved = folderA?.docs.find(item => item.name === 'before-move.tex');
                assert(concurrent && streaming && restart && binary && folderA && folderB && moved,
                    'Restart-recovery fixture set is incomplete after reload');
                assert(restart._id === restartRecoveryMarker.documentId,
                    'Restart-recovery document changed immutable identity across reload');
                documentId = concurrent._id;
                streamingDocumentId = streaming._id;
                restartDocumentId = restart._id;
                binaryId = binary._id;
                movedDocumentId = moved._id;
                return;
            }

            runFolder = await createFolder(root._id, isolatedRunName, 'Could not create E2E run folder');
            runFolderId = runFolder._id;

            const document = await createDocument(
                runFolder._id, 'concurrent.tex', 'Could not create E2E document',
            );
            documentId = document._id;

            const streaming = await createDocument(
                runFolder._id, 'streaming.tex', 'Could not create streaming editor document',
            );
            streamingDocumentId = streaming._id;

            const restart = await createDocument(
                runFolder._id, 'restart-recovery.tex', 'Could not create restart-recovery document',
            );
            restartDocumentId = restart._id;

            const uploaded = await uploadBinary(
                runFolder._id, 'fixture.png', BINARY_FIXTURE, 'Could not upload E2E binary fixture',
            );
            binaryId = uploaded._id;

            folderA = await createFolder(runFolder._id, 'move-a', 'Could not create move source folder');
            folderB = await createFolder(runFolder._id, 'move-b', 'Could not create move target folder');
            const moved = await createDocument(
                folderA._id, 'before-move.tex', 'Could not create move fixture',
            );
            movedDocumentId = moved._id;
        });

        await step('remote text and binary fixtures reach Local Replica', async () => {
            const localDocument = vscode.Uri.joinPath(localRoot!, isolatedRunName, 'concurrent.tex');
            const localStreamingDocument = vscode.Uri.joinPath(
                localRoot!, isolatedRunName, 'streaming.tex',
            );
            const localRestartDocument = vscode.Uri.joinPath(
                localRoot!, isolatedRunName, 'restart-recovery.tex',
            );
            const localBinary = vscode.Uri.joinPath(localRoot!, isolatedRunName, 'fixture.png');
            const restartExpected = restartRecoveryMarker
                ? `RESTART-BASE-${request.nonce}\n${restartRecoveryMarker.token}`
                : '';
            await waitForAsync(async () => {
                const [text, streaming, restart, binary] = await Promise.all([
                    readLocalBytes(localDocument),
                    readLocalBytes(localStreamingDocument),
                    readLocalBytes(localRestartDocument),
                    readLocalBytes(localBinary),
                ]);
                return text !== undefined
                    && Buffer.from(text).toString('utf8') === ''
                    && streaming !== undefined
                    && Buffer.from(streaming).toString('utf8') === ''
                    && restart !== undefined
                    && Buffer.from(restart).toString('utf8') === restartExpected
                    && binary !== undefined
                    && Buffer.from(binary).equals(Buffer.from(BINARY_FIXTURE));
            }, 30000, 'pull remote fixtures into Local Replica');
        });

        await step('real reload settles the same journaled operation exactly once', async () => {
            const fileName = 'restart-recovery.tex';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);
            const base = `RESTART-BASE-${request.nonce}\n`;
            const baseHash = createHash('sha256').update(base).digest('hex');

            if (!restartRecoveryMarker) {
                await commitRemoteSnapshot(restartDocumentId, base, 'restart-recovery-base');
                await waitForAsync(async () => {
                    const [local, state] = await Promise.all([
                        readLocalBytes(localUri),
                        replicaStateStore!.reload(),
                    ]);
                    return local !== undefined
                        && Buffer.from(local).toString('utf8') === base
                        && state.entries[relativePath]?.entityId === restartDocumentId
                        && state.entries[relativePath]?.hash === baseHash
                        && !state.pendingAction
                        && !state.conflicts[relativePath];
                }, 30000, 'establish restart-recovery common base', 100);

                const document = await vscode.workspace.openTextDocument(localUri);
                await vscode.window.showTextDocument(document, {
                    preview: false,
                    preserveFocus: false,
                });
                let baseInspection: Awaited<ReturnType<
                    typeof LocalReplicaSCMProvider.inspectE2EDocumentRecovery
                >>;
                await waitForAsync(async () => {
                    baseInspection = await LocalReplicaSCMProvider.inspectE2EDocumentRecovery(
                        localRoot!.fsPath,
                        relativePath,
                    );
                    return baseInspection?.entityId === restartDocumentId
                        && baseInspection.revision >= 0
                        && baseInspection.visibleContent === base
                        && baseInspection.acknowledgedContent === base
                        && baseInspection.lastKnownPath === relativePath
                        && baseInspection.pendingOperationIds.length === 0;
                }, 15000, 'attach restart-recovery WAL owner', 50);
                assert(baseInspection, 'Restart-recovery WAL owner did not attach');
                await LocalReplicaSCMProvider.suspendE2EDocumentTransport(
                    localRoot!.fsPath,
                    relativePath,
                );

                const token = `RESTART-JOURNALED-${request.nonce}\n`;
                const target = `${base}${token}`;
                const targetHash = createHash('sha256').update(target).digest('hex');
                const edit = new vscode.WorkspaceEdit();
                edit.insert(localUri, document.positionAt(document.getText().length), token);
                assert(await vscode.workspace.applyEdit(edit),
                    'Could not create the pre-reload journaled edit');
                let pendingInspection: Awaited<ReturnType<
                    typeof LocalReplicaSCMProvider.inspectE2EDocumentRecovery
                >>;
                let lastPendingObservation: Record<string, unknown> = {};
                try {
                    await waitForAsync(async () => {
                        const local = await readLocalBytes(localUri);
                        pendingInspection = await LocalReplicaSCMProvider.inspectE2EDocumentRecovery(
                            localRoot!.fsPath,
                            relativePath,
                        );
                        lastPendingObservation = {
                            editorDirty: document.isDirty,
                            editorMatchesTarget: document.getText() === target,
                            diskMatchesTarget: local !== undefined
                                && Buffer.from(local).toString('utf8') === target,
                            entityMatches: pendingInspection?.entityId === restartDocumentId,
                            revision: pendingInspection?.revision,
                            expectedRevision: baseInspection!.revision,
                            visibleMatchesTarget: pendingInspection?.visibleContent === target,
                            acknowledgedMatchesBase: pendingInspection?.acknowledgedContent === base,
                            lastKnownPath: pendingInspection?.lastKnownPath,
                            pendingOperationCount: pendingInspection?.pendingOperationIds.length,
                        };
                        // Auto Save may make the editor clean after the local
                        // operation is journaled. Recovery correctness depends
                        // on the WAL candidate, path, revision, and opId—not on
                        // the transient VS Code dirty indicator.
                        return document.getText() === target
                            && local !== undefined
                            && Buffer.from(local).toString('utf8') === target
                            && pendingInspection?.entityId === restartDocumentId
                            && pendingInspection.revision === baseInspection!.revision
                            && pendingInspection.visibleContent === target
                            && pendingInspection.acknowledgedContent === base
                            && pendingInspection.lastKnownPath === relativePath
                            && pendingInspection.pendingOperationIds.length === 1;
                    }, 15000, 'durably journal the pre-reload operation', 50);
                } catch (error) {
                    throw new Error(
                        `${safeMessage(error)}; last observation: `
                        + JSON.stringify(lastPendingObservation),
                    );
                }
                assert(pendingInspection, 'Restart-recovery pending operation was not inspectable');
                const pendingOperationId = pendingInspection.pendingOperationIds[0];
                const operationInspection = await LocalReplicaSCMProvider
                    .inspectE2EDocumentRecovery(
                        localRoot!.fsPath,
                        relativePath,
                        pendingOperationId,
                    );
                assert(operationInspection?.operationDisposition === 'pending',
                    'Pre-reload operation was not durably pending');

                const detachedLegacyDocumentId = `hershel-e2e-detached-${request.nonce}`;
                const detachedLegacyOperationId = `detached-legacy:${request.nonce}`;
                const detachedLegacyBase = `DETACHED-LEGACY-BASE-${request.nonce}\n`;
                const detachedLegacyVisible = `${detachedLegacyBase}`
                    + `DETACHED-LEGACY-LOCAL-${request.nonce}\n`;
                const detachedState = new OtClientState(0, detachedLegacyBase);
                detachedState.submitOperation(
                    TextOperation.fromDiff(detachedLegacyBase, detachedLegacyVisible),
                    detachedLegacyOperationId,
                );
                await new DocumentStateStore(
                    context,
                    request.serverName,
                    projectId!,
                ).save(detachedLegacyDocumentId, detachedState.serialize());

                restartRecoveryMarker = {
                    schemaVersion: 2,
                    requestNonce: request.nonce,
                    preReloadPid: process.pid,
                    namespace: isolatedRunName,
                    runFolderId: runFolderId!,
                    documentId: restartDocumentId,
                    relativePath,
                    baseRevision: baseInspection.revision,
                    baseSha256: baseHash,
                    targetSha256: targetHash,
                    token,
                    pendingOperationId,
                    detachedLegacyDocumentId,
                    detachedLegacyOperationId,
                    detachedLegacyBaseSha256: createHash('sha256')
                        .update(detachedLegacyBase).digest('hex'),
                    detachedLegacyVisibleSha256: createHash('sha256')
                        .update(detachedLegacyVisible).digest('hex'),
                    createdAt: new Date().toISOString(),
                };
                await writeFileDurably(
                    restartRecoveryMarkerUri.fsPath,
                    Buffer.from(`${JSON.stringify(restartRecoveryMarker, null, 2)}\n`, 'utf8'),
                );
                output.appendLine(
                    `[reload] operation ${pendingOperationId} is durable at revision `
                    + `${baseInspection.revision}; reloading the extension host.`,
                );
                await vscode.commands.executeCommand('workbench.action.reloadWindow');
                await new Promise<never>(() => {
                    // The old extension host must not publish a result. Startup
                    // resumes this same nonce and fixture from the durable marker.
                });
            }

            const marker = restartRecoveryMarker;
            assert(marker, 'Restart-recovery marker disappeared before resume proof');
            assert(process.pid !== marker.preReloadPid,
                'Restart-recovery proof did not enter a new extension-host process');
            assert(marker.documentId === restartDocumentId && marker.runFolderId === runFolderId,
                'Restart-recovery fixture identity changed across reload');
            assert(marker.token === `RESTART-JOURNALED-${request.nonce}\n`,
                'Restart-recovery token changed across reload');
            assert(marker.pendingOperationId.startsWith(`${projectId}:${marker.documentId}:`),
                'Restart-recovery operation id does not belong to the same project and document');
            const target = `${base}${marker.token}`;
            assert(createHash('sha256').update(base).digest('hex') === marker.baseSha256,
                'Restart-recovery common base changed across reload');
            assert(createHash('sha256').update(target).digest('hex') === marker.targetSha256,
                'Restart-recovery target changed across reload');
            assert(
                isExactComposition(target, [base, marker.token])
                && countExactOccurrences(target, marker.token) === 1,
                'Restart-recovery target does not contain the base and token exactly once',
            );

            const document = await vscode.workspace.openTextDocument(localUri);
            let finalRevision = -1;
            await waitForAsync(async () => {
                const [inspection, remote, local, state] = await Promise.all([
                    LocalReplicaSCMProvider.inspectE2EDocumentRecovery(
                        localRoot!.fsPath,
                        relativePath,
                        marker.pendingOperationId,
                    ),
                    freshDocument(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        runFolderId!,
                        fileName,
                    ),
                    readLocalBytes(localUri),
                    replicaStateStore!.reload(),
                ]);
                if (!inspection || !remote || !local) {
                    return false;
                }
                finalRevision = inspection.revision;
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return inspection.entityId === marker.documentId
                    && inspection.operationDisposition === 'settled'
                    && !inspection.pendingOperationIds.includes(marker.pendingOperationId)
                    && inspection.revision === marker.baseRevision + 1
                    && inspection.visibleContent === target
                    && inspection.acknowledgedContent === target
                    && remote.entityId === marker.documentId
                    && remote.revision === marker.baseRevision + 1
                    && remote.content === target
                    && Buffer.from(local).toString('utf8') === target
                    && document.getText() === target
                    && state.entries[relativePath]?.entityId === marker.documentId
                    && state.entries[relativePath]?.hash === marker.targetSha256
                    && !state.pendingAction
                    && !state.conflicts[relativePath]
                    && runtime?.settled === true;
            }, 60000, 'settle the same operation after real extension-host reload', 100);

            let detachedRecovery: DetachedDocumentRecovery | undefined;
            await waitForAsync(async () => {
                const state = await replicaStateStore!.reload();
                detachedRecovery = state.detachedDocumentRecoveries?.[
                    marker.detachedLegacyDocumentId
                ]!;
                return detachedRecovery?.walRetired === true
                    && detachedRecovery.pendingOperationIds.includes(
                        marker.detachedLegacyOperationId,
                    );
            }, 30000, 'archive and retire a pathless legacy WAL without project pause', 100);
            assert(detachedRecovery!, 'Detached legacy recovery record was not persisted');
            const [detachedBase, detachedVisible] = await Promise.all([
                readLocalBytes(vscode.Uri.joinPath(
                    localRoot!,
                    detachedRecovery!.acknowledgedFile,
                )),
                readLocalBytes(vscode.Uri.joinPath(
                    localRoot!,
                    detachedRecovery!.visibleFile,
                )),
            ]);
            assert(detachedBase && detachedVisible,
                'Detached legacy recovery artifacts are incomplete');
            assert(createHash('sha256').update(detachedBase).digest('hex')
                === marker.detachedLegacyBaseSha256,
            'Detached legacy base artifact changed');
            assert(createHash('sha256').update(detachedVisible).digest('hex')
                === marker.detachedLegacyVisibleSha256,
            'Detached legacy local artifact changed');
            const detachedRuntime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
            assert(detachedRuntime && !detachedRuntime.paused,
                'Pathless legacy WAL paused the Local Replica project');
            assert(detachedRuntime.projectPauseEvents === 0,
                'Pathless legacy WAL emitted a project-level pause');
            evidence.detachedLegacyRecovery = {
                documentId: marker.detachedLegacyDocumentId,
                operationId: marker.detachedLegacyOperationId,
                artifactDirectory: detachedRecovery!.artifactDirectory,
                baseSha256: marker.detachedLegacyBaseSha256,
                visibleSha256: marker.detachedLegacyVisibleSha256,
                walRetired: detachedRecovery!.walRetired,
                projectPauseEvents: detachedRuntime.projectPauseEvents,
            };
            await LocalReplicaSCMProvider.cleanupE2EDetachedRecovery(
                localRoot!.fsPath,
                marker.detachedLegacyDocumentId,
            );

            await removeDisk(restartRecoveryMarkerUri.fsPath, {force: true});
            await syncDirectoryDurably(path.dirname(restartRecoveryMarkerUri.fsPath));
            evidence.restartRecovery = {
                path: relativePath,
                entityId: marker.documentId,
                operationId: marker.pendingOperationId,
                preReloadPid: marker.preReloadPid,
                postReloadPid: process.pid,
                baseRevision: marker.baseRevision,
                finalRevision,
                sha256: marker.targetSha256,
            };
            restartRecoveryMarker = undefined;
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
                initialPath: relativePath,
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
            const exactOverlapResults = new Set([
                `${localToken}${remoteToken}${localContent}`,
                `${remoteToken}${localToken}${localContent}`,
            ]);
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
                    && exactOverlapResults.has(convergedContent)
                    && isExactComposition(
                        convergedContent,
                        [localContent, localToken, remoteToken],
                    );
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
            const exactLateAttachResults = new Set([
                `${lateBase}${lateLocalToken}${lateRemoteToken}`,
                `${lateBase}${lateRemoteToken}${lateLocalToken}`,
            ]);
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
                    && exactLateAttachResults.has(convergedContent)
                    && isExactComposition(
                        convergedContent,
                        [lateBase, lateLocalToken, lateRemoteToken],
                    )
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
            const exactWriterOverlap = `${editorToken}${base}${diskToken}`;
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
                    && converged === exactWriterOverlap
                    && isExactComposition(converged, [base, editorToken, diskToken])
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
            await saveDocument(document, 'Could not save the converged writer-overlap fixture');
            assert(!document.isDirty, 'Writer-overlap fixture remained dirty before cleanup');
            // Close the buffer before deleting the file. A held editor turns a
            // delete into a dirty buffer whose file is gone, and the host's
            // auto-save then recreates it: that is a delete racing a create,
            // not the unambiguous fixture delete this assertion is about.
            await closeEditorsFor(localUri);
            await vscode.workspace.fs.delete(localUri);
            try {
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
            } catch (error) {
                output.appendLine(
                    `[diagnostic] writer-overlap delete trace `
                    + `${JSON.stringify(LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                        localRoot!.fsPath,
                        relativePath,
                    ).slice(-40).map(event => ({
                        sequence: event.sequence,
                        elapsedMs: event.elapsedMs,
                        kind: event.kind,
                        reason: event.reason,
                        canonicalLength: event.canonicalBefore?.length,
                        editorLength: event.editor?.length,
                        diskLength: event.diskBefore?.length,
                        observedLength: event.observed?.length,
                    })))}`,
                );
                throw error;
            }
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
            const heldRemoteUpdates: UpdateSchema[] = [];
            let holdRemoteUpdates = false;
            socket.updateEventHandlers({
                onFileChanged: update => {
                    if (holdRemoteUpdates) {
                        heldRemoteUpdates.push(update);
                        return;
                    }
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
                initialPath: relativePath,
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
            const runtimeAtStreamStart = LocalReplicaSCMProvider.getRuntimeStatus(
                localRoot!.fsPath,
            );
            assert(runtimeAtStreamStart, 'Streaming test has no active Local Replica owner');
            const initialIsolationEvents = runtimeAtStreamStart.textIsolationEvents;
            const initialPauseEvents = runtimeAtStreamStart.projectPauseEvents;
            const remoteAdmissionToObservationLatencies: number[] = [];
            const remotePullLatencies: number[] = [];
            const remoteSubmissionTimes = new Map<string, number>();
            const remoteAcknowledgementTimes = new Map<string, number>();
            const remoteObservationTimes = new Map<string, number>();
            const observedRemoteTokens = new Set<string>();
            let remoteWriterFinished = false;
            let localWriterFinished = false;
            let remoteObservedBeforeLocalStop = false;
            // Every streaming token is written exactly once by exactly one
            // writer, so a second copy in any endpoint is a defect the moment
            // it appears. Installed once the token set exists; it reports the
            // operation that duplicated the text while that operation is still
            // inside the diagnostic ring, instead of a converged wrong file.
            let assertNoDuplicatedStreamingTokens: (() => void) | undefined;
            const assertStreamingRuntimeHealthy = (): void => {
                assertNoDuplicatedStreamingTokens?.();
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                assert(runtime && !runtime.paused, 'Streaming test paused the Local Replica owner');
                assert(
                    runtime.textIsolationEvents === initialIsolationEvents,
                    'Streaming test transiently isolated a text document',
                );
                assert(
                    runtime.projectPauseEvents === initialPauseEvents,
                    'Streaming test transiently paused the project',
                );
            };
            // Keep the batching probe inside the extension host so its edit
            // spacing is controlled. `executeCommand('type')` crosses the
            // remote workbench IPC boundary, where one requested burst can be
            // delivered in arbitrarily delayed waves; the concurrent stream
            // below still exercises that real workbench path.
            const batchedLocalTokens = ['~', '!', '@', '#'];
            const rangeKeepA = `RANGE-KEEP-A-${request.nonce}\n`;
            const rangeReplaceSource = `RANGE-REPLACE-SOURCE-${request.nonce}\n`;
            const rangeReplacement = `RANGE-REPLACED-${request.nonce}\n`;
            const rangeDeleteSource = `RANGE-DELETE-SOURCE-${request.nonce}\n`;
            const rangeKeepB = `RANGE-KEEP-B-${request.nonce}\n`;
            const rangeSeed = `${rangeKeepA}${rangeReplaceSource}${rangeDeleteSource}${rangeKeepB}`;
            const finalRangeSeed = `${rangeKeepA}${rangeReplacement}${rangeKeepB}`;
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
            const rawFilesystemTokens = Array.from(
                {length: 12},
                (_, index) => `RAW-FILESYSTEM-${request.nonce}-${index}\n`,
            );
            const expectedTokens = [
                rangeKeepA,
                rangeReplacement,
                rangeKeepB,
                ...batchedLocalTokens,
                ...localTokens,
                ...remoteTokens,
                ...aiTokens,
                ...rawFilesystemTokens,
            ];
            let expectedUtf16Length = finalRangeSeed.length + [
                ...batchedLocalTokens,
                ...localTokens,
                ...remoteTokens,
                ...aiTokens,
                ...rawFilesystemTokens,
            ].reduce(
                (total, token) => total + token.length,
                0,
            );
            const traceTokenDelta = (
                before: string | undefined,
                after: string | undefined,
            ): Array<[number, number]> | undefined => {
                if (before === undefined || after === undefined) {
                    return undefined;
                }
                const beforeCounts = expectedTokens.map(
                    token => countExactOccurrences(before, token),
                );
                return expectedTokens.flatMap((token, index) => {
                    const delta = countExactOccurrences(after, token) - beforeCounts[index];
                    return delta === 0 ? [] : [[index, delta] as [number, number]];
                });
            };
            const traceFingerprint = (content: string | undefined): string | undefined => (
                content === undefined
                    ? undefined
                    : `${content.length}:${createHash('sha256').update(content).digest('hex').slice(0, 12)}`
            );
            const summarizeCausalTrace = (
                limit = 48,
            ): Array<Record<string, unknown>> => (
                LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                    localRoot!.fsPath,
                    relativePath,
                ).slice(-limit).map(event => ({
                    sequence: event.sequence,
                    elapsedMs: event.elapsedMs,
                    kind: event.kind,
                    reason: event.reason,
                    bufferedInput: event.bufferedInput,
                    diskMutation: event.diskMutation,
                    pendingEditorObservations: event.pendingEditorObservations,
                    classification: event.classification,
                    ancestorAccountedFor: event.ancestorAccountedFor,
                    editorDirty: event.editorDirty,
                    projectionId: event.projectionId?.slice(0, 8),
                    revision: event.revision,
                    canonicalBefore: traceFingerprint(event.canonicalBefore),
                    canonicalAfter: traceFingerprint(event.canonicalAfter),
                    diskBefore: traceFingerprint(event.diskBefore),
                    observed: traceFingerprint(event.observed),
                    // Token deltas alone cannot say which state an event acted
                    // on: two states that differ by text outside the expected
                    // token set summarize identically. Carry the editor head
                    // and the endpoints of every projection and commit as
                    // fingerprints so a trace can be followed state by state.
                    editor: traceFingerprint(event.editor),
                    ancestor: traceFingerprint(event.ancestor),
                    target: traceFingerprint(event.target),
                    saveSnapshot: traceFingerprint(event.saveSnapshot),
                    savedAncestor: traceFingerprint(event.savedAncestor),
                    requestedTokenDelta: traceTokenDelta(event.ancestor, event.target),
                    acceptedTokenDelta: traceTokenDelta(
                        event.canonicalBefore,
                        event.result ?? event.canonicalAfter,
                    ),
                    observedTokenDelta: traceTokenDelta(event.diskBefore, event.observed),
                    savedAncestorTokenDelta: traceTokenDelta(
                        event.savedAncestor,
                        event.observed,
                    ),
                    saveDiskTokenDelta: traceTokenDelta(
                        event.diskBefore,
                        event.saveSnapshot,
                    ),
                }))
            );
            assertNoDuplicatedStreamingTokens = (): void => {
                for (const [endpoint, content] of [
                    ['editor', document.getText()],
                    ['remote', remoteSession?.content],
                ] as Array<[string, string | undefined]>) {
                    if (content === undefined) {
                        continue;
                    }
                    const duplicated = expectedTokens.flatMap((token, index) => (
                        countExactOccurrences(content, token) > 1 ? [index] : []
                    ));
                    if (duplicated.length === 0) {
                        continue;
                    }
                    output.appendLine(
                        `[diagnostic] duplicating causal trace `
                        + `${JSON.stringify(summarizeCausalTrace(120))}`,
                    );
                    assert(
                        false,
                        `Streaming ${endpoint} duplicated tokens `
                        + `${duplicated.join(',')} at length ${content.length}`,
                    );
                }
            };
            let localEditCount = 0;
            let editorChangeEventCount = 0;
            let remoteJournaledEditCount = 0;
            let aiCommittedOperationCount = 0;
            let rawFilesystemEditCount = 0;
            let batchedLocalOperationCount = 0;
            // The causal ring keeps only the last few hundred events, and the
            // range replacement happens tens of seconds before the assertion
            // that finds it missing. Snapshot the ring at the instant the
            // buffer stops holding it, which is the only moment that explains
            // a lost update.
            const streamingStartedAt = Date.now();
            let rangeReplacementSeen = false;
            let rangeReplacementLoss: Record<string, unknown> | undefined;
            // Every streaming token is written whole, exactly once, and never
            // rewritten. A token that stops being whole after arriving was
            // therefore not overwritten by its own writer: some other edit
            // landed inside it. Only the change event that did it carries the
            // offset it was applied at, which is what names the writer.
            const wholeTokens = new Set<string>();
            let tokenBreakLoss: Record<string, unknown> | undefined;
            const describeBreak = (text: string, token: string): Record<string, unknown> => {
                for (let length = token.length - 1; length > 0; length--) {
                    const brokenAt = text.indexOf(token.slice(0, length));
                    if (brokenAt >= 0) {
                        return {
                            survivingPrefix: length,
                            brokenAt,
                            neighbourhood: JSON.stringify(
                                text.slice(
                                    Math.max(0, brokenAt - 16),
                                    brokenAt + token.length + 32,
                                ),
                            ),
                        };
                    }
                }
                return {survivingPrefix: 0};
            };
            const editorEvents = vscode.workspace.onDidChangeTextDocument(event => {
                if (event.document.uri.toString() === document.uri.toString()) {
                    editorChangeEventCount += event.contentChanges.length;
                    const text = event.document.getText();
                    if (
                        !localWriterFinished
                        && remoteTokens.some(token => text.includes(token))
                    ) {
                        remoteObservedBeforeLocalStop = true;
                    }
                    for (const token of expectedTokens) {
                        if (text.includes(token)) {
                            wholeTokens.add(token);
                        } else if (wholeTokens.delete(token) && tokenBreakLoss === undefined) {
                            tokenBreakLoss = {
                                at: Date.now() - streamingStartedAt,
                                token: JSON.stringify(token),
                                ...describeBreak(text, token),
                                reason: event.reason,
                                editCount: localEditCount,
                                editorLength: text.length,
                                documentVersion: event.document.version,
                                remoteRevision: remoteSession?.revision,
                                remotePendingOperationIds: [
                                    ...(remoteSession?.pendingOperationIds ?? []),
                                ],
                                changes: event.contentChanges.map(change => ({
                                    offset: change.rangeOffset,
                                    removed: change.rangeLength,
                                    inserted: change.text.length,
                                    text: change.text.length > 96
                                        ? `${change.text.slice(0, 96)}…`
                                        : change.text,
                                })),
                                trace: summarizeCausalTrace(600),
                            };
                            output.appendLine(
                                '[diagnostic] streaming token broke apart '
                                + JSON.stringify(tokenBreakLoss),
                            );
                        }
                    }
                    if (text.includes(rangeReplacement)) {
                        rangeReplacementSeen = true;
                    } else if (rangeReplacementSeen && rangeReplacementLoss === undefined) {
                        rangeReplacementLoss = {
                            at: Date.now() - streamingStartedAt,
                            reason: event.reason,
                            editCount: localEditCount,
                            editor: traceFingerprint(text),
                            changes: event.contentChanges.map(change => ({
                                offset: change.rangeOffset,
                                removed: change.rangeLength,
                                inserted: change.text.length,
                                text: change.text.length > 96
                                    ? `${change.text.slice(0, 96)}…`
                                    : change.text,
                            })),
                            trace: summarizeCausalTrace(600),
                        };
                        output.appendLine(
                            '[diagnostic] range replacement left the buffer '
                            + JSON.stringify(rangeReplacementLoss),
                        );
                    }
                }
            });
            let explicitSaveStarted = false;
            let saveEventsBeforeExplicitSave = 0;
            const autoSaveMode = vscode.workspace.getConfiguration('files', document.uri)
                .get<string>('autoSave', 'off');
            const autoSaveEnabled = autoSaveMode !== 'off';
            const saveEvents = vscode.workspace.onDidSaveTextDocument(saved => {
                if (
                    !explicitSaveStarted
                    && saved.uri.toString() === document.uri.toString()
                ) {
                    saveEventsBeforeExplicitSave += 1;
                }
            });
            context.subscriptions.push(saveEvents);
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
                // A remote operation can reach the workbench between the caret
                // placement and the keystroke it anchors. An offset taken from
                // the extension host's copy of the document is absolute, so
                // honouring it after that insert lands puts the caret inside
                // the inserted line and types the token into the middle of
                // someone else's text. `cursorBottom` runs against the live
                // model in the workbench and the caret then rides later
                // inserts, which is where a person typing at the end would be.
                await vscode.commands.executeCommand('cursorBottom');
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
            const replaceEditorFragment = async (
                source: string,
                replacement: string,
            ): Promise<void> => {
                // The workbench rejects an editor edit whose document moved
                // under it, and a remote operation projected into this buffer
                // does exactly that. The rejection is the guard working, so
                // rebuild the range against the newer buffer and edit again.
                let applied = false;
                for (let attempt = 0; attempt < 8 && !applied; attempt++) {
                    const current = document.getText();
                    assert(
                        countExactOccurrences(current, source) === 1,
                        `Streaming range source does not occur exactly once: ${source.trim()}`,
                    );
                    const startOffset = current.indexOf(source);
                    applied = await streamingEditor.edit(builder => {
                        builder.replace(
                            new vscode.Range(
                                document.positionAt(startOffset),
                                document.positionAt(startOffset + source.length),
                            ),
                            replacement,
                        );
                    }, {
                        undoStopBefore: false,
                        undoStopAfter: false,
                    });
                }
                assert(applied, 'VS Code rejected a streaming range edit');
                assert(
                    !document.getText().includes(source)
                    && (replacement.length === 0 || document.getText().includes(replacement)),
                    'VS Code did not apply the requested streaming range edit',
                );
                localEditCount += 1;
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

            const singleWriterPhases: NonNullable<
                E2EEvidence['singleWriterStreams']
            >['phases'] = [];
            const waitForSingleWriterConvergence = async (
                expected: string,
                tokens: readonly string[],
                label: string,
                writersStoppedAt: number,
            ): Promise<number> => {
                assert(writersStoppedAt > 0, `${label} has no writer-stop timestamp`);
                // Which endpoint is still behind is the whole diagnosis, so
                // carry it in the failure instead of a bare timeout.
                let lastEndpoints = 'never sampled';
                // A single sample of a busy reconcile queue names one pass out
                // of however many ran. Keep the sequence, so a queue that never
                // drains can be told from one pass that is simply slow.
                const reconcileHistory: string[] = [];
                try {
                    await waitForAsync(async () => {
                        assertStreamingRuntimeHealthy();
                        const local = await readLocalBytes(localUri);
                        const disk = local === undefined
                            ? undefined
                            : Buffer.from(local).toString('utf8');
                        const runtime = LocalReplicaSCMProvider
                            .getRuntimeStatus(localRoot!.fsPath);
                        // Read the document VS Code currently has open for this
                        // path. If the captured object was replaced, comparing
                        // its frozen text would report a stale endpoint.
                        const openDocument = vscode.workspace.textDocuments.find(
                            candidate => candidate.uri.fsPath === localUri.fsPath,
                        );
                        const editorText = (openDocument ?? document).getText();
                        const reconciling = runtime?.activeReconcile === undefined
                            ? undefined
                            : `${runtime.activeReconcile.reason}`
                                + `:${runtime.activeReconcile.phase}`;
                        if (
                            reconciling !== undefined
                            && reconciling !== reconcileHistory[reconcileHistory.length - 1]
                            && reconcileHistory.length < 24
                        ) {
                            reconcileHistory.push(reconciling);
                        }
                        lastEndpoints = `expected=${expected.length}`
                            + ` editor=${editorText.length}`
                            + `${editorText === expected ? '=' : '!'}`
                            + (openDocument === document ? '' : ` captured=`
                                + `${document.getText().length}`
                                + `${document.isClosed ? 'closed' : 'open'}`
                                + `/live=${openDocument === undefined ? 'none' : 'other'}`)
                            + ` disk=${disk?.length}${disk === expected ? '=' : '!'}`
                            + ` remote=${remoteSession!.content.length}`
                            + `${remoteSession!.content === expected ? '=' : '!'}`
                            + ` pending=${remoteSession!.pendingOperationIds.length}`
                            + ` settled=${runtime?.settled}`
                            + `(${runtime?.settledBlockers.join(',')})`
                            + ` dirty=${document.isDirty}`
                            + ` reconciles=${runtime?.reconcileStarts}`
                            + (runtime?.activeReconcile === undefined ? '' : ` reconcile=`
                                + `"${runtime.activeReconcile.reason}"`
                                + `@${runtime.activeReconcile.durationMs}ms`
                                + `:${runtime.activeReconcile.phase}`
                                + `#${runtime.activeReconcile.iteration}`)
                            + (runtime?.activeDocumentTasks.length === 0 ? '' : ` tasks=`
                                + runtime!.activeDocumentTasks
                                    .map(task => `${task.source}@${task.durationMs}ms`)
                                    .join(','));
                        return editorText === expected
                            && disk === expected
                            && remoteSession!.content === expected
                            && remoteSession!.pendingOperationIds.length === 0
                            && runtime?.settled === true;
                    }, 5000, label, 25);
                } catch (error) {
                    const trace = LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                        localRoot!.fsPath,
                        relativePath,
                    ).slice(-48).map(event => ({
                        sequence: event.sequence,
                        elapsedMs: event.elapsedMs,
                        kind: event.kind,
                        reason: event.reason,
                        classification: event.classification,
                        bufferedInput: event.bufferedInput,
                        canonicalLength: event.canonicalBefore?.length,
                        canonicalAfterLength: event.canonicalAfter?.length,
                        editorLength: event.editor?.length,
                        diskLength: event.diskBefore?.length,
                        observedLength: event.observed?.length,
                        ancestorLength: event.ancestor?.length,
                        targetLength: event.target?.length,
                    }));
                    output.appendLine(`[diagnostic] ${label}: trace ${JSON.stringify(trace)}`);
                    output.appendLine(
                        `[diagnostic] ${label}: reconciles `
                        + `${reconcileHistory.join(' -> ') || '(none while waiting)'}`,
                    );
                    throw new Error(`${(error as Error).message} [${lastEndpoints}]`);
                }
                const convergenceAfterWriterMs = Date.now() - writersStoppedAt;
                assert(
                    convergenceAfterWriterMs <= 5000,
                    `${label} exceeded the five-second realtime bound`,
                );
                assert(
                    tokens.every(token => countExactOccurrences(expected, token) === 1),
                    `${label} lost, duplicated, or reordered an edit`,
                );
                const authoritative = await freshDocument(
                    serverUrl!, api!, identity!, projectId!, runFolderId!, fileName,
                );
                assert(
                    authoritative?.content === expected,
                    `${label} did not reach the authoritative Overleaf document`,
                );
                const state = await replicaStateStore!.reload();
                assert(!state.pendingAction, `${label} left a pending replica action`);
                assert(
                    !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName)),
                    `${label} created a conflict instead of converging`,
                );
                return convergenceAfterWriterMs;
            };
            const observeWhileWriting = async (
                readContent: () => Promise<string> | string,
                firstToken: string,
                writerStopped: () => boolean,
                label: string,
            ): Promise<boolean> => {
                let observedBeforeWriterStopped = false;
                const startedAt = Date.now();
                let stoppedAt: number | undefined;
                // Every distinct buffer state seen while waiting, so a delay is
                // attributable to the buffer, the save marker, or neither.
                const editorSamples: string[] = [];
                let lastSample = '';
                await waitForAsync(async () => {
                    assertStreamingRuntimeHealthy();
                    if (stoppedAt === undefined && writerStopped()) {
                        stoppedAt = Date.now();
                    }
                    const sample = `${document.getText().length}`
                        + `${document.isDirty ? 'D' : 'S'}v${document.version}`;
                    if (sample !== lastSample && editorSamples.length < 80) {
                        lastSample = sample;
                        editorSamples.push(`${Date.now() - startedAt}:${sample}`);
                    }
                    if ((await readContent()).includes(firstToken)) {
                        observedBeforeWriterStopped = !writerStopped();
                        return true;
                    }
                    return false;
                }, 15000, label, 10);
                output.appendLine(
                    `[diagnostic] ${label}: visible after ${Date.now() - startedAt}ms, `
                    + `writer stopped after ${(stoppedAt ?? Date.now()) - startedAt}ms, `
                    + `editor ${editorSamples.join(' ')}`,
                );
                if (!observedBeforeWriterStopped) {
                    // The first token became visible only after its writer
                    // finished. Dump the state machine timeline so the fixed
                    // delay can be attributed instead of guessed.
                    const trace = LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                        localRoot!.fsPath,
                        relativePath,
                    ).slice(-96).map(event => ({
                        sequence: event.sequence,
                        elapsedMs: event.elapsedMs,
                        kind: event.kind,
                        reason: event.reason,
                        bufferedInput: event.bufferedInput,
                        diskMutation: event.diskMutation,
                        pendingEditorObservations: event.pendingEditorObservations,
                        canonicalLength: event.canonicalBefore?.length,
                        canonicalAfterLength: event.canonicalAfter?.length,
                        editorLength: event.editor?.length,
                        targetLength: event.target?.length,
                        resultLength: event.result?.length,
                    }));
                    output.appendLine(`[diagnostic] ${label}: trace ${JSON.stringify(trace)}`);
                }
                return observedBeforeWriterStopped;
            };
            // A save started by VS Code auto-save can already be in flight, so
            // an explicit save legitimately reports false while doing nothing.
            // The invariant is the saved state itself, not which writer got
            // there, so require that and report what the replica was doing if
            // it never arrives.
            const requireSavedEditor = async (label: string): Promise<void> => {
                try {
                    await saveDocument(document, label);
                } catch (error) {
                    // A buffer that stays dirty after its save resolved is the
                    // host refusing the write, not the replica withholding one.
                    // Report what the host and the file itself say, so the two
                    // can be told apart without another run.
                    let file = 'unreadable';
                    try {
                        const stats = statSync(document.uri.fsPath);
                        file = `size=${stats.size} ino=${stats.ino}`
                            + ` mtime=${Math.round(stats.mtimeMs)}`;
                    } catch (statError) {
                        file = `${(statError as Error).message}`;
                    }
                    const resaved = await document.save().then(
                        value => `${value}`,
                        saveError => `threw:${(saveError as Error).message}`,
                    );
                    output.appendLine(
                        `[diagnostic] ${label}: editor dirty=${document.isDirty}`
                        + ` closed=${document.isClosed} version=${document.version}`
                        + ` length=${document.getText().length} resave=${resaved}`
                        + ` dirtyAfterResave=${document.isDirty} file(${file})`,
                    );
                    const trace = LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                        localRoot!.fsPath,
                        relativePath,
                    ).slice(-32).map(event => ({
                        sequence: event.sequence,
                        elapsedMs: event.elapsedMs,
                        kind: event.kind,
                        reason: event.reason,
                        canonicalLength: event.canonicalBefore?.length,
                        editorLength: event.editor?.length,
                        diskLength: event.diskBefore?.length,
                        targetLength: event.target?.length,
                    }));
                    output.appendLine(`[diagnostic] ${label}: trace ${JSON.stringify(trace)}`);
                    throw error;
                }
            };
            const resetSingleWriterFixture = async (label: string): Promise<void> => {
                await requireSavedEditor(`${label} could not save before reset`);
                await remoteSession!.submitSnapshot('', true);
                await waitForSingleWriterConvergence('', [], `${label} reset`, Date.now());
                await requireSavedEditor(`${label} reset did not leave a clean editor`);
            };

            let aiOverlappedLocalAndRemote = false;
            let rawFilesystemOverlappedLocalAndRemote = false;
            try {
                await step('continuous remote-only writing converges in realtime', async () => {
                    const tokens = Array.from(
                        {length: 6},
                        (_, index) => `REMOTE-ONLY-${request.nonce}-${index}\n`,
                    );
                    const expected = tokens.join('');
                    let writerStopped = false;
                    let writerStoppedAt = 0;
                    const [observedBeforeWriterStopped] = await Promise.all([
                        observeWhileWriting(
                            () => document.getText(),
                            tokens[0],
                            () => writerStopped,
                            'observe remote-only input in VS Code before the writer stops',
                        ),
                        (async () => {
                            try {
                                for (const [index, token] of tokens.entries()) {
                                    const ancestor = remoteSession!.content;
                                    const accepted = await remoteSession!
                                        .acceptSnapshotAgainstAncestor(
                                            ancestor,
                                            `${ancestor}${token}`,
                                            true,
                                        );
                                    await accepted.acknowledgement;
                                    assertStreamingRuntimeHealthy();
                                    if (index < tokens.length - 1) {
                                        await pause(50);
                                    }
                                }
                            } finally {
                                writerStopped = true;
                                writerStoppedAt = Date.now();
                            }
                        })(),
                    ]);
                    assert(
                        observedBeforeWriterStopped,
                        'Remote-only input was visible only after its writer stopped',
                    );
                    const convergenceAfterWriterMs = await waitForSingleWriterConvergence(
                        expected,
                        tokens,
                        'converge remote-only input',
                        writerStoppedAt,
                    );
                    const finalRevision = remoteSession!.revision;
                    await resetSingleWriterFixture('remote-only phase');
                    singleWriterPhases.push({
                        writer: 'remote',
                        editCount: tokens.length,
                        observedBeforeWriterStopped,
                        convergenceAfterWriterMs,
                        finalRevision,
                        sha256: createHash('sha256').update(expected).digest('hex'),
                    });
                });

                await step('continuous VS Code-only writing converges in realtime', async () => {
                    const tokens = Array.from(
                        {length: 24},
                        (_, index) => String.fromCharCode(0xe100 + index),
                    );
                    const expected = tokens.join('');
                    let writerStopped = false;
                    let writerStoppedAt = 0;
                    const [observedBeforeWriterStopped] = await Promise.all([
                        observeWhileWriting(
                            () => remoteSession!.content,
                            tokens[0],
                            () => writerStopped,
                            'observe VS Code-only input remotely before the writer stops',
                        ),
                        (async () => {
                            try {
                                for (const [index, token] of tokens.entries()) {
                                    await applyEditorToken(token);
                                    assertStreamingRuntimeHealthy();
                                    if (index < tokens.length - 1) {
                                        await pause(25);
                                    }
                                }
                            } finally {
                                writerStopped = true;
                                writerStoppedAt = Date.now();
                            }
                        })(),
                    ]);
                    assert(
                        observedBeforeWriterStopped,
                        'VS Code-only input reached Overleaf only after its writer stopped',
                    );
                    const convergenceAfterWriterMs = await waitForSingleWriterConvergence(
                        expected,
                        tokens,
                        'converge VS Code-only input',
                        writerStoppedAt,
                    );
                    const finalRevision = remoteSession!.revision;
                    await resetSingleWriterFixture('VS Code-only phase');
                    singleWriterPhases.push({
                        writer: 'vscode',
                        editCount: tokens.length,
                        observedBeforeWriterStopped,
                        convergenceAfterWriterMs,
                        finalRevision,
                        sha256: createHash('sha256').update(expected).digest('hex'),
                    });
                });

                await step('continuous AI-only writing converges in realtime', async () => {
                    const tokens = Array.from(
                        {length: 6},
                        (_, index) => `AI-ONLY-${request.nonce}-${index}\n`,
                    );
                    const expected = tokens.join('');
                    let writerStopped = false;
                    let writerStoppedAt = 0;
                    const [observedBeforeWriterStopped] = await Promise.all([
                        observeWhileWriting(
                            () => document.getText(),
                            tokens[0],
                            () => writerStopped,
                            'observe AI-only input in VS Code before the writer stops',
                        ),
                        (async () => {
                            try {
                                for (const [index, token] of tokens.entries()) {
                                    await commitAiToken(token);
                                    assertStreamingRuntimeHealthy();
                                    if (index < tokens.length - 1) {
                                        await pause(50);
                                    }
                                }
                            } finally {
                                writerStopped = true;
                                writerStoppedAt = Date.now();
                            }
                        })(),
                    ]);
                    assert(
                        observedBeforeWriterStopped,
                        'AI-only input was visible only after its writer stopped',
                    );
                    const convergenceAfterWriterMs = await waitForSingleWriterConvergence(
                        expected,
                        tokens,
                        'converge AI-only input',
                        writerStoppedAt,
                    );
                    const finalRevision = remoteSession!.revision;
                    await resetSingleWriterFixture('AI-only phase');
                    singleWriterPhases.push({
                        writer: 'ai',
                        editCount: tokens.length,
                        observedBeforeWriterStopped,
                        convergenceAfterWriterMs,
                        finalRevision,
                        sha256: createHash('sha256').update(expected).digest('hex'),
                    });
                });
                evidence.singleWriterStreams = {path: relativePath, phases: singleWriterPhases};

                // The independent phases share this real editor and listeners;
                // start concurrent-stress accounting only after their reset.
                localEditCount = 0;
                editorChangeEventCount = 0;
                aiCommittedOperationCount = 0;
                saveEventsBeforeExplicitSave = 0;
                localWriterFinished = false;
                remoteWriterFinished = false;
                remoteObservedBeforeLocalStop = false;

                await applyEditorBurst([rangeSeed]);
                await waitForAsync(async () => {
                    const local = await readLocalBytes(localUri);
                    return local !== undefined
                        && Buffer.from(local).toString('utf8') === rangeSeed
                        && remoteSession!.content === rangeSeed
                        && document.getText() === rangeSeed;
                }, 30000, 'establish the streaming range-edit baseline', 100);
                const revisionBeforeBatchedBurst = remoteSession.revision;
                await applyEditorBurst(batchedLocalTokens);
                let lastBatchObservation: Record<string, unknown> = {};
                try {
                    await waitForAsync(async () => {
                        const [local, inspection] = await Promise.all([
                            readLocalBytes(localUri),
                            LocalReplicaSCMProvider.inspectE2EDocumentRecovery(
                                localRoot!.fsPath,
                                relativePath,
                            ),
                        ]);
                        const editor = document.getText();
                        const disk = local === undefined
                            ? undefined
                            : Buffer.from(local).toString('utf8');
                        const remote = remoteSession!.content;
                        lastBatchObservation = {
                            batchedTokensInEditor: batchedLocalTokens
                                .map(token => editor.includes(token)),
                            batchedTokensOnDisk: batchedLocalTokens
                                .map(token => disk?.includes(token) ?? false),
                            batchedTokensInRemote: batchedLocalTokens
                                .map(token => remote.includes(token)),
                            editorDirty: document.isDirty,
                            editorLength: editor.length,
                            diskLength: disk?.length,
                            remoteLength: remote.length,
                            remoteRevision: remoteSession!.revision,
                            revisionBeforeBatchedBurst,
                            externalPendingOperationIds: [...remoteSession!.pendingOperationIds],
                            localInspection: inspection,
                            runtime: LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath),
                            sessionErrors: sessionErrors.map(safeMessage),
                            receiveErrors: receiveErrors.map(safeMessage),
                        };
                        return remoteSession!.revision >= revisionBeforeBatchedBurst + 1
                            && batchedLocalTokens.every(token => remote.includes(token));
                    }, 30000, 'journal one batched operation for rapid VS Code input', 100);
                } catch (error) {
                    throw new Error(
                        `${safeMessage(error)}; last observation: `
                        + JSON.stringify(lastBatchObservation),
                    );
                }
                batchedLocalOperationCount = remoteSession.revision - revisionBeforeBatchedBurst;
                assert(
                    batchedLocalOperationCount > 0
                    && batchedLocalOperationCount < batchedLocalTokens.length,
                    `${batchedLocalTokens.length} rapid edits created `
                    + `${batchedLocalOperationCount} remote operations without batching`,
                );

                let signalRemoteJournaled!: () => void;
                const remoteJournaledAll = new Promise<void>(resolve => {
                    signalRemoteJournaled = resolve;
                });
                let remoteJournalSignalSent = false;
                let remoteAcknowledgedEditCount = 0;
                let signalFirstAiCommit!: () => void;
                const firstAiCommit = new Promise<void>(resolve => {
                    signalFirstAiCommit = resolve;
                });
                let firstAiCommitSignalled = false;
                const releaseFirstAiCommit = (): void => {
                    if (!firstAiCommitSignalled) {
                        firstAiCommitSignalled = true;
                        signalFirstAiCommit();
                    }
                };
                holdRemoteUpdates = true;
                const outcomes = await Promise.allSettled([
                    (async () => {
                        try {
                            for (const [index, token] of localTokens.entries()) {
                                if (index === 5) {
                                    await replaceEditorFragment(rangeReplaceSource, rangeReplacement);
                                }
                                if (index === 19) {
                                    await replaceEditorFragment(rangeDeleteSource, '');
                                }
                                await applyEditorToken(token);
                                assertStreamingRuntimeHealthy();
                                await pause(18);
                                if (index === 11) {
                                    await firstAiCommit;
                                }
                            }
                        } finally {
                            localWriterFinished = true;
                        }
                    })(),
                    (async () => {
                        const acknowledgements: Promise<string>[] = [];
                        try {
                            for (const token of remoteTokens) {
                                const ancestor = remoteSession!.content;
                                const submittedAt = Date.now();
                                const accepted = await remoteSession!.acceptSnapshotAgainstAncestor(
                                    ancestor,
                                    `${ancestor}${token}`,
                                );
                                remoteSubmissionTimes.set(token, submittedAt);
                                acknowledgements.push(accepted.acknowledgement.then(content => {
                                    remoteAcknowledgementTimes.set(token, Date.now());
                                    remoteAcknowledgedEditCount += 1;
                                    return content;
                                }));
                                remoteJournaledEditCount += 1;
                                assertStreamingRuntimeHealthy();
                                await pause(23);
                            }
                            remoteJournalSignalSent = true;
                            signalRemoteJournaled();
                            await Promise.all(acknowledgements);
                        } finally {
                            if (!remoteJournalSignalSent) {
                                signalRemoteJournaled();
                            }
                            remoteWriterFinished = true;
                        }
                    })(),
                    (async () => {
                        await remoteJournaledAll;
                        assert(
                            remoteJournaledEditCount === remoteTokens.length,
                            'Remote writer stopped before journaling the complete backlog',
                        );
                        assert(
                            remoteAcknowledgedEditCount === 0,
                            'Remote writer did not build a pending-operation backlog',
                        );
                        await firstAiCommit;
                        while (heldRemoteUpdates.length > 0) {
                            await remoteSession!.receiveUpdate(heldRemoteUpdates.shift()!);
                        }
                        holdRemoteUpdates = false;
                    })(),
                    (async () => {
                        const deadline = Date.now() + 15000;
                        while (observedRemoteTokens.size < remoteTokens.length) {
                            const localEditor = document.getText();
                            for (const [token, submittedAt] of remoteSubmissionTimes) {
                                if (!observedRemoteTokens.has(token) && localEditor.includes(token)) {
                                    const observedAt = Date.now();
                                    observedRemoteTokens.add(token);
                                    remoteObservationTimes.set(token, observedAt);
                                    remoteAdmissionToObservationLatencies.push(
                                        observedAt - submittedAt,
                                    );
                                    if (!localWriterFinished) {
                                        remoteObservedBeforeLocalStop = true;
                                    }
                                }
                            }
                            assertStreamingRuntimeHealthy();
                            if (
                                remoteWriterFinished
                                && remoteSubmissionTimes.size < remoteTokens.length
                            ) {
                                throw new Error('Remote streaming writer stopped before every token');
                            }
                            if (Date.now() >= deadline) {
                                throw new Error(
                                    `Timed out pulling remote streaming operations: `
                                    + `${observedRemoteTokens.size}/${remoteTokens.length}`,
                                );
                            }
                            await pause(25);
                        }
                    })(),
                    (async () => {
                        await remoteJournaledAll;
                        try {
                            for (const [index, token] of aiTokens.entries()) {
                                await commitAiToken(token);
                                if (index === 0) {
                                    aiOverlappedLocalAndRemote = !localWriterFinished
                                        && remoteJournaledEditCount === remoteTokens.length
                                        && remoteAcknowledgedEditCount === 0;
                                    releaseFirstAiCommit();
                                }
                                assertStreamingRuntimeHealthy();
                                await pause(31);
                            }
                        } finally {
                            releaseFirstAiCommit();
                        }
                    })(),
                    (async () => {
                        await remoteJournaledAll;
                        for (const [index, token] of rawFilesystemTokens.entries()) {
                            // Keep one writable descriptor across read and
                            // append. This gives the projection CAS an inode
                            // fence instead of an ambiguous stale-snapshot
                            // overwrite with no provable event-time ancestor.
                            await waitForAsync(() => appendThroughWritableHandle(
                                localUri.fsPath,
                                Buffer.from(token, 'utf8'),
                            ), 5000, 'append through a stable raw-writer inode', 10);
                            rawFilesystemEditCount += 1;
                            if (index === 0) {
                                rawFilesystemOverlappedLocalAndRemote = !localWriterFinished
                                    && remoteJournaledEditCount === remoteTokens.length
                                    && remoteAcknowledgedEditCount === 0;
                            }
                            assertStreamingRuntimeHealthy();
                            await pause(37);
                        }
                    })(),
                ]);
                const failed = outcomes.find(
                    (outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected',
                );
                if (failed) {
                    const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                    const activeTasks = runtime?.activeDocumentTasks ?? [];
                    const suffix = activeTasks.length === 0
                        ? ''
                        : `; active document tasks: ${JSON.stringify(activeTasks)}`;
                    throw new Error(`${safeMessage(failed.reason)}${suffix}`);
                }
                for (const token of remoteTokens) {
                    const acknowledgedAt = remoteAcknowledgementTimes.get(token);
                    const observedAt = remoteObservationTimes.get(token);
                    assert(
                        acknowledgedAt !== undefined && observedAt !== undefined,
                        `Remote token lacks ACK/observation timing evidence: ${token.trim()}`,
                    );
                    remotePullLatencies.push(Math.max(0, observedAt - acknowledgedAt));
                }
            } finally {
                editorEvents.dispose();
            }
            const writersStoppedAt = Date.now();
            if (!remoteObservedBeforeLocalStop) {
                const boundaryTrace = LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                    localRoot!.fsPath,
                    relativePath,
                ).slice(-64).map(event => ({
                    sequence: event.sequence,
                    elapsedMs: event.elapsedMs,
                    kind: event.kind,
                    reason: event.reason,
                    bufferedInput: event.bufferedInput,
                    diskMutation: event.diskMutation,
                    pendingEditorObservations: event.pendingEditorObservations,
                    canonicalLength: event.canonicalBefore?.length,
                    editorLength: event.editor?.length,
                    targetLength: event.target?.length,
                    resultLength: event.result?.length,
                }));
                output.appendLine(
                    `[diagnostic] remote visibility boundary trace ${JSON.stringify(boundaryTrace)}`,
                );
            }
            assert(
                localEditCount === 1 + batchedLocalTokens.length + localTokens.length + 2,
                'Streaming editor did not issue every edit',
            );
            assert(
                editorChangeEventCount >= localEditCount,
                `Streaming editor emitted ${editorChangeEventCount} change events for ${localEditCount} edits`,
            );
            assert(
                remoteJournaledEditCount === remoteTokens.length,
                'Remote streaming writer did not journal every edit',
            );
            assert(
                aiCommittedOperationCount === aiTokens.length,
                'AI streaming writer did not journal every operation',
            );
            assert(
                rawFilesystemEditCount === rawFilesystemTokens.length,
                'Raw filesystem writer did not issue every stable snapshot',
            );
            assert(
                remoteObservedBeforeLocalStop,
                'Remote streaming OT was not visible until the local writer stopped',
            );
            assert(
                aiOverlappedLocalAndRemote,
                'AI commit did not overlap active local input and the remote backlog',
            );
            assert(
                rawFilesystemOverlappedLocalAndRemote,
                'Raw filesystem snapshots did not overlap active local input and the remote backlog',
            );
            const containsAllTokens = (content: string): boolean => (
                expectedTokens.every(token => countExactOccurrences(content, token) === 1)
                && !content.includes(rangeReplaceSource)
                && !content.includes(rangeDeleteSource)
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
                containsInOrder(content, [rangeKeepA, rangeReplacement, rangeKeepB])
                && containsInOrder(content, batchedLocalTokens)
                && containsInOrder(content, localTokens)
                && containsInOrder(content, remoteTokens)
                && containsInOrder(content, aiTokens)
                && containsInOrder(content, rawFilesystemTokens)
            );
            let converged = '';
            let lastStreamingObservation: Record<string, unknown> = {};
            try {
                await waitForAsync(async () => {
                    const [local, state] = await Promise.all([
                        readLocalBytes(localUri),
                        replicaStateStore!.reload(),
                    ]);
                    const editor = document.getText();
                    const disk = local === undefined
                        ? undefined
                        : Buffer.from(local).toString('utf8');
                    const remote = remoteSession!.content;
                    const tokenCounts = (content: string): number[] => expectedTokens
                        .map(token => countExactOccurrences(content, token));
                    const editorTokenCounts = tokenCounts(editor);
                    const diskTokenCounts = disk === undefined ? [] : tokenCounts(disk);
                    const remoteTokenCounts = tokenCounts(remote);
                    // A token counted as missing while the total length is
                    // still right was not dropped, it was broken apart. Report
                    // the longest prefix that survived and what sits where the
                    // rest of it should be, which names the writer that landed
                    // inside it instead of beside it.
                    const explainMissing = (
                        content: string,
                        counts: number[],
                    ): Array<Record<string, unknown>> => counts.flatMap((count, index) => {
                        if (count !== 0) {
                            return [];
                        }
                        const token = expectedTokens[index];
                        let prefix = 0;
                        let at = -1;
                        for (let length = token.length - 1; length > 0; length--) {
                            const found = content.indexOf(token.slice(0, length));
                            if (found >= 0) {
                                prefix = length;
                                at = found;
                                break;
                            }
                        }
                        // Repeated line prefixes occur many times, and only the
                        // occurrence whose tail diverges shows where the token
                        // was broken. Report each of them with its neighbours.
                        const sightings: string[] = [];
                        for (
                            let cursor = at;
                            cursor >= 0 && sightings.length < 4;
                            cursor = prefix > 0
                                ? content.indexOf(token.slice(0, prefix), cursor + 1)
                                : -1
                        ) {
                            if (content.startsWith(token, cursor)) {
                                continue;
                            }
                            sightings.push(JSON.stringify(
                                content.slice(cursor, cursor + token.length + 32),
                            ));
                        }
                        return [{
                            index,
                            token: JSON.stringify(token),
                            survivingPrefix: prefix,
                            at,
                            sightings,
                        }];
                    });
                    const summarizeCounts = (counts: number[]): {
                        missing: number[];
                        duplicated: Array<{index: number; count: number}>;
                    } => ({
                        missing: counts.flatMap((count, index) => count === 0 ? [index] : []),
                        duplicated: counts.flatMap((count, index) => (
                            count > 1 ? [{index, count}] : []
                        )),
                    });
                    const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                    lastStreamingObservation = {
                        expectedUtf16Length,
                        editorLength: editor.length,
                        diskLength: disk?.length,
                        remoteLength: remote.length,
                        editorSha256: createHash('sha256').update(editor).digest('hex'),
                        diskSha256: disk === undefined
                            ? undefined
                            : createHash('sha256').update(disk).digest('hex'),
                        remoteSha256: createHash('sha256').update(remote).digest('hex'),
                        editorEqualsDisk: editor === disk,
                        editorEqualsRemote: editor === remote,
                        diskEqualsRemote: disk === remote,
                        editorTokens: summarizeCounts(editorTokenCounts),
                        diskTokens: summarizeCounts(diskTokenCounts),
                        remoteTokens: summarizeCounts(remoteTokenCounts),
                        remoteMissingDetail: explainMissing(remote, remoteTokenCounts),
                        editorPreservesWriterOrder: preservesWriterOrder(editor),
                        diskPreservesWriterOrder: disk === undefined
                            ? false
                            : preservesWriterOrder(disk),
                        remotePreservesWriterOrder: preservesWriterOrder(remote),
                        editorHasReplacedSources: editor.includes(rangeReplaceSource)
                            || editor.includes(rangeDeleteSource),
                        diskHasReplacedSources: disk?.includes(rangeReplaceSource) === true
                            || disk?.includes(rangeDeleteSource) === true,
                        remoteHasReplacedSources: remote.includes(rangeReplaceSource)
                            || remote.includes(rangeDeleteSource),
                        remoteRevision: remoteSession!.revision,
                        remotePendingOperationIds: [...remoteSession!.pendingOperationIds],
                        pendingAction: state.pendingAction?.kind,
                        fixtureConflicts: Object.keys(state.conflicts)
                            .filter(filePath => isE2EConflictPath(filePath, isolatedRunName)),
                        runtime,
                    };
                    if (
                        disk === undefined
                        || !containsAllTokens(editor)
                        || !containsAllTokens(remote)
                    ) {
                        return false;
                    }
                    converged = remote;
                    return editor === converged
                        && disk === converged
                        && converged.length === expectedUtf16Length
                        && preservesWriterOrder(converged)
                        && !state.pendingAction
                        && !Object.keys(state.conflicts)
                            .some(filePath => isE2EConflictPath(filePath, isolatedRunName));
                }, 60000, 'converge unsaved editor, disk, and remote streaming snapshots', 500);
            } catch (error) {
                const causalTrace = summarizeCausalTrace(600);
                output.appendLine(
                    `[diagnostic] streaming causal trace ${JSON.stringify(causalTrace)}`,
                );
                if (rangeReplacementLoss) {
                    output.appendLine(
                        `[diagnostic] streaming range replacement loss `
                        + `${JSON.stringify(rangeReplacementLoss)}`,
                    );
                }
                if (tokenBreakLoss) {
                    output.appendLine(
                        `[diagnostic] streaming token break `
                        + `${JSON.stringify(tokenBreakLoss)}`,
                    );
                }
                // The result file truncates a long message; the channel does not.
                output.appendLine(
                    `[diagnostic] streaming last observation `
                    + `${JSON.stringify(lastStreamingObservation)}`,
                );
                throw new Error(
                    `${safeMessage(error)}; causal trace tail: `
                    + `${JSON.stringify(causalTrace.slice(-6))}; last observation: `
                    + JSON.stringify(lastStreamingObservation),
                );
            }
            const convergenceAfterWritersMs = Date.now() - writersStoppedAt;
            if (convergenceAfterWritersMs > 5000) {
                output.appendLine(
                    `[diagnostic] slow streaming causal trace `
                    + `${JSON.stringify(summarizeCausalTrace())}`,
                );
            }
            assert(
                convergenceAfterWritersMs <= 5000,
                `Streaming convergence exceeded 5 seconds: ${convergenceAfterWritersMs} ms`,
            );
            // The assertion above is intentionally before save(): journaled
            // text must project to disk without making a user save part of the
            // synchronization protocol.
            if (!autoSaveEnabled) {
                assert(
                    saveEventsBeforeExplicitSave === 0,
                    'Streaming fixture was saved before the explicit E2E save boundary',
                );
                assert(document.isDirty,
                    'Streaming fixture became clean before the explicit save boundary');
            }
            const dirtyBeforeExplicitSave = document.isDirty;
            await waitForAsync(async () => {
                const [local, state] = await Promise.all([
                    readLocalBytes(localUri),
                    replicaStateStore!.reload(),
                ]);
                if (!local) {
                    return false;
                }
                converged = remoteSession!.content;
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
            explicitSaveStarted = true;
            await saveDocument(document, 'Could not save the already-converged streaming document');
            saveEvents.dispose();
            // Once the real editor is clean, two consecutive remote revisions
            // must each reach both the open document and disk without another
            // explicit save. This catches logical-dirty state left behind by a
            // cross-window reload/projection.
            const cleanRemoteTokens = Array.from(
                {length: 2},
                (_, index) => `REMOTE-CLEAN-PULL-${request.nonce}-${index}\n`,
            );
            for (const token of cleanRemoteTokens) {
                const submittedAt = Date.now();
                const ancestor = remoteSession.content;
                const accepted = await remoteSession.acceptSnapshotAgainstAncestor(
                    ancestor,
                    `${ancestor}${token}`,
                    true,
                );
                await accepted.acknowledgement;
                const acknowledgedAt = Date.now();
                remoteTokens.push(token);
                expectedTokens.push(token);
                expectedUtf16Length += token.length;
                remoteJournaledEditCount += 1;
                let lastCleanPullObservation: Record<string, unknown> = {};
                try {
                    await waitForAsync(async () => {
                        const local = await readLocalBytes(localUri);
                        const disk = local === undefined
                            ? undefined
                            : Buffer.from(local).toString('utf8');
                        const editor = document.getText();
                        const remote = remoteSession!.content;
                        lastCleanPullObservation = {
                            diskPresent: disk !== undefined,
                            diskEqualsRemote: disk === remote,
                            editorEqualsRemote: editor === remote,
                            editorDirty: document.isDirty,
                            diskHasToken: disk?.includes(token) ?? false,
                            editorHasToken: editor.includes(token),
                            diskLength: disk?.length,
                            editorLength: editor.length,
                            remoteLength: remote.length,
                            diskSha256: disk === undefined ? undefined : createHash('sha256')
                                .update(disk).digest('hex'),
                            editorSha256: createHash('sha256').update(editor).digest('hex'),
                            remoteSha256: createHash('sha256').update(remote).digest('hex'),
                            remoteRevision: remoteSession!.revision,
                            runtime: LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath),
                        };
                        return disk === remote
                            && editor === remote
                            && document.isDirty === false
                            && disk.includes(token);
                    }, 10000, `project clean remote token ${token} to editor and disk`, 25);
                } catch (error) {
                    throw new Error(
                        `${safeMessage(error)}; last observation: `
                        + JSON.stringify(lastCleanPullObservation),
                    );
                }
                const observedAt = Date.now();
                remoteAdmissionToObservationLatencies.push(observedAt - submittedAt);
                remotePullLatencies.push(observedAt - acknowledgedAt);
                assertStreamingRuntimeHealthy();
            }
            converged = remoteSession.content;
            assert(
                containsAllTokens(converged)
                && preservesWriterOrder(converged)
                && converged.length === expectedUtf16Length,
                'Clean remote pull changed streaming content or writer order',
            );
            assert(sessionErrors.length === 0 && receiveErrors.length === 0,
                'A streaming remote session entered fail-closed state');
            assertStreamingRuntimeHealthy();
            const orderedPullLatencies = [...remotePullLatencies].sort((left, right) => left - right);
            const orderedAdmissionLatencies = [...remoteAdmissionToObservationLatencies]
                .sort((left, right) => left - right);
            const maxRemotePullLatencyMs = orderedPullLatencies.at(-1) ?? 0;
            const p95RemotePullLatencyMs = orderedPullLatencies[
                Math.floor((orderedPullLatencies.length - 1) * 0.95)
            ] ?? 0;
            evidence.streamingWriters = {
                path: relativePath,
                batchedLocalEditCount: batchedLocalTokens.length,
                batchedLocalOperationCount,
                localEditCount,
                editorChangeEventCount,
                remoteJournaledEditCount,
                aiCommittedOperationCount,
                rawFilesystemEditCount,
                finalRevision: remoteSession.revision,
                expectedUtf16Length,
                perWriterOrderPreserved: true,
                textIsolationEvents: 0,
                projectPauseEvents: 0,
                cleanRemotePullCount: cleanRemoteTokens.length,
                remoteObservedBeforeLocalStop,
                remoteAdmissionToObservationLatenciesMs: orderedAdmissionLatencies,
                remotePullLatenciesMs: orderedPullLatencies,
                maxRemotePullLatencyMs,
                p95RemotePullLatencyMs,
                sha256: createHash('sha256').update(converged).digest('hex'),
                savedToDisk: true,
                convergedBeforeExplicitSave: true,
                dirtyBeforeExplicitSave,
                saveEventsBeforeExplicitSave,
                autoSaveMode,
                aiOverlappedLocalAndRemote,
                rawFilesystemOverlappedLocalAndRemote,
                rangeReplaceVerified: true,
                rangeDeleteVerified: true,
                convergenceAfterWritersMs,
            };
            assert(
                maxRemotePullLatencyMs <= 5000,
                `Remote pull latency exceeded the realtime bound: ${maxRemotePullLatencyMs} ms`,
            );
        });

        await step('disjoint VS Code and AI paragraph edits converge and persist', async () => {
            const fileName = 'streaming.tex';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);
            const gatewayUri = vscode.Uri.joinPath(
                localRoot!, '.overleaf', 'safe-sync-gateway.json',
            );
            const document = await vscode.workspace.openTextDocument(localUri);
            const editor = await vscode.window.showTextDocument(document, {
                preview: false,
                preserveFocus: false,
            });
            const oldParagraphA = `PARAGRAPH-A-OLD-${request.nonce}\n`;
            const userParagraphA = `PARAGRAPH-A-USER-${request.nonce}\n`;
            const oldParagraphB = `PARAGRAPH-B-OLD-${request.nonce}\n`;
            const aiParagraphB = `PARAGRAPH-B-AI-${request.nonce}\n`;
            const seed = [
                `INTRO-${request.nonce}\n`,
                oldParagraphA,
                `MIDDLE-${request.nonce}\n`,
                oldParagraphB,
                `OUTRO-${request.nonce}\n`,
            ].join('');
            const userCandidate = seed.replace(oldParagraphA, userParagraphA);
            const aiCandidate = seed.replace(oldParagraphB, aiParagraphB);
            const expected = userCandidate.replace(oldParagraphB, aiParagraphB);
            const seedHash = createHash('sha256').update(seed).digest('hex');
            const expectedHash = createHash('sha256').update(expected).digest('hex');

            const baselineEdit = new vscode.WorkspaceEdit();
            baselineEdit.replace(
                localUri,
                new vscode.Range(
                    new vscode.Position(0, 0),
                    document.positionAt(document.getText().length),
                ),
                seed,
            );
            assert(await vscode.workspace.applyEdit(baselineEdit),
                'Could not establish the disjoint-paragraph common base');
            await saveDocument(document, 'Could not save the disjoint-paragraph common base');
            await waitForAsync(async () => {
                const [local, state] = await Promise.all([
                    readLocalBytes(localUri),
                    replicaStateStore!.reload(),
                ]);
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return local !== undefined
                    && Buffer.from(local).toString('utf8') === seed
                    && document.getText() === seed
                    && state.entries[relativePath]?.text === seed
                    && state.entries[relativePath]?.hash === seedHash
                    && !state.pendingAction
                    && !state.conflicts[relativePath]
                    && runtime?.settled === true;
            }, 60000, 'establish the disjoint-paragraph common baseline', 250);
            const runtimeAtParagraphStart = LocalReplicaSCMProvider.getRuntimeStatus(
                localRoot!.fsPath,
            );
            assert(runtimeAtParagraphStart,
                'Disjoint-paragraph test has no Local Replica runtime');
            const isolationEvents = runtimeAtParagraphStart.textIsolationEvents;
            const pauseEvents = runtimeAtParagraphStart.projectPauseEvents;

            interface ParagraphAiBegin {
                leaseId: string;
                fencingToken: number;
                content: string;
            }
            interface ParagraphAiCommit {
                journaled: boolean;
            }
            const begin = await sendAiGatewayRequest<ParagraphAiBegin>(gatewayUri.fsPath, {
                method: 'begin',
                path: relativePath,
                holder: `e2e-paragraph-ai-${request.nonce}`,
            });
            assert(begin.content === seed,
                'AI paragraph transaction did not begin from the shared common base');
            assert(aiCandidate.includes(oldParagraphA) && !aiCandidate.includes(userParagraphA),
                'AI paragraph candidate unexpectedly observed the later user edit');
            assert(userCandidate.includes(oldParagraphB) && !userCandidate.includes(aiParagraphB),
                'User paragraph candidate unexpectedly contains the later AI edit');

            const paragraphAOffset = document.getText().indexOf(oldParagraphA);
            assert(paragraphAOffset >= 0, 'User paragraph A is absent from the common base');
            const userApplied = await editor.edit(builder => {
                builder.replace(
                    new vscode.Range(
                        document.positionAt(paragraphAOffset),
                        document.positionAt(paragraphAOffset + oldParagraphA.length),
                    ),
                    userParagraphA,
                );
            }, {
                undoStopBefore: true,
                undoStopAfter: true,
            });
            assert(userApplied && document.getText() === userCandidate,
                'VS Code did not create the independent paragraph A candidate');

            let aiCommitJournaled = false;
            try {
                const committed = await sendAiGatewayRequest<ParagraphAiCommit>(gatewayUri.fsPath, {
                    method: 'commit',
                    path: relativePath,
                    leaseId: begin.leaseId,
                    fencingToken: begin.fencingToken,
                    content: aiCandidate,
                });
                assert(committed.journaled, 'AI paragraph B commit was not journaled');
                aiCommitJournaled = true;
            } finally {
                if (!aiCommitJournaled) {
                    await sendAiGatewayRequest(gatewayUri.fsPath, {
                        method: 'abort',
                        path: relativePath,
                        leaseId: begin.leaseId,
                        fencingToken: begin.fencingToken,
                    }).catch(() => {});
                }
            }

            await waitForAsync(async () => {
                const [local, state] = await Promise.all([
                    readLocalBytes(localUri),
                    replicaStateStore!.reload(),
                ]);
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return local !== undefined
                    && document.getText() === expected
                    && Buffer.from(local).toString('utf8') === expected
                    && state.entries[relativePath]?.text === expected
                    && state.entries[relativePath]?.hash === expectedHash
                    && !state.pendingAction
                    && !state.conflicts[relativePath]
                    && runtime?.settled === true
                    && runtime.textIsolationEvents === isolationEvents
                    && runtime.projectPauseEvents === pauseEvents;
            }, 60000, 'converge and persist disjoint user and AI paragraphs', 250);

            const remote = await freshDocument(
                server.server.url,
                api!,
                identity!,
                projectId!,
                runFolderId!,
                fileName,
            );
            assert(remote?.entityId === streamingDocumentId,
                'Disjoint-paragraph verification observed a replacement document');
            assert(remote.content === expected,
                'Overleaf did not retain both disjoint paragraph edits');
            const local = await readLocalBytes(localUri);
            const disk = local === undefined ? undefined : Buffer.from(local).toString('utf8');
            assert(document.getText() === expected && disk === expected,
                'Editor and disk did not retain both disjoint paragraph edits');
            const runtimeAfterParagraphs = LocalReplicaSCMProvider.getRuntimeStatus(
                localRoot!.fsPath,
            );
            assert(runtimeAfterParagraphs,
                'Disjoint-paragraph test lost the Local Replica runtime after convergence');
            assert(
                runtimeAfterParagraphs.textIsolationEvents === isolationEvents
                && runtimeAfterParagraphs.projectPauseEvents === pauseEvents,
                'Disjoint paragraph edits caused text isolation or a project pause',
            );
            assert(
                countExactOccurrences(expected, userParagraphA) === 1
                && countExactOccurrences(expected, aiParagraphB) === 1
                && !expected.includes(oldParagraphA)
                && !expected.includes(oldParagraphB),
                'Expected disjoint paragraph composition is malformed',
            );
            evidence.disjointParagraphEdits = {
                path: relativePath,
                commonBaseSha256: seedHash,
                expectedSha256: expectedHash,
                finalRevision: remote.revision,
                aiBeganBeforeUserEdit: true,
                candidatesSharedBase: true,
                aiCommitJournaled,
                convergedBeforeExplicitSave: true,
                editorEqualsDisk: document.getText() === disk,
                diskEqualsRemote: disk === remote.content,
                userParagraphOccurrences: countExactOccurrences(remote.content, userParagraphA),
                aiParagraphOccurrences: countExactOccurrences(remote.content, aiParagraphB),
                textIsolationEvents: runtimeAfterParagraphs.textIsolationEvents - isolationEvents,
                projectPauseEvents: runtimeAfterParagraphs.projectPauseEvents - pauseEvents,
            };
            await saveDocument(document,
                'Could not clean the converged disjoint-paragraph editor');
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

        await step('local binary cross-folder move preserves identity and bytes', async () => {
            const fileName = 'watcher-binary.png';
            const oldPath = `${isolatedRunName}/${fileName}`;
            const finalPath = `${isolatedRunName}/move-b/local-moved-binary.png`;
            const oldUri = vscode.Uri.joinPath(localRoot!, ...oldPath.split('/'));
            const finalUri = vscode.Uri.joinPath(localRoot!, ...finalPath.split('/'));
            const expectedBytes = Buffer.concat([
                Buffer.from(BINARY_FIXTURE),
                Buffer.from(`REMOTE-BINARY-${request.nonce}`, 'utf8'),
            ]);
            const expectedHash = createHash('sha256').update(expectedBytes).digest('hex');
            const before = await freshProject(
                server.server.url,
                api!,
                identity!,
                projectId!,
                'locate binary before local cross-folder move',
            );
            const oldParent = findFolder(before.rootFolder[0], runFolderId!);
            const resource = oldParent?.fileRefs.find(item => item.name === fileName);
            assert(resource, 'Binary move baseline is not a remote resource');
            const resourceId = resource._id;
            await vscode.workspace.fs.rename(oldUri, finalUri, {overwrite: false});
            await waitForAsync(async () => {
                const [project, state, local] = await Promise.all([
                    freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        'verify local binary cross-folder move',
                    ),
                    replicaStateStore!.reload(),
                    readLocalBytes(finalUri),
                ]);
                const moved = findEntity(project.rootFolder[0], resourceId);
                if (
                    moved?.parentId !== folderB._id
                    || moved.entity.name !== 'local-moved-binary.png'
                    || local === undefined
                ) {
                    return false;
                }
                const downloaded = await api!.getFile(identity!, projectId!, resourceId);
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return downloaded.type === 'success'
                    && downloaded.content !== undefined
                    && Buffer.from(downloaded.content).equals(expectedBytes)
                    && Buffer.from(local).equals(expectedBytes)
                    && state.entries[oldPath] === undefined
                    && state.entries[finalPath]?.entityId === resourceId
                    && state.entries[finalPath]?.fileType === 'file'
                    && state.entries[finalPath]?.hash === expectedHash
                    && !state.pendingAction
                    && !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName))
                    && runtime?.settled === true;
            }, 45000, 'settle local binary cross-folder move', 750);
            evidence.binaryMove = {
                entityId: resourceId,
                finalPath,
                sha256: expectedHash,
            };
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
            await waitForAsync(async () => {
                const [local, remote] = await Promise.all([
                    readLocalBytes(localUri),
                    freshDocument(
                    server.server.url,
                    api!,
                    identity!,
                    projectId!,
                    runFolderId!,
                    fileName,
                    ),
                ]);
                return local !== undefined
                    && Buffer.from(local).toString('utf8') === localToken
                    && remote?.content === localToken;
            }, 30000, 'push and project unsaved local edit after text conflict resolution', 750);
            await saveDocument(document, 'Could not save the already-synchronized local document');

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
                initialPath: relativePath,
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
            const exactResolvedContent = `${remoteToken}${localToken}`;
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
                    && converged === exactResolvedContent
                    && isExactComposition(converged, [localToken, remoteToken])
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

        await step('old document WAL survives immutable-id replacement and reattaches', async () => {
            const fileName = 'orphan-wal.tex';
            const relativePath = `${isolatedRunName}/${fileName}`;
            const localUri = vscode.Uri.joinPath(localRoot!, isolatedRunName, fileName);

            const created = await api!.addDoc(
                identity!, projectId!, runFolderId!, fileName,
            );
            assert(created.type === 'success' && created.entity,
                'Could not create old-ID WAL fixture');
            const oldDocumentId = created.entity._id;
            const commonBase = `ORPHAN-WAL-BASE-${request.nonce}\n`;
            await commitRemoteSnapshot(oldDocumentId, commonBase, 'orphan-base');
            const baseSha256 = createHash('sha256').update(commonBase).digest('hex');
            await waitForAsync(async () => {
                const [local, state] = await Promise.all([
                    readLocalBytes(localUri),
                    replicaStateStore!.reload(),
                ]);
                return local !== undefined
                    && Buffer.from(local).toString('utf8') === commonBase
                    && state.entries[relativePath]?.entityId === oldDocumentId
                    && state.entries[relativePath]?.fileType === 'doc'
                    && state.entries[relativePath]?.hash === baseSha256
                    && !state.pendingAction
                    && !state.conflicts[relativePath];
            }, 30000, 'establish old-ID WAL common base', 500);

            // The manifest base comes from this baseline entry, and it has
            // been arriving empty. Sample it at each stage so the regression
            // is attributed to a stage instead of inferred from the wreckage.
            const baselineStages: Array<Record<string, unknown>> = [];
            const sampleBaseline = async (stage: string): Promise<void> => {
                const sampled = (await replicaStateStore!.reload()).entries[relativePath];
                baselineStages.push({
                    stage,
                    entityId: sampled?.entityId,
                    hash: sampled?.hash,
                    textLength: sampled?.text?.length,
                });
            };
            await sampleBaseline('common-base-established');

            const document = await vscode.workspace.openTextDocument(localUri);
            await vscode.window.showTextDocument(document, {
                preview: false,
                preserveFocus: false,
            });
            await LocalReplicaSCMProvider.suspendE2EDocumentTransport(
                localRoot!.fsPath,
                relativePath,
            );
            const pendingToken = `LOCAL-PENDING-OLD-ID-${request.nonce}\n`;
            const pendingEdit = new vscode.WorkspaceEdit();
            pendingEdit.insert(
                localUri,
                document.positionAt(document.getText().length),
                pendingToken,
            );
            assert(await vscode.workspace.applyEdit(pendingEdit),
                'Could not create unsaved old-ID WAL edit');
            const visibleCandidate = `${commonBase}${pendingToken}`;
            const visibleSha256 = createHash('sha256').update(visibleCandidate).digest('hex');
            await waitForAsync(async () => {
                const disk = await readLocalBytes(localUri);
                return document.isDirty
                    && document.getText() === visibleCandidate
                    && disk !== undefined
                    && Buffer.from(disk).toString('utf8') === visibleCandidate
                    && await LocalReplicaSCMProvider.hasPendingE2EDocumentRecovery(
                        localRoot!.fsPath,
                        relativePath,
                    );
            }, 15000, 'journal and project unsaved old-ID edit while transport is suspended', 100);
            await sampleBaseline('pending-wal-journaled');

            const removed = await api!.deleteEntity(
                identity!, projectId!, 'doc', oldDocumentId,
            );
            assert(removed.type === 'success', 'Could not delete the old immutable document id');
            await sampleBaseline('old-id-deleted');
            const replacement = await api!.addDoc(
                identity!, projectId!, runFolderId!, fileName,
            );
            assert(replacement.type === 'success' && replacement.entity,
                'Could not recreate the WAL fixture under a new document id');
            const replacementDocumentId = replacement.entity._id;
            assert(replacementDocumentId !== oldDocumentId,
                'Remote replacement reused the old immutable document id');
            await sampleBaseline('replacement-created-empty');
            const replacementContent = `REMOTE-REPLACEMENT-${request.nonce}\n`;
            await commitRemoteSnapshot(
                replacementDocumentId,
                replacementContent,
                'orphan-replacement',
            );
            await sampleBaseline('replacement-content-committed');
            const replacementSha256 = createHash('sha256')
                .update(replacementContent)
                .digest('hex');

            let artifactDirectory = '';
            let baselineAtIsolation: unknown;
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
                const record = state.conflicts[relativePath];
                if (!record || state.pendingAction || !remote) {
                    return false;
                }
                artifactDirectory = record.artifactDirectory;
                // The manifest takes its base side straight from the baseline
                // entry, so when the archived base is wrong the question is
                // what the baseline held by then. Record it with the record's
                // own base evidence.
                baselineAtIsolation = {
                    entry: state.entries[relativePath],
                    recordBaseHash: record.baseHash,
                    recordBaseFile: record.baseFile,
                    recordBaselineEntityId: record.baselineEntityId,
                };
                return remote.entityId === replacementDocumentId
                    && remote.content === replacementContent
                    && typeof record.reason === 'string'
                    && record.reason.length > 0;
            }, 45000, 'isolate old-ID pending WAL after same-path replacement', 500);
            evidence.orphanWalBaselineAtIsolation = {
                stages: baselineStages,
                atIsolation: baselineAtIsolation,
            };

            const resolvedArtifactDirectory = await LocalReplicaSCMProvider
                .resolveE2EConflictUsingRemote(localRoot!.fsPath, relativePath);
            assert(
                resolvedArtifactDirectory === artifactDirectory.replace(
                    '.overleaf/conflicts/unresolved/',
                    '.overleaf/conflicts/resolved/',
                ),
                'Old-ID WAL artifacts were not moved to the resolved archive',
            );
            const manifestUri = vscode.Uri.joinPath(
                localRoot!,
                ...resolvedArtifactDirectory.split('/'),
                'manifest.json',
            );
            const manifestBytes = await readLocalBytes(manifestUri);
            assert(manifestBytes, 'Resolved old-ID WAL manifest is missing');
            const verified = assertResolvedOrphanWalManifest(
                JSON.parse(Buffer.from(manifestBytes).toString('utf8')),
                {
                    path: relativePath,
                    oldDocumentId,
                    replacementDocumentId,
                    baseSha256,
                    acknowledgedSha256: baseSha256,
                    visibleSha256,
                    replacementSha256,
                },
            );
            for (const [label, filePath, expectedContent] of [
                ['base', verified.baseFile, commonBase],
                ['acknowledged', verified.acknowledgedFile, commonBase],
                ['visible', verified.visibleFile, visibleCandidate],
                ['local', verified.localFile, visibleCandidate],
                ['replacement', verified.remoteFile, replacementContent],
            ] as const) {
                assert(filePath.startsWith(`${resolvedArtifactDirectory}/`),
                    `${label} WAL evidence escaped its resolved artifact directory`);
                const bytes = await readLocalBytes(vscode.Uri.joinPath(
                    localRoot!, ...filePath.split('/'),
                ));
                assert(bytes !== undefined
                    && Buffer.from(bytes).toString('utf8') === expectedContent,
                `${label} WAL evidence changed after remote resolution`);
            }

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
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return local !== undefined
                    && Buffer.from(local).toString('utf8') === replacementContent
                    && document.getText() === replacementContent
                    && remote?.entityId === replacementDocumentId
                    && remote.content === replacementContent
                    && state.entries[relativePath]?.entityId === replacementDocumentId
                    && !state.conflicts[relativePath]
                    && !state.pendingAction
                    && runtime?.settled === true;
            }, 45000, 'reattach the resolved path to its replacement document id', 500);

            const localAfterResolution = `LOCAL-NEW-ID-${request.nonce}\n`;
            const localToRemote = `${replacementContent}${localAfterResolution}`;
            const newIdLocalEdit = new vscode.WorkspaceEdit();
            newIdLocalEdit.insert(
                localUri,
                document.positionAt(document.getText().length),
                localAfterResolution,
            );
            assert(await vscode.workspace.applyEdit(newIdLocalEdit),
                'Could not edit the replacement document locally');
            assert(document.isDirty, 'Replacement local edit was saved before synchronization');
            const pushLabel = 'push unsaved local edit through the replacement document id';
            // What this proves is that the edit crosses to the replacement
            // document without waiting for a save. Requiring the buffer to
            // still be dirty at the end cannot express that: VS Code's own
            // afterDelay auto-save clears the marker about a second in, long
            // before a 30s propagation wait can finish, and that save is the
            // host's, not safe-sync's. Record instead whether the remote
            // already held the bytes while the buffer was still unsaved.
            let remoteMatchedWhileDirty = false;
            try {
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
                    if (
                        document.isDirty
                        && remote?.entityId === replacementDocumentId
                        && remote.content === localToRemote
                    ) {
                        remoteMatchedWhileDirty = true;
                    }
                    return document.getText() === localToRemote
                        && local !== undefined
                        && Buffer.from(local).toString('utf8') === localToRemote
                        && remote?.entityId === replacementDocumentId
                        && remote.content === localToRemote
                        && state.entries[relativePath]?.entityId === replacementDocumentId
                        && !state.pendingAction
                        && !state.conflicts[relativePath];
                }, 30000, pushLabel, 250);
                // Polling HTTP cannot reliably witness that window: the journal
                // accepts the edit and auto-save follows within about a second,
                // and a single fresh-document read can take longer than that.
                // The actor's own trace states the ordering exactly.
                const trace = LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                    localRoot!.fsPath,
                    relativePath,
                );
                const editIndex = trace.findIndex(event => (
                    event.kind === 'editor-observed' && event.editor === localToRemote
                ));
                assert(editIndex >= 0, 'The replacement local edit never reached the text actor');
                const after = trace.slice(editIndex);
                const acceptedOffset = after.findIndex(event => (
                    event.kind.endsWith('-accepted') && event.canonicalAfter === localToRemote
                ));
                const savedOffset = after.findIndex(event => event.kind === 'save-did');
                assert(
                    acceptedOffset >= 0 && (savedOffset < 0 || acceptedOffset < savedOffset),
                    'Replacement document only received the local edit after the buffer was saved'
                    + ` (accepted at ${acceptedOffset}, saved at ${savedOffset},`
                    + ` remoteMatchedWhileDirty=${remoteMatchedWhileDirty})`,
                );
            } catch (error) {
                // This wait failing says only that some conjunct never held.
                // Which one separates a document whose transport never resumed
                // from one whose editor observation never reached the journal,
                // and the two have nothing in common, so name the failing side
                // and the transitions the text actor did see.
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
                output.appendLine(`[diagnostic] ${pushLabel}: ${JSON.stringify({
                    dirty: document.isDirty,
                    remoteMatchedWhileDirty,
                    editorMatches: document.getText() === localToRemote,
                    editorLength: document.getText().length,
                    diskMatches: local !== undefined
                        && Buffer.from(local).toString('utf8') === localToRemote,
                    diskLength: local?.length,
                    remoteEntityId: remote?.entityId,
                    replacementDocumentId,
                    remoteMatches: remote?.content === localToRemote,
                    remoteLength: remote?.content?.length,
                    stateEntityId: state.entries[relativePath]?.entityId,
                    pendingAction: state.pendingAction,
                    conflict: state.conflicts[relativePath] !== undefined,
                    runtime: LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath),
                })}`);
                output.appendLine(`[diagnostic] ${pushLabel}: trace ${JSON.stringify(
                    LocalReplicaSCMProvider.inspectE2ETextDiagnostics(
                        localRoot!.fsPath,
                        relativePath,
                    ).slice(-120).map(event => ({
                        sequence: event.sequence,
                        elapsedMs: event.elapsedMs,
                        kind: event.kind,
                        reason: event.reason,
                        classification: event.classification,
                        ancestorAccountedFor: event.ancestorAccountedFor,
                        editorDirty: event.editorDirty,
                        bufferedInput: event.bufferedInput,
                        canonicalLength: event.canonicalBefore?.length,
                        canonicalAfterLength: event.canonicalAfter?.length,
                        editorLength: event.editor?.length,
                        diskLength: event.diskBefore?.length,
                        observedLength: event.observed?.length,
                        targetLength: event.target?.length,
                    })),
                )}`);
                throw error;
            }

            const remoteAfterResolution = `REMOTE-NEW-ID-${request.nonce}\n${localToRemote}`;
            await commitRemoteSnapshot(
                replacementDocumentId,
                remoteAfterResolution,
                'orphan-post-resolution-pull',
            );
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
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return document.getText() === remoteAfterResolution
                    && local !== undefined
                    && Buffer.from(local).toString('utf8') === remoteAfterResolution
                    && remote?.entityId === replacementDocumentId
                    && remote.content === remoteAfterResolution
                    && state.entries[relativePath]?.entityId === replacementDocumentId
                    && !state.pendingAction
                    && !state.conflicts[relativePath]
                    && runtime?.settled === true;
            }, 30000, 'pull exact remote edit through the replacement document id', 250);
            await saveDocument(document, 'Could not clean the converged replacement document');

            evidence.orphanDocumentWalRecovery = {
                path: relativePath,
                oldDocumentId,
                replacementDocumentId,
                reason: verified.reason,
                oldRevision: verified.oldRevision,
                replacementRevision: verified.replacementRevision,
                pendingOperationCount: verified.pendingOperationCount,
                baseSha256,
                acknowledgedSha256: baseSha256,
                visibleSha256,
                replacementSha256,
                localToRemoteSha256: createHash('sha256').update(localToRemote).digest('hex'),
                remoteToLocalSha256: createHash('sha256')
                    .update(remoteAfterResolution)
                    .digest('hex'),
                resolvedArtifactDirectory,
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
            const movedContent = `REMOTE-EDIT-DURING-MOVE-${request.nonce}\n`;
            await commitRemoteSnapshot(
                movedDocumentId,
                movedContent,
                'remote-edit-during-move',
            );

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
            await waitForAsync(async () => {
                const projected = await readLocalBytes(remoteMovedLocalUri);
                return projected !== undefined
                    && Buffer.from(projected).toString('utf8') === movedContent
                    && !await localPathExists(oldLocalUri);
            }, 30000, 'apply the remote move, rename, and concurrent edit to Local Replica');

            const moveEditor = new SocketIOAPI(
                server.server.url, api!, identity!, projectId!,
            );
            sockets.push(moveEditor);
            let moveSession: DurableDocumentSession | undefined;
            const moveReceiveErrors: Error[] = [];
            moveEditor.updateEventHandlers({
                onFileChanged: update => {
                    moveSession?.receiveUpdate(update).catch(error => moveReceiveErrors.push(error));
                },
            });
            await joinProjectWithProtocolFallback(
                moveEditor, projectId!, 'join deterministic move/edit writer',
            );
            await waitFor(
                () => moveEditor.sourceId !== undefined,
                5000,
                'receive deterministic move/edit source id',
            );
            const moveJoined = await withTimeout(
                moveEditor.joinDoc(movedDocumentId),
                10000,
                'join deterministic move/edit document',
            );
            assert(moveJoined.docLines.join('\n') === movedContent,
                'Move/edit writer joined an unexpected baseline');
            const moveSessionErrors: Error[] = [];
            const moveJournal = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${movedDocumentId}-deterministic-move-edit`,
                initialRevision: moveJoined.version,
                initialContent: movedContent,
                initialPath: `${isolatedRunName}/move-b/after-move.tex`,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            moveSession = await DurableDocumentSession.create({
                documentId: movedDocumentId,
                authoritativeContent: movedContent,
                authoritativeRevision: moveJoined.version,
                journal: moveJournal,
                sendUpdate: update => moveEditor.applyOtUpdate(
                    movedDocumentId,
                    update as UpdateSchema,
                ),
                getSourceId: () => moveEditor.sourceId,
                createOperationId: () => `e2e:${request.nonce}:move-edit:${randomUUID()}`,
                onChange: () => {},
                onError: error => moveSessionErrors.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            sessions.push(moveSession);

            const localMovedUri = vscode.Uri.joinPath(
                localRoot!, isolatedRunName, 'move-a', 'local-moved.tex',
            );
            const finalPath = `${isolatedRunName}/move-a/local-moved.tex`;
            const oldPath = `${isolatedRunName}/move-b/after-move.tex`;
            const stateBeforeConcurrentMove = await replicaStateStore!.reload();
            assert(
                stateBeforeConcurrentMove.entries[oldPath]?.entityId === movedDocumentId
                && stateBeforeConcurrentMove.entries[finalPath] === undefined,
                'Move/edit barrier did not start from the old stable path',
            );
            const concurrentRemoteToken = `REMOTE-EDIT-DURING-LOCAL-MOVE-${request.nonce}\n`;
            const finalContent = `${movedContent}${concurrentRemoteToken}`;
            let releaseMoveBarrier!: () => void;
            const moveBarrier = new Promise<void>(resolve => {
                releaseMoveBarrier = resolve;
            });
            const localMove = (async () => {
                await moveBarrier;
                await vscode.workspace.fs.rename(
                    remoteMovedLocalUri,
                    localMovedUri,
                    {overwrite: false},
                );
            })();
            const concurrentRemoteEdit = (async () => {
                await moveBarrier;
                await moveSession!.submitSnapshot(finalContent, true);
            })();
            releaseMoveBarrier();
            await Promise.all([localMove, concurrentRemoteEdit]);
            await waitForAsync(async () => {
                const [refreshed, remoteDocument, state] = await Promise.all([
                    freshProject(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        'verify Local Replica rename on remote',
                    ),
                    freshDocument(
                        server.server.url,
                        api!,
                        identity!,
                        projectId!,
                        folderA._id,
                        'local-moved.tex',
                    ),
                    replicaStateStore!.reload(),
                ]);
                const remoteEntity = findEntity(refreshed.rootFolder[0], movedDocumentId);
                const runtime = LocalReplicaSCMProvider.getRuntimeStatus(localRoot!.fsPath);
                return remoteEntity?.parentId === folderA._id
                    && remoteEntity.entity.name === 'local-moved.tex'
                    && remoteDocument?.entityId === movedDocumentId
                    && remoteDocument.content === finalContent
                    && state.entries[finalPath]?.entityId === movedDocumentId
                    && state.entries[oldPath] === undefined
                    && !state.pendingAction
                    && !Object.keys(state.conflicts)
                        .some(filePath => isE2EConflictPath(filePath, isolatedRunName))
                    && runtime?.settled === true;
            }, 45000, 'converge the barriered local move and remote edit', 750);
            const state = await replicaStateStore!.reload();
            assert(!state.pendingAction && !Object.keys(state.conflicts)
                .some(filePath => isE2EConflictPath(filePath, isolatedRunName)),
                'Move/rename left a pending action or conflict');
            assert(state.entries[finalPath]?.entityId === movedDocumentId,
                'Move/rename lost the stable remote entity id in the replica baseline');
            assert(moveSessionErrors.length === 0 && moveReceiveErrors.length === 0,
                'Barriered move/edit writer entered fail-closed state');
            evidence.moveWithConcurrentEdit = {
                entityId: movedDocumentId,
                finalPath,
                sha256: createHash('sha256').update(finalContent).digest('hex'),
            };
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
                initialPath: `${isolatedRunName}/concurrent.tex`,
                walOptions: {idleCommitMs: 1, maxCommitMs: 5},
            });
            const journalB = await DocumentJournal.open({
                directory: e2eJournalDirectory,
                documentId: `${documentId}-client-b`,
                initialRevision: baseRevision,
                initialContent: baseB,
                initialPath: `${isolatedRunName}/concurrent.tex`,
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
            assert(
                (converged === `${left}${right}` || converged === `${right}${left}`)
                && isExactComposition(converged, [left, right]),
                'Converged document duplicated, corrupted, or lost a concurrent insertion',
            );

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
        if (restartRecoveryMarker) {
            output.appendLine(
                `[warn] preserving restart-recovery fixture ${restartRecoveryMarker.namespace} `
                + `and operation ${restartRecoveryMarker.pendingOperationId} for inspection.`,
            );
        } else if (api && identity && projectId && serverUrl && runName) {
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
    let validatedRequest: E2ERequest | undefined;
    try {
        validatedRequest = validateRequest(requestValue);
    } catch {
        // Claim malformed input below so execute() records a redacted failure.
    }
    if (validatedRequest && !await workspaceTargetsApprovedReplica(validatedRequest)) {
        output.appendLine(
            `E2E request left unclaimed: open Local Replica ${validatedRequest.projectName}`
            + ` on ${validatedRequest.serverName}.`,
        );
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
            serverName: typeof request.serverName === 'string'
                ? request.serverName.slice(0, 512)
                : 'invalid-server',
            projectName: typeof request.projectName === 'string'
                ? request.projectName.slice(0, 512)
                : 'invalid-project',
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
        ? `Safe-sync E2E passed for ${result.projectName} on ${result.serverName}.`
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
        const target = await currentWorkspaceE2ETarget();
        const request: E2ERequest = {
            schemaVersion: 3,
            ...target,
            nonce: `${Date.now()}-${process.pid}`,
            expectedExtensionId: extension.extensionId,
            expectedExtensionVersion: extension.extensionVersion,
            expectedBuildId: extension.buildId,
            expectedRunnerSha256: extension.runnerSha256,
        };
        output.show(true);
        output.appendLine(
            `Starting approved safe-sync E2E with ${extension.extensionId}`
            + `@${extension.extensionVersion} (${extension.buildId}) for `
            + `${target.projectName} on ${target.serverName}.`,
        );
        const requestUri = vscode.Uri.joinPath(context.globalStorageUri, REQUEST_FILE);
        const runningUri = vscode.Uri.joinPath(context.globalStorageUri, RUNNING_FILE);
        const restartUri = vscode.Uri.joinPath(context.globalStorageUri, RESTART_RECOVERY_FILE);
        const [queued, active, restart] = await Promise.all([
            readJson<unknown>(requestUri),
            readJson<unknown>(runningUri),
            readJson<unknown>(restartUri),
        ]);
        assert(queued === undefined && active === undefined && restart === undefined,
            'Another approved E2E or restart-recovery proof requires inspection');
        await writeJsonAtomic(requestUri, request);
        await runRequestedE2E(context, output);
        return readJson<E2EResult>(vscode.Uri.joinPath(context.globalStorageUri, RESULT_FILE));
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
