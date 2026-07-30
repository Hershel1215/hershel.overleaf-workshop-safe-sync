import * as vscode from 'vscode';
import { BaseAPI, Identity, ProjectPersist } from '../api/base';
import { SocketIOAPI, UpdateSchema } from '../api/socketio';
import { FolderEntity, ProjectEntity } from '../core/remoteFileSystemProvider';
import { ROOT_NAME } from '../consts';
import { GlobalStateManager } from '../utils/globalStateManager';
import { DocumentSession } from './documentSession';
import { TextOperation } from './textOperation';

const APPROVED_SERVER = '10.10.5.4:8888';
const APPROVED_PROJECT = 'hershel-safe-sync-e2e';
const REQUEST_FILE = 'safe-sync-e2e.request.json';
const RUNNING_FILE = 'safe-sync-e2e.running.json';
const RESULT_FILE = 'safe-sync-e2e.result.json';
const BINARY_FIXTURE = Uint8Array.from(Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
    'base64',
));

interface E2ERequest {
    schemaVersion: 1;
    serverName: typeof APPROVED_SERVER;
    projectName: typeof APPROVED_PROJECT;
    nonce: string;
}

interface E2EStep {
    name: string;
    ok: boolean;
    durationMs: number;
    detail?: string;
}

interface E2EResult {
    schemaVersion: 1;
    status: 'passed' | 'failed';
    serverName: string;
    projectName: string;
    projectId?: string;
    startedAt: string;
    finishedAt: string;
    steps: E2EStep[];
    error?: string;
}

let running: Promise<E2EResult> | undefined;

function safeMessage(error: unknown): string {
    const raw = error instanceof Error ? error.message : String(error);
    return raw
        .replace(/((?:cookie|csrf|token)\s*[:=]\s*)[^\s,;]+/gi, '$1[redacted]')
        .slice(0, 2000);
}

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) {
        throw new Error(message);
    }
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
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
    const temporary = uri.with({path: `${uri.path}.${process.pid}.tmp`});
    await vscode.workspace.fs.writeFile(
        temporary,
        Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8'),
    );
    await vscode.workspace.fs.rename(temporary, uri, {overwrite: true});
}

function validateRequest(value: unknown): E2ERequest {
    const request = value as Partial<E2ERequest>;
    assert(request?.schemaVersion === 1, 'Unsupported E2E request schema');
    assert(request.serverName === APPROVED_SERVER, 'E2E request targets an unapproved server');
    assert(request.projectName === APPROVED_PROJECT, 'E2E request targets an unapproved project');
    assert(
        typeof request.nonce === 'string' && /^[a-zA-Z0-9_-]{8,80}$/.test(request.nonce),
        'Invalid E2E request nonce',
    );
    return request as E2ERequest;
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
    const steps: E2EStep[] = [];
    let projectId: string | undefined;
    let runFolderId: string | undefined;
    let api: BaseAPI | undefined;
    let identity: Identity | undefined;
    const sockets: SocketIOAPI[] = [];
    const sessions: DocumentSession[] = [];

    const step = async <T>(name: string, action: () => Promise<T>): Promise<T> => {
        const started = Date.now();
        try {
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

    try {
        const server = await step('approved target and SecretStorage login', async () => {
            const match = GlobalStateManager.getServers(context)
                .find(item => item.server.name === APPROVED_SERVER);
            assert(match, `Server ${APPROVED_SERVER} is not configured`);
            const url = new URL(match.server.url);
            assert(url.hostname === '10.10.5.4' && url.port === '8888', 'Configured server URL is outside the approved target');
            identity = await GlobalStateManager.authenticate(context, APPROVED_SERVER);
            api = match.api;
            return match;
        });

        await step('create or reuse retained E2E project', async () => {
            const selected = await selectProject(context, api!, identity!);
            projectId = selected.projectId;
            assert(/^[a-zA-Z0-9_-]+$/.test(projectId), 'Server returned an invalid project id');
        });

        let runFolder!: FolderEntity;
        let documentId!: string;
        let binaryId!: string;
        let folderA!: FolderEntity;
        let folderB!: FolderEntity;
        let movedDocumentId!: string;
        const runName = `hershel-e2e-${request.nonce}`;

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

            const folderResponse = await api!.addFolder(identity!, projectId!, runName, root._id);
            assert(folderResponse.type === 'success' && folderResponse.entity, 'Could not create E2E run folder');
            runFolder = folderResponse.entity as FolderEntity;
            runFolderId = runFolder._id;

            const document = await api!.addDoc(identity!, projectId!, runFolder._id, 'concurrent.tex');
            assert(document.type === 'success' && document.entity, 'Could not create E2E document');
            documentId = document.entity._id;

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

        await step('binary upload and authoritative readback', async () => {
            const downloaded = await api!.getFile(identity!, projectId!, binaryId);
            assert(downloaded.type === 'success' && downloaded.content, 'Could not read binary fixture');
            assert(
                Buffer.from(downloaded.content).equals(Buffer.from(BINARY_FIXTURE)),
                'Binary readback differs byte-for-byte',
            );
        });

        await step('stable entity id across remote move and rename', async () => {
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
        });

        await step('two-client concurrent OT convergence', async () => {
            const socketA = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            const socketB = new SocketIOAPI(server.server.url, api!, identity!, projectId!);
            sockets.push(socketA, socketB);

            let sessionA: DocumentSession | undefined;
            let sessionB: DocumentSession | undefined;
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
            sessionA = await DocumentSession.create({
                documentId,
                content: baseA,
                revision: baseRevision,
                sendUpdate: update => socketA.applyOtUpdate(documentId, update as UpdateSchema),
                persist: async () => {},
                getSourceId: () => socketA.sourceId,
                onChange: () => {},
                onError: error => failures.push(error),
                retryDelayMs: 3000,
                fatalDelayMs: 15000,
            });
            sessionB = await DocumentSession.create({
                documentId,
                content: baseB,
                revision: baseRevision,
                sendUpdate: update => socketB.applyOtUpdate(documentId, update as UpdateSchema),
                persist: async () => {},
                getSourceId: () => socketB.sourceId,
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
        });

        await step('cleanup isolated fixtures while retaining project', async () => {
            const response = await api!.deleteEntity(identity!, projectId!, 'folder', runFolderId!);
            assert(response.type === 'success', `Fixture cleanup failed: ${response.message ?? 'unknown error'}`);
            runFolderId = undefined;
        });

        return {
            schemaVersion: 1,
            status: 'passed',
            serverName: request.serverName,
            projectName: request.projectName,
            projectId,
            startedAt,
            finishedAt: new Date().toISOString(),
            steps,
        };
    } catch (error) {
        if (runFolderId && api && identity && projectId) {
            try {
                await api.deleteEntity(identity, projectId, 'folder', runFolderId);
            } catch {
                // The failed fixture remains isolated under its nonce for inspection.
            }
        }
        return {
            schemaVersion: 1,
            status: 'failed',
            serverName: request.serverName,
            projectName: request.projectName,
            projectId,
            startedAt,
            finishedAt: new Date().toISOString(),
            steps,
            error: safeMessage(error),
        };
    } finally {
        for (const session of sessions) {
            session.dispose();
        }
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
    if (running) {
        return running;
    }
    const request = validateRequest(requestValue);
    running = runRemoteE2E(context, request, output);
    try {
        return await running;
    } finally {
        running = undefined;
    }
}

async function runRequestedE2E(
    context: vscode.ExtensionContext,
    output: vscode.OutputChannel,
): Promise<void> {
    const requestUri = vscode.Uri.joinPath(context.globalStorageUri, REQUEST_FILE);
    const runningUri = vscode.Uri.joinPath(context.globalStorageUri, RUNNING_FILE);
    const resultUri = vscode.Uri.joinPath(context.globalStorageUri, RESULT_FILE);
    const requestValue = await readJson<unknown>(requestUri);
    if (requestValue === undefined) {
        return;
    }

    await vscode.workspace.fs.createDirectory(context.globalStorageUri);
    await vscode.workspace.fs.rename(requestUri, runningUri, {overwrite: true});
    output.show(true);
    output.appendLine('Starting approved one-time safe-sync E2E run. Credentials remain in VS Code SecretStorage.');
    let result: E2EResult;
    try {
        result = await execute(context, requestValue, output);
    } catch (error) {
        const now = new Date().toISOString();
        result = {
            schemaVersion: 1,
            status: 'failed',
            serverName: APPROVED_SERVER,
            projectName: APPROVED_PROJECT,
            startedAt: now,
            finishedAt: now,
            steps: [],
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
        const request: E2ERequest = {
            schemaVersion: 1,
            serverName: APPROVED_SERVER,
            projectName: APPROVED_PROJECT,
            nonce: `${Date.now()}-${process.pid}`,
        };
        output.show(true);
        const result = await execute(context, request, output);
        await writeJsonAtomic(vscode.Uri.joinPath(context.globalStorageUri, RESULT_FILE), result);
        return result;
    });
    runRequestedE2E(context, output).catch(error => {
        output.appendLine(`[fail] could not process E2E request: ${safeMessage(error)}`);
    });
    return [output, command];
}
