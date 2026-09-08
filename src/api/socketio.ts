/* eslint-disable @typescript-eslint/naming-convention */
import { Identity, BaseAPI, ProjectMessageResponseSchema } from './base';
import { FileEntity, DocumentEntity, FileRefEntity, FileType, FolderEntity, ProjectEntity } from '../core/remoteFileSystemProvider';
import { SocketIOAlt } from './socketioAlt';

function decodePackedUtf8(text: string): string {
    return Buffer.from(text, 'latin1').toString('utf-8');
}

export interface UpdateUserSchema {
    id: string,
    user_id: string,
    name: string,
    email: string,
    doc_id: string,
    row: number,
    column: number,
    last_updated_at?: number, //unix timestamp
}

export interface OnlineUserSchema {
    client_age: number,
    client_id: string,
    connected: boolean,
    cursorData?: {
        column: number,
        doc_id: string,
        row: number,
    },
    email: string,
    first_name: string,
    last_name?: string,
    last_updated_at: string, //unix timestamp
    user_id: string,
}

export interface UpdateSchema {
    doc: string, //doc id
    op?: {
        p: number, //position
        i?: string, //insert
        d?: string, //delete
        u?: boolean, //isUndo
    }[],
    v: number, //doc version number
    lastV?: number, //last version number
    hash?: string, //(not needed if lastV is provided)
    dupIfSource?: string[], //deduplicate retries submitted by earlier socket ids
    error?: string,
    meta?: {
        source?: string, //socketio client id
        ts?: number, //unix timestamp
        user_id?: string,
        [key:string]: unknown,
    }
}

export interface EventsHandler {
    onFileCreated?: (parentFolderId:string, type:FileType, entity:FileEntity) => void,
    onFileRenamed?: (entityId:string, newName:string) => void,
    onFileRemoved?: (entityId:string) => void,
    onFileMoved?: (entityId:string, newParentFolderId:string) => void,
    onFileChanged?: (update:UpdateSchema) => void,
    //
    onDisconnected?: () => void,
    onConnectionAccepted?: (publicId:string) => void,
    onClientUpdated?: (user:UpdateUserSchema) => void,
    onClientDisconnected?: (id:string) => void,
    //
    onReceivedMessage?: (message:ProjectMessageResponseSchema) => void,
    //
    onSpellCheckLanguageUpdated?: (language:string) => void,
    onCompilerUpdated?: (compiler:string) => void,
    onRootDocUpdated?: (rootDocId:string) => void,
}

type ConnectionScheme = 'Alt' | 'v1' | 'v2';

/**
 * An acknowledgement timeout is a transport observation failure, not an OT
 * rejection.  Keeping it typed lets callers retry reads/reconnect without
 * confusing it with an ancestry or content invariant failure.
 */
export class SocketIOTimeoutError extends Error {
    readonly code = 'SOCKET_IO_TIMEOUT';

    constructor(readonly event: string, readonly timeoutMs = 10000) {
        super(`Socket.IO ${event} acknowledgement timed out after ${timeoutMs} ms`);
        this.name = 'SocketIOTimeoutError';
    }
}

/**
 * A membership RPC refused because the socket it would travel on is known to
 * be unusable. It is a timeout in the sense callers care about — the server
 * epoch is unknowable and the transport must be rebuilt — but no time was
 * spent waiting, and reporting it as an acknowledgement that "timed out after
 * 0 ms" hides the one fact that matters, which is that retrying this socket
 * can never succeed.
 */
export class TransportInvalidatedError extends SocketIOTimeoutError {
    constructor(event: string) {
        super(event, 0);
        this.name = 'TransportInvalidatedError';
        this.message = `Socket.IO ${event} refused: `
            + 'the socket transport was invalidated and must be rebuilt';
    }
}

export function isSocketIOTimeout(error: unknown): boolean {
    return error instanceof SocketIOTimeoutError
        || (typeof error === 'string' && error === 'timeout')
        || (error instanceof Error && error.name === 'SocketIOTimeoutError');
}

const SOCKET_EVENT_TIMEOUT_MS = 10000;

export class SocketIOAPI {
    private scheme: ConnectionScheme = 'v1';
    private record?: Promise<ProjectEntity>;
    private _handlers: Array<EventsHandler> = [];
    /**
     * Overleaf guards joinDoc/leaveDoc with one joinLeaveEpoch per socket.
     * Serialize only those membership RPCs; document updates remain concurrent.
     */
    private membershipChain: Promise<void> = Promise.resolve();

    private socket?: any;
    private emit: any;
    private _publicId?: string;
    /** Track the scheme used when the socket was last initialized */
    private _socketInitScheme?: ConnectionScheme;
    /** A timed-out membership RPC leaves the server-side epoch unknowable. */
    private transportInvalidated = false;
    /** Whether the current socket generation already told handlers it went away. */
    private disconnectAnnounced = false;
    private generation = 0;
    private retired = false;

    constructor(private url:string,
                private readonly api:BaseAPI,
                private readonly identity:Identity,
                private readonly projectId:string)
    {
        this.init();
    }

    init() {
        if (this.retired) {
            throw new Error('Socket.IO client is permanently retired');
        }
        const generation = ++this.generation;
        this._publicId = undefined;

        if (this.socket) {
            this.permanentlyRetireSocket(this.socket);
        }

        // connect
        switch(this.scheme) {
            case 'Alt':
                this.socket = new SocketIOAlt(this.url, this.api, this.identity, this.projectId, this.record!);
                break;
            case 'v1':
                this.record = undefined;
                this.socket = this.api._initSocketV0(this.identity);
                break;
            case 'v2':
                this.record = undefined;
                const query = `?projectId=${this.projectId}&t=${Date.now()}`;
                this.socket = this.api._initSocketV0(this.identity, query);
                break;
        }
        this.transportInvalidated = false;
        this.disconnectAnnounced = false;
        const socket = this.socket;
        // create emit
        (socket.emit)[require('util').promisify.custom] = (event:string, ...args:any[]) => {
            return new Promise((resolve, reject) => {
                let settled = false;
                const timer = setTimeout(() => {
                    settled = true;
                    reject(new SocketIOTimeoutError(event, SOCKET_EVENT_TIMEOUT_MS));
                }, SOCKET_EVENT_TIMEOUT_MS);
                try {
                    socket.emit(event, ...args, (err:any, ...data:any[]) => {
                        if (settled) {
                            return;
                        }
                        settled = true;
                        clearTimeout(timer);
                        if (generation !== this.generation || this.retired) {
                            reject(new TransportInvalidatedError(event));
                        } else if (err) {
                            reject(err);
                        } else {
                            resolve(data);
                        }
                    });
                } catch (error) {
                    if (!settled) {
                        settled = true;
                        clearTimeout(timer);
                        reject(error);
                    }
                }
            });
        };
        this.emit = require('util').promisify(this.socket.emit).bind(this.socket);
        // resume handlers
        this.initInternalHandlers();
        // Re-register existing event handlers on the new socket
        this.resumeEventHandlers(this._handlers);
        // Track which scheme this socket was created with
        this._socketInitScheme = this.scheme;
    }

    /** Returns true if the socket needs re-initialization (scheme changed, or socket was never init'd) */
    get needsReinit(): boolean {
        return this._socketInitScheme !== this.scheme
            || !this.socket
            || this.transportInvalidated;
    }

    private initInternalHandlers() {
        this.socket.on('connectionAccepted', (_:any, publicId:string) => {
            this._publicId = publicId;
        });
        this.socket.on('connect', () => {
            console.log('SocketIOAPI: connected');
        });
        this.socket.on('disconnect', () => {
            // Registered before any caller handler, so a real transport
            // disconnect is always recorded before the fallback below can run.
            this.disconnectAnnounced = true;
            this.transportInvalidated = true;
        });
        this.socket.on('connect_failed', () => {
            console.log('SocketIOAPI: connect_failed');
        });
        this.socket.on('forceDisconnect', (message:string, delay=10) => {
            console.log('SocketIOAPI: forceDisconnect', message);
        });
        this.socket.on('connectionRejected', (err:any) => {
            console.log('SocketIOAPI: connectionRejected.', err?.message || err);
            // If v2 also gets rejected, fall back to v1 rather than staying stuck
            if (this.scheme === 'v2') {
                console.log('SocketIOAPI: v2 rejected, falling back to v1');
                this.scheme = 'v1';
            }
            // Disable auto-reconnect on this socket: the server explicitly rejected
            // our connection parameters. Reconnecting would just get rejected again,
            // creating unnecessary TCP connection churn (and RST packets).
            this.disableAutomaticReconnect(this.socket);
        });
        this.socket.on('error', (err:any) => {
            // Log error instead of throwing to avoid crashing the extension
            console.error('SocketIOAPI: socket error', err?.message || err);
        });

        if (this.scheme==='v2') {
            this.record = new Promise(resolve => {
                this.socket.on('joinProjectResponse', (res:any) => {
                    const publicId = res.publicId as string;
                    this._publicId = publicId;
                    const project = res.project as ProjectEntity;
                    for (const handlers of this._handlers) {
                        handlers.onConnectionAccepted?.(publicId);
                    }
                    resolve(project);
                });
            });
        }
    }

    disconnect() {
        this.socket?.disconnect();
    }

    /** Permanently retire this client without allowing late handlers to run. */
    retire() {
        this.retired = true;
        this._publicId = undefined;
        this._handlers = [];
        if (this.socket) {
            this.permanentlyRetireSocket(this.socket);
            this.socket = undefined;
        }
    }

    /**
     * socket.io-client 0.9 returns a namespace whose transport manager lives
     * at `.socket`. Namespace.disconnect() is a no-op while the manager is
     * between reconnect attempts, so permanent retirement must cancel the
     * manager timers and close its transport explicitly.
     */
    private permanentlyRetireSocket(socket: any): void {
        const manager = this.socketManager(socket);
        if (manager?.destroyed) {
            return;
        }
        if (manager) {
            // The pinned transport patch checks this before late HTTP/WS callbacks.
            manager.destroyed = true;
            const handshake = manager.handshakeRequest;
            manager.handshakeRequest = undefined;
            try {
                handshake?.abort();
            } catch {
                // Complete transport cleanup even if the HTTP request is already closed.
            }
        }
        this.disableAutomaticReconnect(socket);
        const wasActive = manager?.connected || manager?.connecting || manager?.open;
        socket.removeAllListeners?.();
        try {
            socket.disconnect?.();
        } catch {
            // Continue with direct manager cleanup below.
        }
        if (manager) {
            this.clearManagerTimer(manager, 'connectTimeoutTimer');
            this.clearManagerTimer(manager, 'heartbeatTimeoutTimer');
            manager.transport?.clearTimeouts?.();
            if (!wasActive) {
                try {
                    manager.transport?.close?.();
                } catch {
                    // A failed transport can already be closed.
                }
            }
            manager.connected = false;
            manager.connecting = false;
            manager.open = false;
            manager.removeAllListeners?.();
            if (manager.namespaces) {
                manager.namespaces = {};
            }
        }
    }

    private disableAutomaticReconnect(socket: any): void {
        const manager = this.socketManager(socket);
        if (!manager) {
            return;
        }
        if (manager.options) {
            manager.options.reconnect = false;
        }
        manager.reconnecting = false;
        this.clearManagerTimer(manager, 'reconnectionTimer');
    }

    private socketManager(socket: any): any | undefined {
        return socket?.socket ?? socket?.io;
    }

    private clearManagerTimer(manager: any, name: string): void {
        if (manager[name]) {
            clearTimeout(manager[name]);
            delete manager[name];
        }
    }

    get handlers() {
        return this._handlers;
    }

    /** Socket source id used by Overleaf to identify acknowledgements. */
    get sourceId(): string | undefined {
        return this._publicId;
    }

    get isUsingAlternativeConnectionScheme() {
        return this.scheme==='Alt';
    }

    toggleAlternativeConnectionScheme(url: string, updatedRecord?: ProjectEntity) {
        this.scheme = this.scheme==='Alt' ? 'v1' : 'Alt';
        if (updatedRecord) {
            this.url = url;
            this.record = Promise.resolve(updatedRecord);
        }
    }

    resumeEventHandlers(handlers: Array<EventsHandler>) {
        this._handlers = [];
        handlers.forEach((handler) => {
            this.updateEventHandlers(handler);
        });
    }

    updateEventHandlers(handlers: EventsHandler) {
        this._handlers.push(handlers);
        Object.values(handlers).forEach((handler) => {
            switch (handler) {
                case handlers.onFileCreated:
                    this.socket.on('reciveNewDoc', (parentFolderId:string, doc:DocumentEntity) => {
                        handler(parentFolderId, 'doc', doc);
                    });
                    this.socket.on('reciveNewFile', (parentFolderId:string, file:FileRefEntity) => {
                        handler(parentFolderId, 'file', file);
                    });
                    this.socket.on('reciveNewFolder', (parentFolderId:string, folder:FolderEntity) => {
                        handler(parentFolderId, 'folder', folder);
                    });
                    break;
                case handlers.onFileRenamed:
                    this.socket.on('reciveEntityRename', (entityId:string, newName:string) => {
                        handler(entityId, newName);
                    });
                    break;
                case handlers.onFileRemoved:
                    this.socket.on('removeEntity', (entityId:string) => {
                        handler(entityId);
                    });
                    break;
                case handlers.onFileMoved:
                    this.socket.on('reciveEntityMove', (entityId:string, folderId:string) => {
                        handler(entityId, folderId);
                    });
                    break;
                case handlers.onFileChanged:
                    this.socket.on('otUpdateApplied', (update: UpdateSchema) => {
                        handler(update);
                    });
                    break;
                case handlers.onDisconnected:
                    this.socket.on('disconnect', () => {
                        handler();
                    });
                    break;
                case handlers.onConnectionAccepted:
                    this.socket.on('connectionAccepted', (_:any, publicId:any) => {
                        handler(publicId);
                    });
                    break;
                case handlers.onClientUpdated:
                    this.socket.on('clientTracking.clientUpdated', (user:UpdateUserSchema) => {
                        handler(user);
                    });
                    break;
                case handlers.onClientDisconnected:
                    this.socket.on('clientTracking.clientDisconnected', (id:string) => {
                        handler(id);
                    });
                    break;
                case handlers.onReceivedMessage:
                    this.socket.on('new-chat-message', (message:ProjectMessageResponseSchema) => {
                        handler(message);
                    });
                    break;
                case handlers.onSpellCheckLanguageUpdated:
                    this.socket.on('spellCheckLanguageUpdated', (language:string) => {
                        handler(language);
                    });
                    break;
                case handlers.onCompilerUpdated:
                    this.socket.on('compilerUpdated', (compiler:string) => {
                        handler(compiler);
                    });
                    break;
                case handlers.onRootDocUpdated:
                    this.socket.on('rootDocUpdated', (rootDocId:string) => {
                        handler(rootDocId);
                    });
                    break;
                default:
                    break;
            }
        });
    }

    get unSyncFileChanges(): number {
        if (this.socket instanceof SocketIOAlt) {
            return this.socket.unSyncedChanges;
        }
        return 0;
    }

    async syncFileChanges() {
        if (this.socket instanceof SocketIOAlt) {
            return await this.socket.uploadToVFS();
        }
    }

    /**
     * Reference: services/web/frontend/js/ide/connection/ConnectionManager.js#L427
     * @param {string} projectId - The project id.
     * @returns {Promise}
     */
    async joinProject(project_id:string): Promise<ProjectEntity> {
        const generation = this.generation;
        const socket = this.socket;
        if (this.retired) {
            throw new Error('Socket.IO client is permanently retired');
        }
        let timeout: NodeJS.Timeout;
        let onRejected: ((err: any) => void) | undefined;
        const timeoutPromise: Promise<ProjectEntity> = new Promise((_, reject) => {
            timeout = setTimeout(() => {
                reject(new SocketIOTimeoutError('joinProject', SOCKET_EVENT_TIMEOUT_MS));
            }, SOCKET_EVENT_TIMEOUT_MS);
        });

        let result: Promise<ProjectEntity>;
        switch(this.scheme) {
            case 'Alt':
            case 'v1':
                const joinPromise = this.emit('joinProject', {project_id})
                .then((returns:[ProjectEntity, string, number]) => {
                    if (generation !== this.generation || this.retired) {
                        throw new TransportInvalidatedError('joinProject');
                    }
                    const [project, permissionsLevel, protocolVersion] = returns;
                    this.record = Promise.resolve(project);
                    return project;
                });
                const rejectPromise = new Promise((_, reject) => {
                    onRejected = (err:any) => {
                        // Only fall back to v2 if we haven't already tried it;
                        // otherwise let the outer retry logic handle backoff
                        if (generation === this.generation && !this.retired) {
                            this.scheme = 'v2';
                        }
                        reject(err.message);
                    };
                    socket.on('connectionRejected', onRejected);
                });
                result = Promise.race([joinPromise, rejectPromise, timeoutPromise]);
                break;
            case 'v2':
                result = Promise.race([this.record!, timeoutPromise]);
                break;
        }
        return result.catch(error => {
            if (isSocketIOTimeout(error)) {
                // A late join acknowledgement can belong to a server-side
                // project membership generation we no longer understand.
                // Reusing this socket makes the retry observationally
                // ambiguous, so the next attempt must start a fresh transport.
                this.invalidateTransport(generation);
            }
            throw error;
        }).finally(() => {
            clearTimeout(timeout);
            if (onRejected) {
                socket.removeListener('connectionRejected', onRejected);
            }
        });
    }

    /**
     * Reference: services/web/frontend/js/ide/editor/Document.js#L500
     * @param {string} docId - The document id.
     * @returns {Promise}
     */
    async joinDoc(docId:string, fromVersion?:number): Promise<{
        docLines: string[],
        version: number,
        updates: UpdateSchema[],
        ranges: any,
        otType: string,
    }> {
        return this.runMembershipRpc(`joinDoc(${docId})`, async () => {
            const args = fromVersion === undefined
                ? [docId, { encodeRanges: true, supportsHistoryOT: true }]
                : [docId, fromVersion, { encodeRanges: true, age: 0, supportsHistoryOT: true }];
            const returns = await this.emit('joinDoc', ...args) as [
                Array<string>, number, Array<UpdateSchema>, any, string?,
            ];
            const [docLinesAscii, version, updates, ranges, otType = 'sharejs-text-ot'] = returns;
            const docLines = docLinesAscii.map((line) => decodePackedUtf8(line));
            return {docLines, version, updates, ranges, otType};
        });
    }

    /**
     * Reference: services/web/frontend/js/ide/editor/Document.js#L591
     * @param {string} docId - The document id.
     * @returns {Promise}
     */
    async leaveDoc(docId:string): Promise<void> {
        return this.runMembershipRpc(`leaveDoc(${docId})`, async () => {
            await this.emit('leaveDoc', docId);
        });
    }

    private runMembershipRpc<T>(
        event: string,
        task: () => Promise<T>,
    ): Promise<T> {
        let generation: number;
        const execute = () => {
            generation = this.generation;
            return this.transportInvalidated || this.retired
                ? Promise.reject<T>(new TransportInvalidatedError(event))
                : task();
        };
        const raw = this.membershipChain.then(execute, execute);
        const result = raw.catch(error => {
            if (!isSocketIOTimeout(error)) {
                throw error;
            }
            const timeoutMs = error instanceof SocketIOTimeoutError
                ? error.timeoutMs
                : SOCKET_EVENT_TIMEOUT_MS;
            const contextual = error instanceof SocketIOTimeoutError
                && error.event === event
                ? error
                : new SocketIOTimeoutError(event, timeoutMs);
            this.invalidateTransport(generation);
            throw contextual;
        });
        this.membershipChain = result.then(() => undefined, () => undefined);
        return result;
    }

    private invalidateTransport(generation = this.generation): void {
        if (this.transportInvalidated || this.retired || generation !== this.generation) {
            return;
        }
        this.transportInvalidated = true;
        const invalidated = this.socket;
        try {
            this.socket?.disconnect();
        } catch {
            // The next VFS initialization still sees needsReinit=true.
        }
        // disconnect() emits 'disconnect' only on a socket that still believes
        // it is connected, and an acknowledgement that timed out usually means
        // it does not. Nothing else rebuilds the transport: the reconnect is
        // driven by that event alone, so a silent disconnect leaves this flag
        // set forever and every later membership RPC is refused against it.
        // Announce the loss ourselves when the socket did not.
        setTimeout(() => {
            if (this.retired || generation !== this.generation
                || this.socket !== invalidated || this.disconnectAnnounced) {
                return;
            }
            this.disconnectAnnounced = true;
            for (const handlers of this._handlers) {
                handlers.onDisconnected?.();
            }
        }, 0);
    }

    /**
     * Reference: services/web/frontend/js/ide/editor/ShareJsDocs.js#L78
     * @param {string} docId - The document id.
     * @param {any} update - The changes.
     * @returns {Promise}
     */
    async applyOtUpdate(docId:string, update:UpdateSchema) {
        return this.emit('applyOtUpdate', docId, update)
            .then(() => {
                return;
            });
    }

    /**
     * Reference: services/web/frontend/js/ide/online-users/OnlineUserManager.js#L42
     * @returns {Promise}
     */
    async getConnectedUsers(): Promise<OnlineUserSchema[]> {
        return this.emit('clientTracking.getConnectedUsers')
            .then((returns:[OnlineUserSchema[]]) => {
                const [connectedUsers] = returns;
                return connectedUsers;
            });
    }

    /**
     * Reference: services/web/frontend/js/ide/online-users/OnlineUserManager.js#L150
     * @param {string} docId - The document id.
     * @returns {Promise}
     */
    async updatePosition(doc_id:string, row:number, column:number) {
        return this.emit('clientTracking.updatePosition', {row, column, doc_id})
            .then(() => {
                return;
            });
    }
}
