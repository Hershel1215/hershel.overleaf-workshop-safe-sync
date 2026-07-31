import { randomUUID } from 'crypto';
import { mkdir, readFile, rename, rm, writeFile } from 'fs/promises';
import * as net from 'net';
import * as path from 'path';

const GATEWAY_SCHEMA_VERSION = 1;
const MAX_MESSAGE_BYTES = 20 * 1024 * 1024;

export type AiGatewayMethod = 'begin' | 'heartbeat' | 'commit' | 'abort';

export interface AiGatewayRequest {
    token: string;
    requestId: string;
    method: AiGatewayMethod;
    [key: string]: unknown;
}

export interface AiGatewayMetadata {
    schemaVersion: 1;
    socketPath: string;
    token: string;
    cliPath?: string;
}

export interface AiGatewayHandler {
    handle: (request: AiGatewayRequest) => Promise<unknown>;
}

export class AiGatewayServer {
    private server?: net.Server;
    private metadata?: AiGatewayMetadata;

    constructor(
        private readonly metadataPath: string,
        private readonly handler: AiGatewayHandler,
        private readonly socketPath = defaultSocketPath(metadataPath),
        private readonly cliPath?: string,
    ) {}

    get info(): AiGatewayMetadata | undefined {
        return this.metadata && { ...this.metadata };
    }

    async start(): Promise<AiGatewayMetadata> {
        if (this.server) {
            return this.metadata!;
        }
        await mkdir(path.dirname(this.metadataPath), { recursive: true, mode: 0o700 });
        if (process.platform !== 'win32') {
            await rm(this.socketPath, { force: true });
        }
        const metadata: AiGatewayMetadata = {
            schemaVersion: GATEWAY_SCHEMA_VERSION,
            socketPath: this.socketPath,
            token: randomUUID(),
            cliPath: this.cliPath,
        };
        const server = net.createServer(socket => this.handleConnection(socket, metadata));
        await new Promise<void>((resolve, reject) => {
            const onError = (error: Error) => {
                server.removeListener('listening', onListening);
                reject(error);
            };
            const onListening = () => {
                server.removeListener('error', onError);
                resolve();
            };
            server.once('error', onError);
            server.once('listening', onListening);
            server.listen(this.socketPath);
        });
        this.server = server;
        this.metadata = metadata;
        await writeJsonAtomically(this.metadataPath, metadata);
        return { ...metadata };
    }

    async stop(): Promise<void> {
        const server = this.server;
        this.server = undefined;
        if (server) {
            await new Promise<void>(resolve => server.close(() => resolve()));
        }
        await rm(this.metadataPath, { force: true });
        if (process.platform !== 'win32') {
            await rm(this.socketPath, { force: true });
        }
        this.metadata = undefined;
    }

    private handleConnection(socket: net.Socket, metadata: AiGatewayMetadata): void {
        let received = Buffer.alloc(0);
        let completed = false;
        socket.setEncoding('utf8');
        socket.on('data', chunk => {
            if (completed) {
                return;
            }
            received = Buffer.concat([received, Buffer.from(chunk)]);
            if (received.length > MAX_MESSAGE_BYTES) {
                completed = true;
                socket.end(JSON.stringify({ ok: false, error: 'AI gateway request is too large' }) + '\n');
                return;
            }
            const newline = received.indexOf(0x0a);
            if (newline < 0) {
                return;
            }
            completed = true;
            const encoded = received.subarray(0, newline).toString('utf8');
            void this.dispatch(encoded, metadata, socket);
        });
        socket.on('error', () => {});
    }

    private async dispatch(
        encoded: string,
        metadata: AiGatewayMetadata,
        socket: net.Socket,
    ): Promise<void> {
        try {
            const request = JSON.parse(encoded) as AiGatewayRequest;
            if (request.token !== metadata.token) {
                throw new Error('AI gateway authentication failed');
            }
            if (typeof request.requestId !== 'string' || typeof request.method !== 'string') {
                throw new Error('AI gateway request shape is invalid');
            }
            const result = await this.handler.handle(request);
            socket.end(JSON.stringify({ ok: true, requestId: request.requestId, result }) + '\n');
        } catch (error) {
            socket.end(JSON.stringify({
                ok: false,
                error: error instanceof Error ? error.message : String(error),
            }) + '\n');
        }
    }
}

export async function sendAiGatewayRequest<T>(
    metadataPath: string,
    request: Omit<AiGatewayRequest, 'token' | 'requestId'> & { requestId?: string },
    timeoutMs = 10000,
): Promise<T> {
    const metadata = JSON.parse(await readFile(metadataPath, 'utf8')) as AiGatewayMetadata;
    if (metadata.schemaVersion !== GATEWAY_SCHEMA_VERSION) {
        throw new Error('Unsupported AI gateway metadata schema');
    }
    const payload = {
        ...request,
        requestId: request.requestId ?? randomUUID(),
        token: metadata.token,
    };
    return new Promise<T>((resolve, reject) => {
        const socket = net.createConnection(metadata.socketPath);
        let received = Buffer.alloc(0);
        const timer = setTimeout(() => {
            socket.destroy();
            reject(new Error('AI gateway request timed out'));
        }, timeoutMs);
        const finish = (error?: Error, value?: T) => {
            clearTimeout(timer);
            if (error) {
                reject(error);
            } else {
                resolve(value as T);
            }
        };
        socket.setEncoding('utf8');
        socket.on('connect', () => socket.end(JSON.stringify(payload) + '\n'));
        socket.on('data', chunk => {
            received = Buffer.concat([received, Buffer.from(chunk)]);
            const newline = received.indexOf(0x0a);
            if (newline < 0) {
                return;
            }
            try {
                const response = JSON.parse(received.subarray(0, newline).toString('utf8')) as {
                    ok: boolean;
                    error?: string;
                    result?: T;
                };
                if (!response.ok) {
                    finish(new Error(response.error ?? 'AI gateway request failed'));
                } else {
                    finish(undefined, response.result);
                }
            } catch (error) {
                finish(error instanceof Error ? error : new Error(String(error)));
            }
            socket.destroy();
        });
        socket.on('error', error => finish(error));
    });
}

async function writeJsonAtomically(filePath: string, value: unknown): Promise<void> {
    const temporary = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
    await writeFile(temporary, JSON.stringify(value), { mode: 0o600 });
    await rename(temporary, filePath);
}

function defaultSocketPath(metadataPath: string): string {
    if (process.platform === 'win32') {
        return `\\\\.\\pipe\\hershel-safe-sync-${randomUUID()}`;
    }
    return `/tmp/hershel-safe-sync-${process.pid}-${randomUUID()}.sock`;
}
