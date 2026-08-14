import { createHash, randomUUID } from 'crypto';
import { open } from 'fs/promises';
import * as path from 'path';
import * as vscode from 'vscode';
import { OtClientState, SerializedOtClientState } from './otClientState';

export interface StoredDocumentState {
    schemaVersion: 1;
    documentId: string;
    updatedAt: string;
    state: SerializedOtClientState;
}

/** Atomically persists per-document OT state outside the project tree. */
export class DocumentStateStore {
    private readonly directory: vscode.Uri;

    constructor(context: vscode.ExtensionContext, serverName: string, projectId: string) {
        const projectKey = createHash('sha256')
            .update(`${serverName}\0${projectId}`)
            .digest('hex');
        this.directory = vscode.Uri.joinPath(
            context.globalStorageUri,
            'safe-sync',
            'documents',
            projectKey,
        );
    }

    async load(documentId: string): Promise<StoredDocumentState | undefined> {
        const uri = this.stateUri(documentId);
        try {
            const content = await vscode.workspace.fs.readFile(uri);
            const parsed = JSON.parse(new TextDecoder().decode(content)) as StoredDocumentState;
            if (parsed.schemaVersion !== 1 || parsed.documentId !== documentId) {
                throw new Error(`Invalid durable document state for ${documentId}`);
            }
            OtClientState.restore(parsed.state);
            return parsed;
        } catch (error) {
            if (error instanceof vscode.FileSystemError && error.code === 'FileNotFound') {
                return undefined;
            }
            throw error;
        }
    }

    /** Enumerate legacy durable OT states so pending writes cannot become unreachable. */
    async listExisting(): Promise<StoredDocumentState[]> {
        let entries: [string, vscode.FileType][];
        try {
            entries = await vscode.workspace.fs.readDirectory(this.directory);
        } catch (error) {
            if (error instanceof vscode.FileSystemError && error.code === 'FileNotFound') {
                return [];
            }
            throw error;
        }
        const states: StoredDocumentState[] = [];
        const documentIds = new Set<string>();
        for (const [name, type] of entries) {
            if (type !== vscode.FileType.File || !name.endsWith('.json')) {
                continue;
            }
            const uri = vscode.Uri.joinPath(this.directory, name);
            let parsed: StoredDocumentState;
            try {
                parsed = JSON.parse(
                    new TextDecoder().decode(await vscode.workspace.fs.readFile(uri)),
                ) as StoredDocumentState;
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                throw new Error(`Invalid legacy durable document state ${name}: ${message}`);
            }
            if (
                parsed.schemaVersion !== 1
                || typeof parsed.documentId !== 'string'
                || parsed.documentId.length === 0
                || !parsed.state
                || parsed.state.schemaVersion !== 1
                || name !== this.fileName(parsed.documentId)
            ) {
                throw new Error(`Invalid legacy durable document state ${name}`);
            }
            try {
                OtClientState.restore(parsed.state);
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                throw new Error(`Invalid legacy durable document state ${name}: ${message}`);
            }
            if (documentIds.has(parsed.documentId)) {
                throw new Error(`Duplicate legacy durable document state for ${parsed.documentId}`);
            }
            documentIds.add(parsed.documentId);
            states.push(parsed);
        }
        return states.sort((left, right) => left.documentId.localeCompare(right.documentId));
    }

    async save(documentId: string, state: SerializedOtClientState): Promise<void> {
        await vscode.workspace.fs.createDirectory(this.directory);
        const target = this.stateUri(documentId);
        const temporary = vscode.Uri.joinPath(
            this.directory,
            `${this.fileName(documentId)}.${randomUUID()}.tmp`,
        );
        const payload: StoredDocumentState = {
            schemaVersion: 1,
            documentId,
            updatedAt: new Date().toISOString(),
            state,
        };
        await vscode.workspace.fs.writeFile(
            temporary,
            new TextEncoder().encode(JSON.stringify(payload)),
        );
        await this.syncFile(temporary);
        await vscode.workspace.fs.rename(temporary, target, { overwrite: true });
        await this.syncDirectory(target);
    }

    async remove(documentId: string): Promise<void> {
        const target = this.stateUri(documentId);
        try {
            await vscode.workspace.fs.delete(target);
        } catch (error) {
            if (error instanceof vscode.FileSystemError && error.code === 'FileNotFound') {
                return;
            } else {
                throw error;
            }
        }
        await this.syncDirectory(target);
    }

    private stateUri(documentId: string): vscode.Uri {
        return vscode.Uri.joinPath(this.directory, this.fileName(documentId));
    }

    private fileName(documentId: string): string {
        return `${createHash('sha256').update(documentId).digest('hex')}.json`;
    }

    private async syncFile(uri: vscode.Uri): Promise<void> {
        if (uri.scheme !== 'file') {
            return;
        }
        const handle = await open(uri.fsPath, 'r');
        try {
            await handle.sync();
        } finally {
            await handle.close();
        }
    }

    private async syncDirectory(uri: vscode.Uri): Promise<void> {
        if (uri.scheme !== 'file') {
            return;
        }
        try {
            const handle = await open(path.dirname(uri.fsPath), 'r');
            try {
                await handle.sync();
            } finally {
                await handle.close();
            }
        } catch (error) {
            if (process.platform !== 'win32') {
                throw error;
            }
        }
    }
}
