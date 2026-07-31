import { createHash } from 'crypto';
import { BufferedDocumentInput } from './bufferedDocumentInput';
import { AcceptedDocumentOperation } from './durableDocumentSession';
import { ProjectionReceipt, ProjectionTracker } from './projectionTracker';

export interface TextReplicaIsolation {
    path: string;
    reason: string;
    canonicalContent: string;
    editorContent?: string;
    diskContent?: string;
}

export interface TextReplicaPort {
    commit: (
        ancestor: string,
        target: string,
        barrier: boolean,
    ) => Promise<AcceptedDocumentOperation>;
    projectEditor: (content: string) => Promise<void>;
    projectDisk: (content: string) => Promise<void>;
    isolate: (record: TextReplicaIsolation) => Promise<void>;
    persistProjectionReceipts?: (receipts: ProjectionReceipt[]) => Promise<void>;
}

export interface TextReplicaInitialState {
    path: string;
    canonicalContent: string;
    editorContent?: string;
    editorDirty?: boolean;
    diskContent?: string;
    projectionReceipts?: ProjectionReceipt[];
}

/**
 * Coordinates one canonical OT document with its VS Code and disk projections.
 * It never saves an editor and never interprets a matching projection receipt
 * as a user edit.
 */
export class TextReplicaStateMachine {
    private readonly path: string;
    private readonly port: TextReplicaPort;
    private readonly projections = new ProjectionTracker();
    private readonly input: BufferedDocumentInput;
    private canonicalContent: string;
    private editorContent?: string;
    private editorDirty: boolean;
    private diskContent?: string;
    private isolatedReason?: string;

    constructor(initial: TextReplicaInitialState, port: TextReplicaPort) {
        this.path = initial.path;
        this.port = port;
        this.canonicalContent = initial.canonicalContent;
        this.editorContent = initial.editorContent;
        this.editorDirty = initial.editorDirty ?? false;
        this.diskContent = initial.diskContent;
        this.projections.restore(initial.projectionReceipts ?? []);
        this.input = new BufferedDocumentInput({
            onError: error => void this.isolate(error.message),
            commit: async (ancestor, target, barrier) => {
                const accepted = await this.port.commit(ancestor, target, barrier);
                this.canonicalContent = accepted.content;
                accepted.acknowledgement.catch(error => {
                    void this.isolate(`Remote acknowledgement failed: ${error.message}`);
                });
                await this.projectCanonical();
                return accepted;
            },
        });
    }

    get isIsolated(): boolean {
        return this.isolatedReason !== undefined;
    }

    get hasBufferedInput(): boolean {
        return this.input.hasBufferedInput;
    }

    get canonical(): string {
        return this.canonicalContent;
    }

    serializeProjectionReceipts(): ProjectionReceipt[] {
        return this.projections.serialize();
    }

    async onCanonicalChanged(content: string): Promise<void> {
        if (this.isIsolated) {
            return;
        }
        this.canonicalContent = content;
        if (this.input.hasBufferedInput) {
            await this.input.flush(true);
            return;
        }
        await this.projectCanonical();
    }

    async onEditorChanged(content: string): Promise<void> {
        if (this.isIsolated) {
            return;
        }
        const observation = { kind: 'change' as const, hash: textHash(content) };
        if (this.projections.observe(editorProjectionPath(this.path), observation).kind === 'projection') {
            this.editorContent = content;
            await this.persistProjectionReceipts();
            return;
        }
        const ancestor = this.editorContent ?? this.canonicalContent;
        this.editorContent = content;
        this.editorDirty = true;
        this.input.update(ancestor, content);
    }

    async beforeEditorSave(content: string): Promise<void> {
        if (this.isIsolated) {
            return;
        }
        this.projections.record(diskProjectionPath(this.path), {
            kind: this.diskContent === undefined ? 'create' : 'change',
            hash: textHash(content),
        });
        await this.persistProjectionReceipts();
    }

    onEditorSaved(content: string): void {
        if (this.isIsolated) {
            return;
        }
        this.editorContent = content;
        this.editorDirty = false;
        this.diskContent = content;
    }

    async onDiskObserved(content: string | undefined): Promise<void> {
        if (this.isIsolated) {
            return;
        }
        const observation = {
            kind: content === undefined
                ? 'delete' as const
                : this.diskContent === undefined ? 'create' as const : 'change' as const,
            hash: content === undefined ? undefined : textHash(content),
        };
        if (this.projections.observe(diskProjectionPath(this.path), observation).kind === 'projection') {
            this.diskContent = content;
            await this.persistProjectionReceipts();
            return;
        }
        if (content === this.canonicalContent) {
            this.diskContent = content;
            return;
        }
        if (
            content !== undefined
            && this.diskContent !== undefined
        ) {
            const ancestor = this.diskContent;
            this.diskContent = content;
            if (this.input.hasBufferedInput) {
                await this.input.flush(true);
            }
            const accepted = await this.port.commit(ancestor, content, false);
            this.canonicalContent = accepted.content;
            accepted.acknowledgement.catch(error => {
                void this.isolate(`Remote acknowledgement failed: ${error.message}`);
            });
            await this.projectCanonical();
            return;
        }
        await this.isolate('Filesystem snapshot does not have a provable canonical base');
    }

    async flushInput(barrier = true): Promise<void> {
        await this.input.flush(barrier);
    }

    dispose(): void {
        this.input.dispose();
    }

    private async projectCanonical(): Promise<void> {
        if (this.isIsolated || this.input.hasBufferedInput) {
            return;
        }
        if (this.editorContent !== undefined && this.editorContent !== this.canonicalContent) {
            this.projections.record(editorProjectionPath(this.path), {
                kind: 'change',
                hash: textHash(this.canonicalContent),
            });
            await this.persistProjectionReceipts();
            await this.port.projectEditor(this.canonicalContent);
            this.editorContent = this.canonicalContent;
        }
        if (!this.editorDirty && this.diskContent !== this.canonicalContent) {
            this.projections.record(diskProjectionPath(this.path), {
                kind: this.diskContent === undefined ? 'create' : 'change',
                hash: textHash(this.canonicalContent),
            });
            await this.persistProjectionReceipts();
            await this.port.projectDisk(this.canonicalContent);
            this.diskContent = this.canonicalContent;
        }
    }

    private async persistProjectionReceipts(): Promise<void> {
        await this.port.persistProjectionReceipts?.(this.projections.serialize());
    }

    private async isolate(reason: string): Promise<void> {
        if (this.isolatedReason) {
            return;
        }
        this.isolatedReason = reason;
        this.input.dispose();
        await this.port.isolate({
            path: this.path,
            reason,
            canonicalContent: this.canonicalContent,
            editorContent: this.editorContent,
            diskContent: this.diskContent,
        });
    }
}

function textHash(content: string): string {
    return createHash('sha256').update(content).digest('hex');
}

function editorProjectionPath(path: string): string {
    return `editor:${path}`;
}

function diskProjectionPath(path: string): string {
    return `disk:${path}`;
}
