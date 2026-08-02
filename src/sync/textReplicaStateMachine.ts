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
    /** Apply only if the visible editor still equals expectedContent. */
    projectEditor: (content: string, expectedContent: string) => Promise<boolean | void>;
    projectDisk: (content: string) => Promise<void>;
    /** Read the visible editor before a plugin edit can overwrite pending input. */
    readEditor?: () => Promise<string | undefined>;
    /** Read the current disk projection before a plugin write can overwrite it. */
    readDisk?: () => Promise<string | undefined>;
    /** Persist a common baseline only after all local projections converge. */
    onConverged?: (content: string) => Promise<void>;
    isolate: (record: TextReplicaIsolation) => Promise<void>;
    persistProjectionReceipts?: (receipts: ProjectionReceipt[]) => Promise<void>;
    /** True while earlier VS Code observations still await this document queue. */
    hasPendingEditorObservations?: () => boolean;
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
    private allowDirtyDiskProjection = false;
    private diskMutationInProgress = false;
    private isolatedReason?: string;
    private disposed = false;
    private projectionInProgress = false;
    private projectionPending = false;
    private readonly projectionRetirementWaiters: Array<() => void> = [];

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
                accepted.acknowledgement.catch(error => {
                    if (!this.disposed) {
                        void this.isolate(`Remote acknowledgement failed: ${error.message}`);
                    }
                });
                if (this.disposed) {
                    return accepted;
                }
                this.canonicalContent = accepted.content;
                return accepted;
            },
            onIdle: () => this.projectCanonical(),
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
        if (this.disposed || this.isIsolated) {
            return;
        }
        this.canonicalContent = content;
        if (this.diskMutationInProgress) {
            this.projectionPending = true;
            return;
        }
        if (this.input.hasBufferedInput) {
            // The VFS OT session already owns the remote operation. Keep the
            // editor batch in memory until its normal idle/hard deadline, then
            // commit it against its event-time ancestor; the document session
            // transforms it over this newer canonical content. Projecting is
            // forbidden while the batch exists, so no durability is traded
            // for avoiding one WAL/fsync per remote callback.
            this.projectionPending = true;
            return;
        }
        await this.projectCanonical();
    }

    async onEditorChanged(
        content: string,
        observedAncestor?: string,
        requireObservedAncestor = false,
    ): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        const observation = { kind: 'change' as const, hash: textHash(content) };
        if (this.projections.observe(editorProjectionPath(this.path), observation).kind === 'projection') {
            this.editorContent = content;
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            await this.projectCanonical();
            return;
        }
        if (content === this.editorContent) {
            // A duplicate event may invalidate a stale receipt, but it is not
            // another user operation.
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            await this.projectCanonical();
            return;
        }
        if (content === this.canonicalContent) {
            // A file reload or coalesced editor notification can lack an
            // adjacent VS Code version while still observing the exact
            // canonical bytes. Missing metadata is not a content conflict.
            this.editorContent = content;
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            await this.projectCanonical();
            return;
        }
        if (requireObservedAncestor && observedAncestor === undefined) {
            await this.isolate('Cannot prove the event-time ancestor of a VS Code edit');
            return;
        }
        const ancestor = observedAncestor ?? this.editorContent ?? this.canonicalContent;
        this.editorContent = content;
        this.editorDirty = true;
        await this.input.updateTransition(ancestor, content);
    }

    async beforeEditorSave(content: string): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        this.projections.record(diskProjectionPath(this.path), {
            kind: this.diskContent === undefined ? 'create' : 'change',
            hash: textHash(content),
        });
        await this.persistProjectionReceipts();
    }

    async onEditorSaved(content: string): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        this.editorContent = content;
        this.editorDirty = false;
        this.diskContent = content;
        await this.notifyConverged();
    }

    async onDiskObserved(content: string | undefined): Promise<void> {
        if (this.disposed || this.isIsolated) {
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
            await this.notifyConverged();
            return;
        }
        if (content === this.canonicalContent) {
            this.diskContent = content;
            await this.notifyConverged();
            return;
        }
        if (
            content !== undefined
            && this.diskContent !== undefined
        ) {
            const ancestor = this.diskContent;
            this.diskMutationInProgress = true;
            try {
                this.diskContent = content;
                if (this.input.hasBufferedInput) {
                    await this.input.flush(true);
                }
                if (this.disposed || this.isIsolated) {
                    return;
                }
                const accepted = await this.port.commit(ancestor, content, false);
                accepted.acknowledgement.catch(error => {
                    if (!this.disposed) {
                        void this.isolate(`Remote acknowledgement failed: ${error.message}`);
                    }
                });
                if (this.disposed || this.isIsolated) {
                    return;
                }
                this.canonicalContent = accepted.content;
                // This exact raw disk branch is now journaled into canonical,
                // so one direct disk projection is safe even while VS Code
                // keeps the editor's dirty UI flag.
                this.allowDirtyDiskProjection = true;
            } catch (error) {
                const normalized = error instanceof Error ? error : new Error(String(error));
                await this.isolate(normalized.message);
                throw normalized;
            } finally {
                this.diskMutationInProgress = false;
            }
            await this.projectCanonical();
            return;
        }
        await this.isolate('Filesystem snapshot does not have a provable canonical base');
    }

    async flushInput(barrier = true): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        await this.input.flush(barrier);
    }

    dispose(): void {
        this.disposed = true;
        this.input.dispose();
    }

    async disposeAndWait(): Promise<void> {
        this.disposed = true;
        const projectionRetirement = this.waitForProjectionRetirement();
        await Promise.all([
            this.input.disposeAndWait(),
            projectionRetirement,
        ]);
    }

    private async projectCanonical(): Promise<void> {
        if (
            this.disposed
            || this.isIsolated
            || this.input.hasBufferedInput
            || this.diskMutationInProgress
        ) {
            if (this.diskMutationInProgress) {
                this.projectionPending = true;
            }
            return;
        }
        if (this.projectionInProgress) {
            this.projectionPending = true;
            return;
        }
        this.projectionInProgress = true;
        try {
            do {
                this.projectionPending = false;
                await this.projectCanonicalOnce();
            } while (
                this.projectionPending
                && !this.disposed
                && !this.isIsolated
            );
        } catch (error) {
            const normalized = error instanceof Error ? error : new Error(String(error));
            await this.isolate(normalized.message);
            throw normalized;
        } finally {
            this.projectionInProgress = false;
            for (const resolve of this.projectionRetirementWaiters.splice(0)) {
                resolve();
            }
        }
    }

    private async projectCanonicalOnce(): Promise<void> {
        if (this.disposed || this.isIsolated || this.input.hasBufferedInput) {
            return;
        }
        // Frozen editor observations are causal transitions, not latest-value
        // notifications. Let the document queue consume them before a remote
        // projection can advance the editor head they are based on.
        if (this.port.hasPendingEditorObservations?.()) {
            return;
        }
        const editorTarget = this.canonicalContent;
        if (this.editorContent !== undefined && this.editorContent !== editorTarget) {
            if (await this.importUnobservedEditor()) {
                return;
            }
        }
        if (this.input.hasBufferedInput) {
            await this.input.flush(true);
            return;
        }
        if (this.editorContent !== undefined && this.editorContent !== editorTarget) {
            this.projections.record(editorProjectionPath(this.path), {
                kind: 'change',
                hash: textHash(editorTarget),
            });
            await this.persistProjectionReceipts();
            if (this.disposed || this.isIsolated) {
                return;
            }
            if (this.input.hasBufferedInput) {
                await this.input.flush(true);
                return;
            }
            try {
                const applied = await this.port.projectEditor(
                    editorTarget,
                    this.editorContent,
                );
                if (applied === false) {
                    return;
                }
            } catch (error) {
                if (await this.importUnobservedEditor()) {
                    return;
                }
                throw error;
            }
            if (this.disposed || this.isIsolated) {
                return;
            }
            this.editorContent = editorTarget;
            if (this.canonicalContent !== editorTarget) {
                this.projectionPending = true;
                return;
            }
        }
        const diskTarget = this.canonicalContent;
        if (
            this.diskContent !== diskTarget
            && !this.diskMutationInProgress
            && (!this.editorDirty || this.allowDirtyDiskProjection)
        ) {
            // A raw filesystem writer may have changed the file before the
            // watcher callback ran. Import that snapshot against the last
            // observed disk base instead of overwriting it with remote text.
            if (this.port.readDisk) {
                const observedDisk = await this.port.readDisk();
                if (observedDisk !== this.diskContent) {
                    await this.onDiskObserved(observedDisk);
                    return;
                }
            }
            this.projections.record(diskProjectionPath(this.path), {
                kind: this.diskContent === undefined ? 'create' : 'change',
                hash: textHash(diskTarget),
            });
            await this.persistProjectionReceipts();
            if (this.disposed || this.isIsolated) {
                return;
            }
            if (this.input.hasBufferedInput) {
                await this.input.flush(true);
                return;
            }
            await this.port.projectDisk(diskTarget);
            if (this.disposed || this.isIsolated) {
                return;
            }
            this.diskContent = diskTarget;
            this.allowDirtyDiskProjection = false;
            if (this.canonicalContent !== diskTarget) {
                this.projectionPending = true;
                return;
            }
        }
        await this.notifyConverged();
    }

    /** Journal a VS Code edit whose change event has not reached the queue. */
    private async importUnobservedEditor(): Promise<boolean> {
        if (!this.port.readEditor) {
            return false;
        }
        if (this.port.hasPendingEditorObservations?.()) {
            return true;
        }
        const observedEditor = await this.port.readEditor();
        if (this.disposed || this.isIsolated) {
            return true;
        }
        if (this.port.hasPendingEditorObservations?.()) {
            return true;
        }
        if (observedEditor === undefined) {
            this.editorContent = undefined;
            return false;
        }
        if (observedEditor === this.editorContent) {
            return false;
        }
        await this.onEditorChanged(observedEditor);
        await this.input.flush(true);
        return true;
    }

    private waitForProjectionRetirement(): Promise<void> {
        if (!this.projectionInProgress) {
            return Promise.resolve();
        }
        return new Promise<void>(resolve => this.projectionRetirementWaiters.push(resolve));
    }

    private async persistProjectionReceipts(): Promise<void> {
        await this.port.persistProjectionReceipts?.(this.projections.serialize());
    }

    private async notifyConverged(): Promise<void> {
        if (
            this.disposed
            || this.isIsolated
            || this.input.hasBufferedInput
            || this.diskMutationInProgress
            || this.diskContent !== this.canonicalContent
            || (
                this.editorContent !== undefined
                && this.editorContent !== this.canonicalContent
            )
        ) {
            return;
        }
        await this.port.onConverged?.(this.canonicalContent);
    }

    private async isolate(reason: string): Promise<void> {
        if (this.disposed || this.isolatedReason) {
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
