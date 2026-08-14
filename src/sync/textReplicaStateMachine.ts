import { createHash } from 'crypto';
import { BufferedDocumentInput } from './bufferedDocumentInput';
import {
    AcceptedDocumentOperation,
    DocumentSessionRetiredError,
} from './durableDocumentSession';
import { ProjectionReceipt, ProjectionTracker } from './projectionTracker';

/**
 * A filesystem observation this actor cannot attribute to any state it holds.
 *
 * Two shapes reach here, and both are resource events rather than forks of the
 * text. The file may appear underneath an actor that attached for a remote-only
 * document, with no local file and no durable base, so the actor has no disk
 * history to name an ancestor from. Or the file may vanish: a remote move is
 * applied as a delete and a create, and the actor re-reads disk between them
 * while projecting. Either way the resource scan can prove what the actor
 * cannot, because it holds the baseline and the entity identity the move
 * carried, so the path is handed back rather than archived as a conflict.
 */
export class UnattributedDiskSnapshotError extends Error {}

export interface TextReplicaIsolation {
    path: string;
    remoteEntityId?: string;
    reason: string;
    canonicalContent: string;
    editorContent?: string;
    diskContent?: string;
}

export interface DiskProjectionOutcome {
    applied: boolean;
    /** Bytes observed after the compare-and-swap attempt. */
    actualContent?: string;
    /** Proven event-time ancestor of actualContent. */
    installedAncestor?: string;
}

export interface TextReplicaDiagnosticEvent {
    sequence: number;
    elapsedMs?: number;
    kind: string;
    classification?: 'projection' | 'mutation';
    projectionId?: string;
    canonicalBefore?: string;
    canonicalAfter?: string;
    editor?: string;
    diskBefore?: string;
    observed?: string;
    saveSnapshot?: string;
    savedAncestor?: string;
    ancestor?: string;
    target?: string;
    result?: string;
    reason?: string;
    bufferedInput?: boolean;
    diskMutation?: boolean;
    pendingEditorObservations?: boolean;
    /** OT revision of the projection that carried this canonical content. */
    revision?: number;
    /**
     * Whether the editor head a projection is about to overwrite is one this
     * actor can account for: a state it submitted, or one it projected. An
     * editor head that is neither holds bytes that never crossed the journal,
     * and projecting over it destroys them.
     */
    ancestorAccountedFor?: 'submitted' | 'projected' | 'canonical' | 'unjournaled';
    /** Whether the buffer held unsaved bytes when the event was recorded. */
    editorDirty?: boolean;
}

export interface TextReplicaPort {
    commit: (
        ancestor: string,
        target: string,
        barrier: boolean,
    ) => Promise<AcceptedDocumentOperation>;
    /** Apply only if the visible editor still equals expectedContent. */
    projectEditor: (content: string, expectedContent: string) => Promise<boolean | void>;
    /** Apply only if disk still equals the state machine's previous observation. */
    projectDisk: (
        content: string,
        expectedContent: string | undefined,
    ) => Promise<boolean | void | DiskProjectionOutcome>;
    /** Read the visible editor before a plugin edit can overwrite pending input. */
    readEditor?: () => Promise<string | undefined>;
    /**
     * Save a buffer that a projection had to edit to make remote text visible,
     * so a document the user had saved does not keep an unsaved marker for
     * bytes the user never wrote. Implementations must save only while the
     * editor still holds expectedContent, and must report whether the host
     * actually wrote the file: that write carries this projection's receipt.
     */
    restoreSavedEditor?: (expectedContent: string) => Promise<boolean>;
    /** Read the current disk projection before a plugin write can overwrite it. */
    readDisk?: () => Promise<string | undefined>;
    /** Persist a common baseline only after all local projections converge. */
    onConverged?: (content: string) => Promise<void>;
    isolate: (record: TextReplicaIsolation) => Promise<void>;
    persistProjectionReceipts?: (receipts: ProjectionReceipt[]) => Promise<void>;
    /** Run timer-originated commits inside the per-document scheduler. */
    scheduleInputBoundary?: (task: () => Promise<void>) => Promise<void>;
    /** Retry a path-busy disk CAS through the same document scheduler. */
    scheduleProjectionRetry?: (task: () => Promise<void>, delayMs: number) => void;
    /** True while earlier VS Code observations still await this document queue. */
    hasPendingEditorObservations?: () => boolean;
}

export interface TextReplicaInitialState {
    path: string;
    /** Immutable Overleaf document identity owned by this OT actor. */
    remoteEntityId?: string;
    canonicalContent: string;
    /** Ordinal of the session state `canonicalContent` was read from. */
    canonicalSequence?: number;
    editorContent?: string;
    editorDirty?: boolean;
    diskContent?: string;
    projectionReceipts?: ProjectionReceipt[];
    /** Bounded in-memory causal trace, enabled only for reserved E2E paths. */
    diagnosticsEnabled?: boolean;
}

/**
 * Coordinates one canonical OT document with its VS Code and disk projections.
 * It never saves an editor and never interprets a matching projection receipt
 * as a user edit.
 */
export class TextReplicaStateMachine {
    private readonly path: string;
    private readonly ownedRemoteEntityId?: string;
    private readonly port: TextReplicaPort;
    private readonly projections = new ProjectionTracker();
    private readonly input: BufferedDocumentInput;
    private canonicalContent: string;
    /**
     * Ordinal of the session state canonicalContent came from. Every canonical
     * value this machine holds was read from the document session, and the
     * session stamps each of its states with a monotonic ordinal, so this is
     * what proves whether an arriving projection is newer than what is already
     * held or an older state that merely arrived late.
     */
    private canonicalSequence?: number;
    /** The last editor state this actor installed itself, for trace provenance. */
    private lastProjectedEditorTarget?: string;
    private editorContent?: string;
    private editorDirty: boolean;
    private diskContent?: string;
    private diskMutationInProgress = false;
    private isolatedReason?: string;
    private disposed = false;
    private projectionInProgress = false;
    private projectionPending = false;
    private unacknowledgedOperations = 0;
    /** Exact save snapshots whose watcher echo may be coalesced with a later writer. */
    private readonly savedDiskAncestors: Array<{receiptId: string; content: string}> = [];
    /** Save snapshots this machine requested, already carrying a disk receipt. */
    private readonly pendingRestoreSaves: string[] = [];
    private projectionResumeArmed = false;
    /**
     * True while this machine's projection has left an editor the user had
     * saved carrying an unsaved marker. Convergence is not announced until the
     * saved state is restored, so no observer sees a settled document with an
     * unsaved marker for bytes the user never wrote.
     */
    private projectedEditorNeedsSave = false;
    private readonly projectionRetirementWaiters: Array<() => void> = [];
    private readonly diagnosticsEnabled: boolean;
    private readonly diagnosticsStartedAt = Date.now();
    private diagnosticSequence = 0;
    private readonly diagnostics: TextReplicaDiagnosticEvent[] = [];

    constructor(initial: TextReplicaInitialState, port: TextReplicaPort) {
        this.path = initial.path;
        this.ownedRemoteEntityId = initial.remoteEntityId;
        this.port = port;
        this.diagnosticsEnabled = initial.diagnosticsEnabled ?? false;
        this.canonicalContent = initial.canonicalContent;
        this.canonicalSequence = initial.canonicalSequence;
        this.editorContent = initial.editorContent;
        this.editorDirty = initial.editorDirty ?? false;
        this.diskContent = initial.diskContent;
        this.projections.restore(initial.projectionReceipts ?? []);
        this.input = new BufferedDocumentInput({
            onError: error => void this.isolate(error.message),
            commit: async (ancestor, target, barrier) => {
                const canonicalBefore = this.canonicalContent;
                this.recordDiagnostic({
                    kind: 'buffered-commit',
                    ancestor,
                    target,
                    canonicalBefore,
                    editor: this.editorContent,
                    diskBefore: this.diskContent,
                });
                // The VFS owns reconnect recovery. Once this call returns a
                // receipt, every later generation follows its fixed opId and
                // no upper layer may derive a second snapshot operation.
                const accepted: AcceptedDocumentOperation = await this.port.commit(
                    ancestor,
                    target,
                    barrier,
                );
                accepted.acknowledgement.catch(error => {
                    if (!this.disposed && !(error instanceof DocumentSessionRetiredError)) {
                        void this.isolate(`Remote acknowledgement failed: ${error.message}`);
                    }
                });
                if (accepted.opId) {
                    this.unacknowledgedOperations += 1;
                    void accepted.acknowledgement.finally(() => {
                        this.unacknowledgedOperations = Math.max(
                            0,
                            this.unacknowledgedOperations - 1,
                        );
                        if (!this.disposed && !this.isIsolated) {
                            void this.projectCanonical().catch(() => {});
                        }
                    }).catch(() => {});
                }
                if (this.disposed) {
                    return accepted;
                }
                this.adoptCanonical(accepted.content, accepted.sequence);
                this.recordDiagnostic({
                    kind: 'buffered-accepted',
                    ancestor,
                    target,
                    canonicalBefore,
                    canonicalAfter: accepted.content,
                    result: accepted.content,
                });
                return accepted;
            },
            onIdle: () => this.projectCanonical(),
            onBoundary: () => this.projectBufferedBoundary(),
            scheduleBoundary: this.port.scheduleInputBoundary,
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

    /** The last filesystem snapshot this replica proved an ancestor for. */
    get diskSnapshot(): string | undefined {
        return this.diskContent;
    }

    get remoteEntityId(): string | undefined {
        return this.ownedRemoteEntityId;
    }

    get hasUnacknowledgedOperations(): boolean {
        return this.unacknowledgedOperations > 0;
    }

    get isSettled(): boolean {
        return !this.disposed
            && !this.isIsolated
            && !this.input.hasBufferedInput
            && !this.diskMutationInProgress
            && !this.projectionInProgress
            && !this.projectionPending
            && this.unacknowledgedOperations === 0
            && !this.projectedEditorNeedsSave
            && this.diskContent === this.canonicalContent
            && (
                this.editorContent === undefined
                || this.editorContent === this.canonicalContent
            );
    }

    /** Capture any editor bytes that arrived before its change callback. */
    async prepareForHandoff(): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        const observedEditor = await this.port.readEditor?.();
        if (
            observedEditor !== undefined
            && observedEditor !== this.editorContent
        ) {
            await this.onEditorChanged(observedEditor, this.editorContent, false);
        }
        await this.input.flush(true);
        await this.input.waitForActiveCommit();
    }

    /**
     * Fence a resource-level conflict without submitting buffered editor bytes.
     * The caller persists the returned candidate before retiring this owner.
     */
    async freezeForConflict(reason: string): Promise<TextReplicaIsolation> {
        if (!this.isolatedReason) {
            this.isolatedReason = reason;
            await this.input.disposeAndWait();
        }
        return {
            path: this.path,
            ...(this.ownedRemoteEntityId
                ? {remoteEntityId: this.ownedRemoteEntityId}
                : {}),
            reason: this.isolatedReason,
            canonicalContent: this.canonicalContent,
            editorContent: this.editorContent,
            diskContent: this.diskContent,
        };
    }

    serializeProjectionReceipts(): ProjectionReceipt[] {
        return this.projections.serialize();
    }

    inspectDiagnostics(): TextReplicaDiagnosticEvent[] {
        return this.diagnostics.map(event => ({...event}));
    }

    /** Let the event ingress preserve plugin projection boundaries while coalescing typing. */
    isExpectedEditorProjection(
        content: string,
        observedAncestor = this.editorContent,
    ): boolean {
        return this.projections.matches(editorProjectionPath(this.path), {
            kind: 'change',
            hash: textHash(content),
            ancestor: observedAncestor,
            content,
        });
    }

    /**
     * Admit the common key-event path synchronously. The event is already a UI
     * fact; placing it in the in-memory OT batch here prevents async scheduler
     * backlog from hiding it past the hard durability/projection boundary.
     */
    tryBufferEditorChange(
        content: string,
        observedAncestor: string | undefined,
        editorDirty = true,
    ): boolean {
        if (
            this.disposed
            || this.isIsolated
            || observedAncestor === undefined
            || this.isExpectedEditorProjection(content, observedAncestor)
            || content === this.editorContent
            || (content === this.canonicalContent && !this.input.hasBufferedInput)
            || !this.input.tryUpdateTransition(observedAncestor, content)
        ) {
            return false;
        }
        this.editorContent = content;
        this.editorDirty = editorDirty;
        this.projectedEditorNeedsSave = false;
        return true;
    }

    async onCanonicalChanged(
        content: string,
        revision?: number,
        sequence?: number,
    ): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        // A projection is delivered through the per-document queue, behind
        // whatever work was already waiting there, while this machine's own
        // commits read the session directly and return its newest state. A
        // projection that was published before that commit therefore describes
        // a state this machine has already moved past, and adopting it would
        // reinstate text the commit had replaced — the local edit is not lost
        // remotely, but the canonical it is projected from went backwards.
        // The revision cannot decide this: it only advances on server
        // acknowledgement, so a burst of local operations shares one revision
        // across many contents.
        // Only a strictly older state is refused. The state this machine is
        // already on is delivered again by design — a commit reads the session
        // directly and the same state also arrives through the queue — and
        // that redelivery is what drives the projection of an attached or
        // just-committed document.
        if (
            sequence !== undefined
            && this.canonicalSequence !== undefined
            && sequence < this.canonicalSequence
        ) {
            this.recordDiagnostic({
                kind: 'canonical-superseded',
                canonicalBefore: this.canonicalContent,
                canonicalAfter: content,
                editor: this.editorContent,
                diskBefore: this.diskContent,
                revision,
            });
            return;
        }
        this.recordDiagnostic({
            kind: 'canonical-changed',
            canonicalBefore: this.canonicalContent,
            canonicalAfter: content,
            editor: this.editorContent,
            diskBefore: this.diskContent,
            revision,
        });
        this.adoptCanonical(content, sequence);
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

    /**
     * Take a session state as the new canonical, remembering which state it
     * was. A caller that cannot name the ordinal clears it rather than keeping
     * the previous one: an unknown position must not let a later projection be
     * compared against a state it does not belong to.
     */
    private adoptCanonical(content: string, sequence?: number): void {
        this.canonicalContent = content;
        this.canonicalSequence = sequence;
    }

    /**
     * Say where the current editor head came from, so a trace can tell a
     * projection that merely refreshes the buffer from one that overwrites
     * bytes this actor never journaled. Only the last submitted target and the
     * last projected target are states this actor put there; anything else the
     * buffer holds arrived from a writer whose operation must still be owed.
     */
    private classifyEditorAncestor(): 'submitted' | 'projected' | 'canonical' | 'unjournaled' {
        const editor = this.editorContent;
        if (editor === this.input.lastSubmittedTarget) {
            return 'submitted';
        }
        if (editor === this.lastProjectedEditorTarget) {
            return 'projected';
        }
        if (editor === this.canonicalContent) {
            return 'canonical';
        }
        return 'unjournaled';
    }

    async onEditorChanged(
        content: string,
        observedAncestor?: string,
        requireObservedAncestor = false,
        editorDirty = true,
    ): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        const observation = {
            kind: 'change' as const,
            hash: textHash(content),
            ancestor: observedAncestor ?? this.editorContent,
            content,
        };
        this.recordDiagnostic({
            kind: 'editor-observed',
            observed: content,
            ancestor: observedAncestor,
            editor: this.editorContent,
            canonicalBefore: this.canonicalContent,
            bufferedInput: this.input.hasBufferedInput,
        });
        const classification = this.projections.observe(
            editorProjectionPath(this.path),
            observation,
        );
        if (classification.kind === 'projection') {
            // Bytes admitted here become the editor head without crossing the
            // journal, because this actor is supposed to have put them there.
            // A receipt that matched something else makes the head unjournaled,
            // and the projection that later overwrites it destroys real text,
            // so name the receipt that claimed these bytes.
            this.recordDiagnostic({
                kind: 'editor-observed',
                classification: 'projection',
                projectionId: classification.projectionId,
                observed: content,
                ancestor: observedAncestor,
                editor: this.editorContent,
                canonicalBefore: this.canonicalContent,
            });
            this.editorContent = content;
            this.editorDirty = editorDirty;
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            await this.projectCanonical();
            return;
        }
        if (content === this.editorContent) {
            // A duplicate event may invalidate a stale receipt, but it is not
            // another user operation.
            this.editorDirty = editorDirty;
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            await this.projectCanonical();
            return;
        }
        if (content === this.canonicalContent && !this.input.hasBufferedInput) {
            // A file reload or coalesced editor notification can lack an
            // adjacent VS Code version while still observing the exact
            // canonical bytes. Missing metadata is not a content conflict.
            this.editorContent = content;
            this.editorDirty = editorDirty;
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            await this.projectCanonical();
            return;
        }
        if (
            observedAncestor !== undefined
            && observedAncestor !== this.editorContent
            && (
                content === this.canonicalContent
                || content === this.input.lastSubmittedTarget
            )
        ) {
            // A frozen observation that resumed after its own transition had
            // already crossed the journal. These exact bytes were submitted
            // and the editor head has moved past them, so there is nothing
            // left to journal. Admitting it would splice an already-committed
            // ancestor onto the live batch, and transforming that batch over
            // the operation it repeats writes this history a second time.
            this.recordDiagnostic({
                kind: 'editor-observed',
                reason: 'already-journaled',
                observed: content,
                ancestor: observedAncestor,
                editor: this.editorContent,
                canonicalBefore: this.canonicalContent,
                bufferedInput: this.input.hasBufferedInput,
            });
            this.editorDirty = editorDirty;
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            await this.projectCanonical();
            return;
        }
        if (requireObservedAncestor && observedAncestor === undefined) {
            await this.isolate(
                'Cannot prove the event-time ancestor of a VS Code edit',
                content,
            );
            return;
        }
        const ancestor = observedAncestor ?? this.editorContent ?? this.canonicalContent;
        this.editorContent = content;
        this.editorDirty = editorDirty;
        // These bytes are the user's now. Saving them later would commit an
        // unsaved branch they chose to keep unsaved.
        this.projectedEditorNeedsSave = false;
        await this.input.updateTransition(ancestor, content);
    }

    /** Record which actor asked VS Code to save, for causal traces. */
    noteSaveReason(reason: number): void {
        this.recordDiagnostic({
            kind: 'save-requested',
            reason: `vscode-reason-${reason}`,
            editor: this.editorContent,
            diskBefore: this.diskContent,
        });
    }

    async beforeEditorSave(content: string): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        const restore = this.pendingRestoreSaves.indexOf(content);
        if (restore >= 0) {
            // This machine asked for this save to clear the unsaved marker its
            // own projection created, and already recorded the receipt that
            // classifies the write. There is no user branch to journal.
            this.pendingRestoreSaves.splice(restore, 1);
            this.recordDiagnostic({kind: 'save-will', reason: 'restore', saveSnapshot: content});
            return;
        }
        this.recordDiagnostic({
            kind: 'save-will',
            saveSnapshot: content,
            canonicalBefore: this.canonicalContent,
            editor: this.editorContent,
            diskBefore: this.diskContent,
            savedAncestor: this.savedDiskAncestors.at(-1)?.content,
        });
        // A user save is not a plugin projection until the editor bytes have
        // crossed the journal boundary. Persisting this receipt earlier would
        // let crash recovery misclassify real disk input as our own write.
        await this.input.flush(true);
        if (this.disposed || this.isIsolated) {
            return;
        }
        // `content` was frozen synchronously at onWillSave. Newer editor input
        // may exist by the time this scheduler task runs, but the disk write
        // still contains this exact older snapshot. Classify its watcher echo
        // only after the preceding editor branch crossed a hard journal cut.
        const receipt = this.projections.record(diskProjectionPath(this.path), {
            kind: this.diskContent === undefined ? 'create' : 'change',
            hash: textHash(content),
        });
        this.savedDiskAncestors.push({receiptId: receipt.id, content});
        this.recordDiagnostic({
            kind: 'save-receipt',
            projectionId: receipt.id,
            saveSnapshot: content,
            canonicalBefore: this.canonicalContent,
            diskBefore: this.diskContent,
        });
        await this.persistProjectionReceipts();
    }

    async onEditorSaved(content: string, editorDirtyAfterSave = false): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        // Whoever saved it, the projection no longer owes this buffer a save.
        this.projectedEditorNeedsSave = false;
        this.recordDiagnostic({
            kind: 'save-did',
            saveSnapshot: content,
            canonicalBefore: this.canonicalContent,
            editor: this.editorContent,
            diskBefore: this.diskContent,
            savedAncestor: this.savedDiskAncestors.at(-1)?.content,
        });
        // onWillSave is deliberately non-blocking so VS Code never times out a
        // busy extension listener. Consequently this queued callback can run
        // after newer input. `content` and the dirty bit are event-time editor
        // facts; the actual saved disk snapshot is consumed through the exact
        // receipt recorded by beforeEditorSave.
        await this.input.flush(true);
        if (this.disposed || this.isIsolated) {
            return;
        }
        const currentEditor = await this.port.readEditor?.();
        if (currentEditor !== undefined && currentEditor !== this.editorContent) {
            await this.onEditorChanged(
                currentEditor,
                this.editorContent ?? this.canonicalContent,
                false,
                currentEditor !== content,
            );
            await this.input.flush(true);
        }
        if (this.disposed || this.isIsolated) {
            return;
        }
        // Equal bytes mean this save still describes the visible editor. If
        // they differ, newer input exists and must remain logically dirty until
        // its own save/projection boundary.
        this.editorDirty = currentEditor === undefined
            ? this.editorDirty
            : currentEditor === content ? editorDirtyAfterSave : true;
        await this.projectCanonical();
        await this.notifyConverged();
    }

    async onDiskObserved(content: string | undefined): Promise<void> {
        if (this.disposed || this.isIsolated) {
            return;
        }
        const canonicalBeforeObservation = this.canonicalContent;
        const diskBeforeObservation = this.diskContent;
        const observation = {
            kind: content === undefined
                ? 'delete' as const
                : this.diskContent === undefined ? 'create' as const : 'change' as const,
            hash: content === undefined ? undefined : textHash(content),
        };
        const latestSavedAncestor = this.savedDiskAncestors.at(-1);
        this.recordDiagnostic({
            kind: 'disk-observed',
            observed: content,
            canonicalBefore: canonicalBeforeObservation,
            editor: this.editorContent,
            diskBefore: diskBeforeObservation,
            savedAncestor: latestSavedAncestor?.content,
        });
        const classification = this.projections.observe(
            diskProjectionPath(this.path),
            observation,
        );
        this.recordDiagnostic({
            kind: 'disk-classified',
            classification: classification.kind,
            ...(classification.kind === 'projection'
                ? {projectionId: classification.projectionId}
                : {}),
            observed: content,
            canonicalBefore: canonicalBeforeObservation,
            diskBefore: diskBeforeObservation,
            savedAncestor: latestSavedAncestor?.content,
        });
        if (classification.kind === 'projection') {
            const savedIndex = this.savedDiskAncestors.findIndex(candidate => (
                candidate.receiptId === classification.projectionId
            ));
            if (savedIndex >= 0) {
                this.savedDiskAncestors.splice(0, savedIndex + 1);
            }
            // Matching a newer coalesced projection discards every older
            // receipt at this endpoint. Drop the parallel save-base entries as
            // well, otherwise a later raw write could be diffed from a stale
            // save snapshot and duplicate already-journaled text.
            for (let index = this.savedDiskAncestors.length - 1; index >= 0; index--) {
                if (!this.projections.hasReceipt(this.savedDiskAncestors[index].receiptId)) {
                    this.savedDiskAncestors.splice(index, 1);
                }
            }
            this.diskContent = content;
            await this.persistProjectionReceipts();
            await this.notifyConverged();
            return;
        }
        // A watcher can coalesce an acknowledged VS Code save with a raw
        // writer that immediately edits the saved inode. The latest frozen
        // save snapshot is then the exact disk-side ancestor; using the older
        // observed disk head would resubmit the already-journaled editor prefix.
        const savedDiskAncestor = latestSavedAncestor?.content;
        this.savedDiskAncestors.length = 0;
        if (content === this.canonicalContent) {
            this.diskContent = content;
            await this.notifyConverged();
            return;
        }
        if (
            content !== undefined
            && this.diskContent !== undefined
        ) {
            const ancestor = savedDiskAncestor ?? this.diskContent;
            const editorAlreadyOwnsSnapshot = content === this.editorContent
                && this.input.hasBufferedInput;
            this.diskMutationInProgress = true;
            try {
                this.diskContent = content;
                if (this.input.hasBufferedInput) {
                    await this.input.flush(true);
                }
                if (this.disposed || this.isIsolated) {
                    return;
                }
                // A save in another VS Code window can surface first as an
                // editor reload and then as a filesystem notification. The
                // editor transition above already journaled these exact bytes;
                // importing the disk notification again would duplicate the
                // same operation, especially after a concurrent remote rebase.
                if (editorAlreadyOwnsSnapshot) {
                    // The editor already journaled these exact bytes.
                } else {
                    const canonicalBeforeCommit = this.canonicalContent;
                    this.recordDiagnostic({
                        kind: 'disk-commit',
                        ancestor,
                        target: content,
                        canonicalBefore: canonicalBeforeCommit,
                        diskBefore: diskBeforeObservation,
                        savedAncestor: savedDiskAncestor,
                    });
                    const accepted = await this.port.commit(ancestor, content, false);
                    accepted.acknowledgement.catch(error => {
                        if (!this.disposed && !(error instanceof DocumentSessionRetiredError)) {
                            void this.isolate(`Remote acknowledgement failed: ${error.message}`);
                        }
                    });
                    if (this.disposed || this.isIsolated) {
                        return;
                    }
                    this.adoptCanonical(accepted.content, accepted.sequence);
                    this.recordDiagnostic({
                        kind: 'disk-accepted',
                        ancestor,
                        target: content,
                        canonicalBefore: canonicalBeforeCommit,
                        canonicalAfter: accepted.content,
                        result: accepted.content,
                    });
                }
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
        // Either these bytes predate anything this actor knows about the path,
        // or the path is gone. Both are resource events, and the resource scan
        // can still prove what happened because it holds the baseline and the
        // entity identity a move carried. Escalate instead of freezing the
        // document against evidence this actor does not have.
        throw new UnattributedDiskSnapshotError(
            content === undefined
                ? `The local file for ${this.path} disappeared while its text actor was active`
                : `Filesystem snapshot for ${this.path} has no attributable disk history`,
        );
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

    /**
     * Re-enter projection after the document queue drained the observations
     * that deferred it. A projection-classified editor event buffers no input,
     * so no idle commit timer would otherwise fire and the remote revision
     * would simply be dropped.
     */
    async resumeProjection(): Promise<void> {
        if (this.disposed || this.isIsolated || this.isSettled) {
            return;
        }
        await this.projectCanonical();
        if (
            this.disposed
            || this.isIsolated
            || this.isSettled
            || this.projectionResumeArmed
        ) {
            return;
        }
        // Work admitted between the drain and this task can defer the
        // projection again. One armed nudge guarantees liveness; re-entering
        // immediately would enqueue retries onto the queue that has to drain.
        this.projectionResumeArmed = true;
        this.port.scheduleProjectionRetry?.(async () => {
            this.projectionResumeArmed = false;
            await this.resumeProjection();
        }, 50);
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
            // Every path that leaves canonical unprojected is a candidate
            // explanation for a document that never converges, so none of them
            // may be silent in a trace.
            this.recordDiagnostic({
                kind: 'projection-deferred',
                reason: this.disposed ? 'disposed'
                    : this.isIsolated ? 'isolated'
                    : this.input.hasBufferedInput ? 'buffered-input'
                    : 'disk-mutation',
                canonicalBefore: this.canonicalContent,
                editor: this.editorContent,
            });
            return;
        }
        if (this.projectionInProgress) {
            this.projectionPending = true;
            this.recordDiagnostic({
                kind: 'projection-deferred',
                reason: 'projection-in-progress',
                canonicalBefore: this.canonicalContent,
                editor: this.editorContent,
            });
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
            // A projection re-reads disk before it writes, so a remote move
            // applied as delete-then-create can make this path observe a file
            // that is momentarily gone. That is the one failure here this
            // actor cannot adjudicate, and isolating on it archives a conflict
            // whose local side is the untouched baseline. Hand it to the
            // resource scan, which holds the entity identity the move carried.
            if (normalized instanceof UnattributedDiskSnapshotError) {
                throw normalized;
            }
            await this.isolate(normalized.message);
            throw normalized;
        } finally {
            this.projectionInProgress = false;
            for (const resolve of this.projectionRetirementWaiters.splice(0)) {
                resolve();
            }
        }
    }

    /**
     * Project remote OT at a finite durability cut without writing the newer,
     * still-buffered local suffix to disk. The suffix remains owned by its
     * event-time ancestor and will be transformed again when journaled.
     */
    private async projectBufferedBoundary(): Promise<void> {
        const pendingEditorObservations = this.port.hasPendingEditorObservations?.() ?? false;
        this.recordDiagnostic({
            kind: 'boundary-enter',
            canonicalBefore: this.canonicalContent,
            editor: this.editorContent,
            diskBefore: this.diskContent,
            bufferedInput: this.input.hasBufferedInput,
            diskMutation: this.diskMutationInProgress,
            pendingEditorObservations,
        });
        if (
            this.disposed
            || this.isIsolated
            || this.diskMutationInProgress
            || pendingEditorObservations
        ) {
            this.recordDiagnostic({
                kind: 'boundary-deferred',
                reason: this.disposed
                    ? 'disposed'
                    : this.isIsolated
                        ? 'isolated'
                        : this.diskMutationInProgress
                            ? 'disk-mutation'
                            : 'pending-editor-observations',
            });
            this.projectionPending = true;
            return;
        }
        const editorTarget = this.input.previewAgainst(this.canonicalContent);
        this.recordDiagnostic({
            kind: 'boundary-preview',
            canonicalBefore: this.canonicalContent,
            editor: this.editorContent,
            target: editorTarget,
        });
        if (editorTarget === undefined) {
            await this.projectCanonical();
            return;
        }
        if (this.editorContent === undefined) {
            this.recordDiagnostic({kind: 'boundary-deferred', reason: 'editor-unavailable'});
            return;
        }
        if (this.editorContent === editorTarget) {
            const adopted = this.input.adoptProjectedPreview(this.canonicalContent, editorTarget);
            this.recordDiagnostic({
                kind: adopted ? 'boundary-adopted' : 'boundary-deferred',
                reason: adopted ? 'already-visible' : 'preview-advanced',
            });
            if (!adopted) {
                this.projectionPending = true;
            }
            return;
        }
        const expectedEditorContent = this.editorContent;
        if (this.port.readEditor) {
            const observedEditor = await this.port.readEditor();
            if (
                observedEditor !== expectedEditorContent
                || this.port.hasPendingEditorObservations?.()
            ) {
                this.recordDiagnostic({
                    kind: 'boundary-deferred',
                    reason: observedEditor !== expectedEditorContent
                        ? 'editor-preflight-advanced'
                        : 'pending-editor-observations-after-preflight',
                });
                this.projectionPending = true;
                return;
            }
        }
        const projectionCanonical = this.canonicalContent;
        const receipt = this.projections.recordTextMerge(
            editorProjectionPath(this.path),
            expectedEditorContent,
            editorTarget,
        );
        await this.persistProjectionReceipts();
        if (
            this.disposed
            || this.isIsolated
            || this.port.hasPendingEditorObservations?.()
            || this.canonicalContent !== projectionCanonical
        ) {
            await this.cancelProjectionReceipt(receipt.id);
            this.projectionPending = true;
            return;
        }
        let projectionTarget = editorTarget;
        let projectionExpected = expectedEditorContent;
        if (
            this.editorContent !== expectedEditorContent
            || this.input.previewAgainst(this.canonicalContent) !== editorTarget
        ) {
            const currentPreview = this.input.previewAgainst(this.canonicalContent);
            if (currentPreview === undefined || this.editorContent === undefined) {
                await this.cancelProjectionReceipt(receipt.id);
                this.projectionPending = true;
                return;
            }
            projectionTarget = currentPreview;
            projectionExpected = this.editorContent;
        }
        let applied: boolean | void;
        try {
            applied = await this.port.projectEditor(projectionTarget, projectionExpected);
        } catch (error) {
            await this.cancelProjectionReceipt(receipt.id);
            throw error;
        }
        if (applied === false) {
            this.recordDiagnostic({kind: 'boundary-deferred', reason: 'editor-cas-rejected'});
            await this.cancelProjectionReceipt(receipt.id);
            this.projectionPending = true;
            return;
        }
        if (!this.input.adoptProjectedPreview(this.canonicalContent, projectionTarget)) {
            this.recordDiagnostic({kind: 'boundary-deferred', reason: 'preview-advanced-after-cas'});
            await this.cancelProjectionReceipt(receipt.id);
            this.projectionPending = true;
            return;
        }
        this.editorContent = projectionTarget;
        this.lastProjectedEditorTarget = projectionTarget;
        this.recordDiagnostic({
            kind: 'boundary-projected',
            canonicalBefore: this.canonicalContent,
            editor: this.editorContent,
            target: projectionTarget,
        });
    }

    private async projectCanonicalOnce(): Promise<void> {
        if (this.disposed || this.isIsolated || this.input.hasBufferedInput) {
            if (this.input.hasBufferedInput && !this.disposed && !this.isIsolated) {
                this.recordDiagnostic({
                    kind: 'projection-deferred',
                    reason: 'buffered-input',
                    canonicalBefore: this.canonicalContent,
                    editor: this.editorContent,
                });
            }
            return;
        }
        // Frozen editor observations are causal transitions, not latest-value
        // notifications. Let the document queue consume them before a remote
        // projection can advance the editor head they are based on. The drain
        // itself re-enters this path once it has admitted them; rescheduling
        // here instead would enqueue retries onto the very queue the drain
        // needs, and each retry would defer again and starve it.
        if (this.port.hasPendingEditorObservations?.()) {
            this.recordDiagnostic({
                kind: 'projection-deferred',
                reason: 'pending-editor-observations',
                canonicalBefore: this.canonicalContent,
                editor: this.editorContent,
            });
            return;
        }
        const editorTarget = this.canonicalContent;
        if (this.editorContent !== undefined && this.editorContent !== editorTarget) {
            const observedEditor = await this.port.readEditor?.();
            if (
                observedEditor !== undefined
                && observedEditor !== this.editorContent
            ) {
                // The visible editor can still be the machine's already-journaled
                // local branch while the canonical OT head has advanced behind
                // it. Import only a true forward observation. Re-importing the
                // unchanged local branch against the newer canonical snapshot
                // would duplicate every journaled local edit and starve remote
                // projection during continuous input.
                await this.onEditorChanged(
                    observedEditor,
                    this.editorContent,
                    false,
                    true,
                );
                await this.input.flush(true);
                return;
            }
            if (observedEditor === undefined && this.editorContent !== undefined) {
                this.editorContent = undefined;
            }
        }
        if (this.input.hasBufferedInput) {
            await this.input.flush(true);
            return;
        }
        // A clean buffer was once refreshed by writing disk and waiting for the
        // host to reload it, so that TextEditor.edit could not turn a remote
        // revision into an unsaved user mutation. That made remote visibility a
        // function of watcher and reload latency, which is unbounded while a
        // burst of revisions keeps rewriting the same file, so remote text
        // could stay invisible for seconds. Project every open editor through
        // the versioned editor CAS and restore the saved state afterwards: the
        // buffer still never keeps bytes the user did not write, but seeing
        // them no longer waits on a reload.
        const restoreSavedEditor = this.editorContent !== undefined
            && this.editorContent !== editorTarget
            && !this.editorDirty
            && this.port.restoreSavedEditor !== undefined;
        if (this.editorContent !== undefined && this.editorContent !== editorTarget) {
            const expectedEditorContent = this.editorContent;
            const receipt = this.projections.record(editorProjectionPath(this.path), {
                kind: 'change',
                hash: textHash(editorTarget),
            });
            await this.persistProjectionReceipts();
            if (
                this.disposed
                || this.isIsolated
                || this.port.hasPendingEditorObservations?.()
                || this.editorContent !== expectedEditorContent
                || this.canonicalContent !== editorTarget
            ) {
                await this.cancelProjectionReceipt(receipt.id);
                this.projectionPending = true;
                return;
            }
            if (this.input.hasBufferedInput) {
                await this.cancelProjectionReceipt(receipt.id);
                await this.input.flush(true);
                return;
            }
            this.recordDiagnostic({
                kind: 'project-editor',
                ancestor: expectedEditorContent,
                target: editorTarget,
                editor: this.editorContent,
                canonicalBefore: this.canonicalContent,
                ancestorAccountedFor: this.classifyEditorAncestor(),
                // An unsaved buffer is the case where an overwrite is
                // destructive rather than merely redundant, so the trace has to
                // carry it next to the ancestor classification.
                editorDirty: this.editorDirty,
            });
            try {
                const applied = await this.port.projectEditor(
                    editorTarget,
                    expectedEditorContent,
                );
                if (applied === false) {
                    await this.cancelProjectionReceipt(receipt.id);
                    return;
                }
            } catch (error) {
                await this.cancelProjectionReceipt(receipt.id);
                if (await this.importUnobservedEditor()) {
                    return;
                }
                throw error;
            }
            if (this.disposed || this.isIsolated) {
                return;
            }
            this.editorContent = editorTarget;
            this.lastProjectedEditorTarget = editorTarget;
            if (restoreSavedEditor) {
                this.projectedEditorNeedsSave = true;
            }
            if (this.canonicalContent !== editorTarget) {
                this.projectionPending = true;
                return;
            }
        }
        const diskTarget = this.canonicalContent;
        // A buffer this projection had to dirty installs its own bytes. Writing
        // the file first and saving the buffer afterwards makes VS Code refuse
        // that save: the file changed behind the editor it is written from,
        // which is exactly the dirty write its host protects against, and the
        // document would keep an unsaved marker for bytes nobody typed.
        if (this.projectedEditorNeedsSave) {
            await this.saveProjectedEditor(diskTarget);
            if (this.projectedEditorNeedsSave) {
                // The save was postponed rather than abandoned, so the buffer
                // is still the writer of these bytes. Writing the file now is
                // the very ordering described above: the host would see the
                // file change behind a dirty editor and refuse its save from
                // then on, stranding an unsaved marker on text nobody typed.
                // isSettled reports this document as unfinished until the
                // postponed save runs, so nothing reads the wait as converged.
                return;
            }
        }
        await this.projectDiskTarget(diskTarget);
        await this.notifyConverged();
    }

    /**
     * Install a projection through the save of the buffer it dirtied, returning
     * that document to the saved state the user left it in. Skipped while newer
     * work exists, so a burst of remote revisions costs one save at its end.
     */
    private async saveProjectedEditor(projected: string): Promise<void> {
        const blocked = this.disposed ? 'disposed'
            : this.isIsolated ? 'isolated'
            : this.projectionPending ? 'projection-pending'
            : this.input.hasBufferedInput ? 'buffered-input'
            : this.diskMutationInProgress ? 'disk-mutation'
            : this.editorContent !== projected ? 'editor-advanced'
            : this.canonicalContent !== projected ? 'canonical-advanced'
            : this.port.hasPendingEditorObservations?.() ? 'pending-editor-observations'
            : undefined;
        if (blocked !== undefined) {
            this.recordDiagnostic({kind: 'restore-skipped', reason: blocked});
            if (blocked !== 'projection-pending' && blocked !== 'pending-editor-observations') {
                // Only a queue that has yet to drain is worth waiting for. In
                // every other case this buffer is no longer the saved document
                // this projection dirtied, so nothing is owed.
                this.projectedEditorNeedsSave = false;
            }
            return;
        }
        // VS Code's writer does not compare and swap. Save only over the disk
        // snapshot this machine last observed, so the save can never overwrite
        // a raw writer that has already landed; a writer that lands afterwards
        // is the same race an ordinary user save runs.
        const expectedDiskContent = this.diskContent;
        if (this.port.readDisk) {
            const observedDisk = await this.port.readDisk();
            if (observedDisk !== expectedDiskContent) {
                this.recordDiagnostic({kind: 'restore-skipped', reason: 'disk-not-ours'});
                this.projectedEditorNeedsSave = false;
                await this.onDiskObserved(observedDisk);
                return;
            }
        }
        if (
            this.disposed
            || this.isIsolated
            || this.editorContent !== projected
            || this.canonicalContent !== projected
            || this.diskContent !== expectedDiskContent
            || this.input.hasBufferedInput
        ) {
            return;
        }
        this.recordDiagnostic({kind: 'editor-save-restore', target: projected});
        // beforeEditorSave normally records the receipt that classifies a save's
        // disk write. It is queued on the document scheduler behind this very
        // projection, so the write would land before it runs and be read as
        // external input. Record it here and let that queued callback recognise
        // its own restore instead.
        const receipt = this.projections.record(diskProjectionPath(this.path), {
            kind: expectedDiskContent === undefined ? 'create' : 'change',
            hash: textHash(projected),
        });
        this.savedDiskAncestors.push({receiptId: receipt.id, content: projected});
        this.pendingRestoreSaves.push(projected);
        await this.persistProjectionReceipts();
        let saved: boolean;
        try {
            saved = await this.port.restoreSavedEditor?.(projected) ?? false;
        } catch (error) {
            this.forgetPendingRestoreSave(projected);
            await this.cancelProjectionReceipts(receipt.id);
            throw error;
        }
        if (!saved) {
            // The host refused or skipped the save, so no write carries this
            // receipt. Leave the disk projection to the CAS below and let the
            // document settle: a marker that cannot be cleared must not also
            // stop this replica from converging.
            this.forgetPendingRestoreSave(projected);
            await this.cancelProjectionReceipts(receipt.id);
            this.projectedEditorNeedsSave = false;
            this.recordDiagnostic({kind: 'restore-refused', target: projected});
            return;
        }
        this.projectedEditorNeedsSave = false;
        if (this.disposed || this.isIsolated) {
            return;
        }
        // The save wrote the file. Adopt those bytes so the disk CAS does not
        // write them a second time behind the buffer that just saved them.
        if (await this.port.readDisk?.() === projected && !this.disposed && !this.isIsolated) {
            this.diskContent = projected;
            this.recordDiagnostic({
                kind: 'disk-adopted',
                reason: 'editor-save',
                ancestor: expectedDiskContent,
                target: projected,
            });
        }
    }

    private forgetPendingRestoreSave(projected: string): void {
        const pending = this.pendingRestoreSaves.indexOf(projected);
        if (pending >= 0) {
            this.pendingRestoreSaves.splice(pending, 1);
        }
    }

    private async projectDiskTarget(diskTarget: string): Promise<void> {
        if (
            this.diskMutationInProgress
            || this.diskContent === diskTarget
            || (this.editorContent !== undefined && this.editorContent !== diskTarget)
        ) {
            return;
        }
        const expectedDiskContent = this.diskContent;
        // A raw filesystem writer may have changed the file before the watcher
        // callback ran. Import that snapshot against the last observed disk
        // base instead of overwriting it with remote text.
        if (this.port.readDisk) {
            const observedDisk = await this.port.readDisk();
            if (observedDisk !== this.diskContent) {
                await this.onDiskObserved(observedDisk);
                return;
            }
        }
        const diskReceipt = this.projections.record(diskProjectionPath(this.path), {
            kind: this.diskContent === undefined ? 'create' : 'change',
            hash: textHash(diskTarget),
        });
        await this.persistProjectionReceipts();
        if (
            this.disposed
            || this.isIsolated
            || this.diskMutationInProgress
            || this.diskContent !== expectedDiskContent
            || this.canonicalContent !== diskTarget
        ) {
            await this.cancelProjectionReceipts(diskReceipt.id);
            this.projectionPending = true;
            return;
        }
        if (this.input.hasBufferedInput) {
            await this.cancelProjectionReceipts(diskReceipt.id);
            await this.input.flush(true);
            return;
        }
        if (this.port.readDisk) {
            const observedDisk = await this.port.readDisk();
            if (observedDisk !== expectedDiskContent) {
                await this.cancelProjectionReceipts(diskReceipt.id);
                await this.onDiskObserved(observedDisk);
                return;
            }
        }
        this.recordDiagnostic({
            kind: 'project-disk',
            ancestor: expectedDiskContent,
            target: diskTarget,
        });
        let projectionOutcome: boolean | void | DiskProjectionOutcome;
        try {
            projectionOutcome = await this.port.projectDisk(
                diskTarget,
                expectedDiskContent,
            );
        } catch (error) {
            await this.cancelProjectionReceipts(diskReceipt.id);
            throw error;
        }
        const applied = typeof projectionOutcome === 'object'
            ? projectionOutcome.applied
            : projectionOutcome;
        if (applied === false) {
            // A projection is defined by bytes, not by which writer installed
            // them. VS Code auto-saving the buffer this projection just filled
            // wins the compare-and-swap with the exact target content; that is
            // this projection completing, not an external mutation to import.
            // The receipt stays so the winning write's watcher echo remains
            // classified as plugin output.
            if (
                (typeof projectionOutcome === 'object'
                    && projectionOutcome.actualContent === diskTarget)
                || await this.port.readDisk?.() === diskTarget
            ) {
                if (this.disposed || this.isIsolated) {
                    return;
                }
                this.diskContent = diskTarget;
                this.recordDiagnostic({
                    kind: 'disk-adopted',
                    target: diskTarget,
                    ancestor: expectedDiskContent,
                });
                if (this.canonicalContent !== diskTarget) {
                    this.projectionPending = true;
                    return;
                }
                await this.notifyConverged();
                return;
            }
            await this.cancelProjectionReceipts(diskReceipt.id);
            if (
                typeof projectionOutcome === 'object'
                && projectionOutcome.installedAncestor === diskTarget
                && projectionOutcome.actualContent !== undefined
            ) {
                // The raw writer edited the exact inode that this projection
                // installed. Use that installed content as the event-time
                // ancestor, otherwise journaled bytes can be submitted twice.
                this.diskContent = diskTarget;
                await this.onDiskObserved(projectionOutcome.actualContent);
                return;
            }
            const observedDisk = await this.port.readDisk?.();
            if (observedDisk === expectedDiskContent) {
                this.port.scheduleProjectionRetry?.(
                    () => this.projectCanonical(),
                    100,
                );
                return;
            }
            await this.onDiskObserved(observedDisk);
            return;
        }
        if (this.disposed || this.isIsolated) {
            return;
        }
        this.diskContent = diskTarget;
        if (this.canonicalContent !== diskTarget) {
            this.projectionPending = true;
            return;
        }
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

    private async cancelProjectionReceipt(receiptId: string): Promise<void> {
        await this.cancelProjectionReceipts(receiptId);
    }

    private async cancelProjectionReceipts(
        ...receiptIds: Array<string | undefined>
    ): Promise<void> {
        let changed = false;
        for (const receiptId of receiptIds) {
            if (receiptId !== undefined && this.projections.cancel(receiptId)) {
                changed = true;
            }
        }
        if (changed) {
            await this.persistProjectionReceipts();
        }
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

    private recordDiagnostic(
        event: Omit<TextReplicaDiagnosticEvent, 'sequence'>,
    ): void {
        if (!this.diagnosticsEnabled) {
            return;
        }
        this.diagnostics.push({
            sequence: ++this.diagnosticSequence,
            elapsedMs: Date.now() - this.diagnosticsStartedAt,
            ...event,
        });
        // A streaming run emits a few hundred events before the first symptom
        // shows, and the cause is always older than the symptom. A ring that
        // wraps mid-run drops exactly the transition worth reading, so keep
        // enough history to cover a whole run; the entries are fingerprints
        // and short strings, and the ring only exists while E2E diagnostics
        // are enabled.
        if (this.diagnostics.length > 4096) {
            this.diagnostics.shift();
        }
    }

    private async isolate(reason: string, editorContent = this.editorContent): Promise<void> {
        if (this.disposed || this.isolatedReason) {
            return;
        }
        this.isolatedReason = reason;
        // A journal commit may already have crossed the durability boundary
        // when the observation that triggered isolation is reported. Drain it
        // before freezing evidence so the conflict record cannot describe an
        // older canonical head than the WAL/session.
        await this.input.disposeAndWait();
        await this.port.isolate({
            path: this.path,
            ...(this.ownedRemoteEntityId
                ? {remoteEntityId: this.ownedRemoteEntityId}
                : {}),
            reason,
            canonicalContent: this.canonicalContent,
            editorContent,
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
