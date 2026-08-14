import { randomUUID } from 'crypto';
import { TextComponent, TextOperation } from './textOperation';

export interface SerializedOperationEnvelope {
    ids: string[];
    operation: TextComponent[];
}

export interface SerializedOtClientState {
    schemaVersion: 1;
    revision: number;
    serverContent: string;
    localContent: string;
    outstanding?: SerializedOperationEnvelope;
    pending?: SerializedOperationEnvelope;
    /** Transport recovery metadata persisted by DocumentSession. */
    submittedSources?: string[];
    lastSubmittedRevision?: number;
}

export interface OutgoingOperation {
    revision: number;
    ids: string[];
    operation: TextOperation;
}

export interface ReceiveResult {
    acknowledgedIds: string[];
    localChanged: boolean;
    send?: OutgoingOperation;
}

interface OperationEnvelope {
    ids: string[];
    operation: TextOperation;
}

/**
 * Client-side OT state with one in-flight operation and one composed pending
 * operation. It deliberately has no transport or VS Code dependencies so the
 * convergence and crash-recovery rules can be tested in isolation.
 */
export class OtClientState {
    private outstanding?: OperationEnvelope;
    private pending?: OperationEnvelope;

    constructor(
        private revision: number,
        private serverContent: string,
        private localContent: string = serverContent,
        private readonly idFactory: () => string = randomUUID,
    ) {
        if (!Number.isInteger(revision) || revision < 0) {
            throw new Error(`Invalid document revision: ${revision}`);
        }
    }

    get currentRevision(): number {
        return this.revision;
    }

    get acknowledgedContent(): string {
        return this.serverContent;
    }

    get visibleContent(): string {
        return this.localContent;
    }

    get hasOutstandingOperation(): boolean {
        return this.outstanding !== undefined;
    }

    get hasPendingOperations(): boolean {
        return this.outstanding !== undefined || this.pending !== undefined;
    }

    get activeOperationIds(): readonly string[] {
        return [
            ...(this.outstanding?.ids ?? []),
            ...(this.pending?.ids ?? []),
        ];
    }

    get outstandingOperation(): TextOperation | undefined {
        return this.outstanding?.operation;
    }

    get currentOutgoingOperation(): OutgoingOperation | undefined {
        return this.outstanding ? this.outgoing(this.outstanding) : undefined;
    }

    submitSnapshot(content: string, id?: string): { id?: string; send?: OutgoingOperation } {
        return this.submitOperation(TextOperation.fromDiff(this.localContent, content), id);
    }

    submitOperation(
        operation: TextOperation,
        suppliedId?: string,
        suppliedIds?: readonly string[],
    ): { id?: string; send?: OutgoingOperation } {
        if (operation.baseLength !== this.localContent.length) {
            throw new Error('Local operation does not apply to the visible document');
        }
        if (operation.isNoop) {
            return {};
        }
        const ids = suppliedIds === undefined
            ? [suppliedId ?? this.idFactory()]
            : [...suppliedIds];
        if (ids.length === 0 || ids.some(id => typeof id !== 'string' || id.length === 0)) {
            throw new Error('Operation id must not be empty');
        }
        this.localContent = operation.apply(this.localContent);
        if (this.outstanding) {
            if (this.pending) {
                this.pending = {
                    ids: [...this.pending.ids, ...ids],
                    operation: this.pending.operation.compose(operation),
                };
            } else {
                this.pending = { ids, operation };
            }
            return { id: ids[0] };
        }

        this.outstanding = { ids, operation };
        return { id: ids[0], send: this.outgoing(this.outstanding) };
    }

    /**
     * Apply the next authoritative server operation.
     *
     * Overleaf's ShareJS client transforms the in-flight client operation on
     * the left and the authoritative server operation on the right. This is
     * observable for concurrent inserts at the same position: the in-flight
     * client insertion stays before the server insertion. An own operation
     * must match that transformed outstanding target before it is acknowledged.
     */
    receive(operation: TextOperation, revision: number, ownOperation: boolean): ReceiveResult {
        if (revision !== this.revision) {
            throw new Error(`Expected server revision ${this.revision}, received ${revision}`);
        }
        if (operation.baseLength !== this.serverContent.length) {
            throw new Error('Server operation does not apply to the acknowledged document');
        }

        if (ownOperation) {
            if (!this.outstanding) {
                throw new Error('Received an own acknowledgement without an outstanding operation');
            }
            const actualContent = operation.apply(this.serverContent);
            const expectedContent = this.outstanding.operation.apply(this.serverContent);
            if (actualContent !== expectedContent) {
                throw new Error('Acknowledged operation differs from the transformed outstanding operation');
            }

            const acknowledgedIds = [...this.outstanding.ids];
            this.serverContent = actualContent;
            this.revision += 1;
            this.outstanding = undefined;
            let send: OutgoingOperation | undefined;
            if (this.pending && !this.pending.operation.isNoop) {
                this.outstanding = this.pending;
                this.pending = undefined;
                send = this.outgoing(this.outstanding);
            } else {
                // A composed pending operation can cancel itself before it is
                // ever sent. Its source operations are nevertheless settled:
                // their visible effect is already represented by the now
                // acknowledged server state.
                acknowledgedIds.push(...(this.pending?.ids ?? []));
                this.pending = undefined;
            }
            if (!this.outstanding && this.localContent !== this.serverContent) {
                throw new Error('Client became idle before local and acknowledged content converged');
            }
            return { acknowledgedIds, localChanged: false, send };
        }

        const previousLocalContent = this.localContent;
        const newServerContent = operation.apply(this.serverContent);
        let remoteForLocal = operation;

        if (this.outstanding) {
            const [outstandingPrime, remotePrime] = TextOperation.transform(
                this.outstanding.operation,
                remoteForLocal,
            );
            remoteForLocal = remotePrime;
            this.outstanding.operation = outstandingPrime;
        }
        if (this.pending) {
            const [pendingPrime, remotePrime] = TextOperation.transform(
                this.pending.operation,
                remoteForLocal,
            );
            remoteForLocal = remotePrime;
            this.pending.operation = pendingPrime;
        }

        this.serverContent = newServerContent;
        this.localContent = remoteForLocal.apply(this.localContent);
        this.revision += 1;
        return {
            acknowledgedIds: [],
            localChanged: previousLocalContent !== this.localContent,
        };
    }

    serialize(): SerializedOtClientState {
        return {
            schemaVersion: 1,
            revision: this.revision,
            serverContent: this.serverContent,
            localContent: this.localContent,
            outstanding: this.serializeEnvelope(this.outstanding),
            pending: this.serializeEnvelope(this.pending),
        };
    }

    static restore(serialized: SerializedOtClientState, idFactory: () => string = randomUUID): OtClientState {
        if (serialized.schemaVersion !== 1) {
            throw new Error(`Unsupported OT state schema: ${serialized.schemaVersion}`);
        }
        const state = new OtClientState(
            serialized.revision,
            serialized.serverContent,
            serialized.localContent,
            idFactory,
        );
        state.outstanding = state.restoreEnvelope(serialized.outstanding);
        state.pending = state.restoreEnvelope(serialized.pending);
        state.assertConsistent();
        return state;
    }

    /**
     * Reconcile a durable state with a freshly joined server snapshot. This is
     * used after crashes or disconnects when an acknowledgement may have been
     * lost. The saved acknowledged content is the common ancestor.
     */
    static recover(
        serverContent: string,
        revision: number,
        serialized: SerializedOtClientState,
        idFactory: () => string = randomUUID,
    ): { state: OtClientState; send?: OutgoingOperation; mergedContent: string } {
        if (revision < serialized.revision) {
            throw new Error('Server revision moved backwards; the saved ancestry is no longer trustworthy');
        }

        const desiredContent = serialized.localContent;
        if (serverContent === desiredContent) {
            const recovered = new OtClientState(revision, serverContent, serverContent, idFactory);
            return { state: recovered, mergedContent: serverContent };
        }
        if (revision === serialized.revision && serverContent === serialized.serverContent) {
            const recovered = OtClientState.restore(serialized, idFactory);
            return {
                state: recovered,
                send: recovered.currentOutgoingOperation,
                mergedContent: desiredContent,
            };
        }
        throw new Error(
            'Cannot prove whether the saved local operation is already present in the server snapshot',
        );
    }

    private outgoing(envelope: OperationEnvelope): OutgoingOperation {
        return {
            revision: this.revision,
            ids: [...envelope.ids],
            operation: envelope.operation,
        };
    }

    private serializeEnvelope(envelope?: OperationEnvelope): SerializedOperationEnvelope | undefined {
        if (!envelope) {
            return undefined;
        }
        return {
            ids: [...envelope.ids],
            operation: envelope.operation.toJSON(),
        };
    }

    private restoreEnvelope(envelope?: SerializedOperationEnvelope): OperationEnvelope | undefined {
        if (!envelope) {
            return undefined;
        }
        return {
            ids: [...envelope.ids],
            operation: TextOperation.fromJSON(envelope.operation),
        };
    }

    private assertConsistent(): void {
        let expectedLocalContent = this.serverContent;
        if (this.outstanding) {
            expectedLocalContent = this.outstanding.operation.apply(expectedLocalContent);
        }
        if (this.pending) {
            if (!this.outstanding) {
                throw new Error('Persisted pending operation has no outstanding predecessor');
            }
            expectedLocalContent = this.pending.operation.apply(expectedLocalContent);
        }
        if (expectedLocalContent !== this.localContent) {
            throw new Error('Persisted OT state content does not match its operation queue');
        }
    }
}
