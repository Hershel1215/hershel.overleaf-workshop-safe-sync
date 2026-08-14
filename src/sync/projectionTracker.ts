import { randomUUID } from 'crypto';
import { TextOperation } from './textOperation';

export type ProjectionKind = 'create' | 'change' | 'delete';

export interface ProjectionReceipt {
    id: string;
    path: string;
    kind: ProjectionKind;
    expectedHash?: string;
    expectedIdentity?: string;
    /** Durable OT projection that can be rebased over newer editor input. */
    mergeBase?: string;
    mergeTarget?: string;
}

export interface ProjectionObservation {
    kind: ProjectionKind;
    hash?: string;
    identity?: string;
    ancestor?: string;
    content?: string;
}

export type ProjectionClassification =
    | { kind: 'projection'; projectionId: string }
    | { kind: 'mutation' };

/**
 * Matches watcher observations to writes emitted by the replica itself.
 * Receipts use evidence, never a time-based suppression window.
 */
export class ProjectionTracker {
    private readonly pending = new Map<string, ProjectionReceipt[]>();
    private readonly settled = new Map<string, ProjectionReceipt>();

    constructor(private readonly idFactory: () => string = randomUUID) {}

    record(
        path: string,
        observation: ProjectionObservation,
    ): ProjectionReceipt {
        const receipt: ProjectionReceipt = {
            id: this.idFactory(),
            path,
            kind: observation.kind,
            expectedHash: observation.hash,
            expectedIdentity: observation.identity,
        };
        const receipts = this.pending.get(path) ?? [];
        receipts.push(receipt);
        this.pending.set(path, receipts);
        return receipt;
    }

    recordTextMerge(path: string, mergeBase: string, mergeTarget: string): ProjectionReceipt {
        const receipt: ProjectionReceipt = {
            id: this.idFactory(),
            path,
            kind: 'change',
            mergeBase,
            mergeTarget,
        };
        const receipts = this.pending.get(path) ?? [];
        receipts.push(receipt);
        this.pending.set(path, receipts);
        return receipt;
    }

    observe(path: string, observation: ProjectionObservation): ProjectionClassification {
        const receipts = this.pending.get(path) ?? [];
        const matchingIndex = receipts.findIndex(receipt => matches(receipt, observation));
        if (matchingIndex >= 0) {
            const receipt = receipts[matchingIndex];
            const remaining = receipts.slice(matchingIndex + 1);
            if (remaining.length > 0) {
                this.pending.set(path, remaining);
            } else {
                this.pending.delete(path);
            }
            this.settled.set(path, receipt.mergeBase === undefined
                ? receipt
                : {
                    id: receipt.id,
                    path: receipt.path,
                    kind: receipt.kind,
                    expectedHash: observation.hash,
                    expectedIdentity: observation.identity,
                });
            return { kind: 'projection', projectionId: receipt.id };
        }

        const settled = this.settled.get(path);
        if (settled && matches(settled, observation)) {
            return { kind: 'projection', projectionId: settled.id };
        }

        this.pending.delete(path);
        this.settled.delete(path);
        return { kind: 'mutation' };
    }

    /** Remove evidence for a projection that provably never reached its endpoint. */
    cancel(receiptId: string): boolean {
        let removed = false;
        for (const [path, receipts] of this.pending) {
            const remaining = receipts.filter(receipt => receipt.id !== receiptId);
            if (remaining.length !== receipts.length) {
                removed = true;
                if (remaining.length > 0) {
                    this.pending.set(path, remaining);
                } else {
                    this.pending.delete(path);
                }
            }
        }
        for (const [path, receipt] of this.settled) {
            if (receipt.id === receiptId) {
                this.settled.delete(path);
                removed = true;
            }
        }
        return removed;
    }

    matches(path: string, observation: ProjectionObservation): boolean {
        return (this.pending.get(path) ?? []).some(receipt => matches(receipt, observation))
            || matchesOptional(this.settled.get(path), observation);
    }

    hasReceipt(receiptId: string): boolean {
        return [...this.pending.values()].some(receipts => (
            receipts.some(receipt => receipt.id === receiptId)
        )) || [...this.settled.values()].some(receipt => receipt.id === receiptId);
    }

    serialize(): ProjectionReceipt[] {
        return [...this.pending.values()].flat().map(receipt => ({ ...receipt }));
    }

    restore(receipts: readonly ProjectionReceipt[]): void {
        this.pending.clear();
        this.settled.clear();
        for (const receipt of receipts) {
            const pending = this.pending.get(receipt.path) ?? [];
            pending.push({ ...receipt });
            this.pending.set(receipt.path, pending);
        }
    }
}

function matches(receipt: ProjectionReceipt, observation: ProjectionObservation): boolean {
    if (receipt.mergeBase !== undefined || receipt.mergeTarget !== undefined) {
        if (
            receipt.kind !== 'change'
            || receipt.mergeBase === undefined
            || receipt.mergeTarget === undefined
            || observation.kind !== 'change'
            || observation.ancestor === undefined
            || observation.content === undefined
        ) {
            return false;
        }
        try {
            const remoteOperation = TextOperation.fromDiff(
                receipt.mergeBase,
                receipt.mergeTarget,
            );
            const localOperation = TextOperation.fromDiff(
                receipt.mergeBase,
                observation.ancestor,
            );
            const [, localPrime] = TextOperation.transform(remoteOperation, localOperation);
            return localPrime.apply(receipt.mergeTarget) === observation.content;
        } catch {
            return false;
        }
    }
    return receipt.kind === observation.kind
        && receipt.expectedHash === observation.hash
        && (
            receipt.expectedIdentity === undefined
            || receipt.expectedIdentity === observation.identity
        );
}

function matchesOptional(
    receipt: ProjectionReceipt | undefined,
    observation: ProjectionObservation,
): boolean {
    return receipt !== undefined && matches(receipt, observation);
}
