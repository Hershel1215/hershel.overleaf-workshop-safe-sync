import { randomUUID } from 'crypto';

export type ProjectionKind = 'create' | 'change' | 'delete';

export interface ProjectionReceipt {
    id: string;
    path: string;
    kind: ProjectionKind;
    expectedHash?: string;
    expectedIdentity?: string;
}

export interface ProjectionObservation {
    kind: ProjectionKind;
    hash?: string;
    identity?: string;
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
            this.settled.set(path, receipt);
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
    return receipt.kind === observation.kind
        && receipt.expectedHash === observation.hash
        && (
            receipt.expectedIdentity === undefined
            || receipt.expectedIdentity === observation.identity
        );
}
