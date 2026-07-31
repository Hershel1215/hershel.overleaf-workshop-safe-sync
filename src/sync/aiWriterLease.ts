import { randomUUID } from 'crypto';

export class AiWriterLeaseHeldError extends Error {}
export class InvalidAiWriterLeaseError extends Error {}

export interface AiWriterLease {
    documentId: string;
    holder: string;
    leaseId: string;
    fencingToken: number;
    baseRevision: number;
    expiresAt: number;
}

export interface SerializedAiWriterLeaseState {
    nextFencingToken: number;
    leases: AiWriterLease[];
}

/** One exclusive AI read-edit-commit transaction per document. */
export class AiWriterLeaseManager {
    private readonly leases = new Map<string, AiWriterLease>();
    private nextFencingToken = 1;

    constructor(
        private readonly leaseDurationMs = 30000,
        private readonly idFactory: () => string = randomUUID,
    ) {
        if (!Number.isInteger(leaseDurationMs) || leaseDurationMs <= 0) {
            throw new Error('AI writer lease duration must be positive');
        }
    }

    acquire(
        documentId: string,
        holder: string,
        baseRevision: number,
        now = Date.now(),
    ): AiWriterLease {
        this.expire(documentId, now);
        const existing = this.leases.get(documentId);
        if (existing) {
            throw new AiWriterLeaseHeldError(
                `AI writer lease is already held for ${documentId} by ${existing.holder}`,
            );
        }
        const lease: AiWriterLease = {
            documentId,
            holder,
            leaseId: this.idFactory(),
            fencingToken: this.nextFencingToken++,
            baseRevision,
            expiresAt: now + this.leaseDurationMs,
        };
        this.leases.set(documentId, lease);
        return { ...lease };
    }

    heartbeat(
        documentId: string,
        leaseId: string,
        fencingToken: number,
        now = Date.now(),
    ): AiWriterLease {
        const lease = this.requireLease(documentId, leaseId, fencingToken, now);
        lease.expiresAt = now + this.leaseDurationMs;
        return { ...lease };
    }

    assertCanCommit(
        documentId: string,
        leaseId: string,
        fencingToken: number,
        now = Date.now(),
    ): AiWriterLease {
        return { ...this.requireLease(documentId, leaseId, fencingToken, now) };
    }

    release(documentId: string, leaseId: string, fencingToken: number): void {
        const lease = this.leases.get(documentId);
        if (!lease || lease.leaseId !== leaseId || lease.fencingToken !== fencingToken) {
            throw new InvalidAiWriterLeaseError(`AI writer lease is no longer valid for ${documentId}`);
        }
        this.leases.delete(documentId);
    }

    serialize(): SerializedAiWriterLeaseState {
        return {
            nextFencingToken: this.nextFencingToken,
            leases: [...this.leases.values()].map(lease => ({ ...lease })),
        };
    }

    restore(state: SerializedAiWriterLeaseState): void {
        if (!Number.isInteger(state.nextFencingToken) || state.nextFencingToken < 1) {
            throw new Error('Invalid AI writer fencing-token state');
        }
        this.nextFencingToken = state.nextFencingToken;
        this.leases.clear();
        for (const lease of state.leases) {
            if (this.leases.has(lease.documentId)) {
                throw new Error(`Duplicate AI writer lease for ${lease.documentId}`);
            }
            this.leases.set(lease.documentId, { ...lease });
            this.nextFencingToken = Math.max(this.nextFencingToken, lease.fencingToken + 1);
        }
    }

    private requireLease(
        documentId: string,
        leaseId: string,
        fencingToken: number,
        now: number,
    ): AiWriterLease {
        this.expire(documentId, now);
        const lease = this.leases.get(documentId);
        if (!lease || lease.leaseId !== leaseId || lease.fencingToken !== fencingToken) {
            throw new InvalidAiWriterLeaseError(`AI writer lease is no longer valid for ${documentId}`);
        }
        return lease;
    }

    private expire(documentId: string, now: number): void {
        const lease = this.leases.get(documentId);
        if (lease && lease.expiresAt <= now) {
            this.leases.delete(documentId);
        }
    }
}
