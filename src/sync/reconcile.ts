import { BaselineEntry, BaselineFileType } from './replicaStateStore';
import { TextOperation } from './textOperation';

export interface SyncSnapshot {
    path: string;
    fileType: BaselineFileType;
    hash: string;
    entityId?: string;
    text?: string;
    treeHash?: string;
}

export type ReconcileDecision =
    | { action: 'baseline' | 'noop' | 'pull' | 'push' | 'delete-local' | 'delete-remote' }
    | { action: 'merge-text'; content: string }
    | { action: 'conflict'; reason: string };

export function decideReconciliation(
    baseline: BaselineEntry | undefined,
    local: SyncSnapshot | undefined,
    remote: SyncSnapshot | undefined,
): ReconcileDecision {
    if (!baseline) {
        if (!local && !remote) {
            return { action: 'noop' };
        }
        if (local && !remote) {
            return { action: 'push' };
        }
        if (!local && remote) {
            return { action: 'pull' };
        }
        if (local!.fileType !== remote!.fileType) {
            return { action: 'conflict', reason: 'Both sides created different entity types at the same path' };
        }
        if (local!.hash === remote!.hash) {
            return { action: 'baseline' };
        }
        return { action: 'conflict', reason: 'Both sides independently created different content at the same path' };
    }

    if (!local && !remote) {
        return { action: 'baseline' };
    }
    if (local && local.fileType !== baseline.fileType) {
        return { action: 'conflict', reason: 'Local entity type changed since the common baseline' };
    }
    if (remote && remote.fileType !== baseline.fileType) {
        return { action: 'conflict', reason: 'Remote entity type changed since the common baseline' };
    }

    const localUnchanged = snapshotMatchesBaseline(local, baseline);
    const remoteUnchanged = snapshotMatchesBaseline(remote, baseline);
    if (!local && remote) {
        return remoteUnchanged
            ? { action: 'delete-remote' }
            : { action: 'conflict', reason: 'Local deletion overlaps a remote modification' };
    }
    if (local && !remote) {
        return localUnchanged
            ? { action: 'delete-local' }
            : { action: 'conflict', reason: 'Remote deletion overlaps a local modification' };
    }

    if (local!.hash === remote!.hash) {
        return local!.hash === baseline.hash ? { action: 'noop' } : { action: 'baseline' };
    }
    if (localUnchanged && !remoteUnchanged) {
        return { action: 'pull' };
    }
    if (!localUnchanged && remoteUnchanged) {
        return { action: 'push' };
    }
    if (localUnchanged && remoteUnchanged) {
        return { action: 'noop' };
    }

    if (
        baseline.fileType === 'doc'
        && baseline.text !== undefined
        && local!.text !== undefined
        && remote!.text !== undefined
    ) {
        return {
            action: 'merge-text',
            content: mergeConcurrentText(baseline.text, local!.text, remote!.text),
        };
    }
    return { action: 'conflict', reason: 'Concurrent non-text modifications cannot be transformed' };
}

function snapshotMatchesBaseline(
    snapshot: SyncSnapshot | undefined,
    baseline: BaselineEntry,
): boolean {
    if (!snapshot || snapshot.hash !== baseline.hash) {
        return false;
    }
    if (baseline.fileType === 'folder' && baseline.treeHash !== undefined) {
        return snapshot.treeHash === baseline.treeHash;
    }
    return true;
}

/** Rebase local text behind the already-authoritative remote operation. */
export function mergeConcurrentText(base: string, local: string, remote: string): string {
    const remoteOperation = TextOperation.fromDiff(base, remote);
    const localOperation = TextOperation.fromDiff(base, local);
    const [, localPrime] = TextOperation.transform(remoteOperation, localOperation);
    return localPrime.apply(remote);
}
