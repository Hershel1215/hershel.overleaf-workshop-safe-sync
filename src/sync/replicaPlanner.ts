import { createHash } from 'crypto';
import {
    BaselineEntry,
    PendingReplicaAction,
    ReplicaState,
} from './replicaStateStore';
import { SyncSnapshot } from './reconcile';

export interface ReplicaSnapshot extends SyncSnapshot {
    content?: Uint8Array;
    localIdentity?: string;
    readonly?: boolean;
    localVariants?: {
        diskContent: Uint8Array;
        editorContent: Uint8Array;
        diskHash: string;
        editorHash: string;
        mergedContent?: Uint8Array;
    };
}

export type ReplicaMoveDecision =
    | { action: 'move-local'; oldPath: string; newPath: string; entityId: string }
    | { action: 'move-remote'; oldPath: string; newPath: string; entityId: string }
    | { action: 'adopt-move'; oldPath: string; newPath: string; entityId: string }
    | { action: 'conflict'; oldPath: string; reason: string };

export function attachTreeHashes(entries: Map<string, ReplicaSnapshot>): void {
    const paths = [...entries.keys()].sort();
    for (const [folderPath, folder] of entries) {
        if (folder.fileType !== 'folder') {
            continue;
        }
        const prefix = folderPath === '' ? '' : `${folderPath}/`;
        const descendants = paths
            .filter(candidate => candidate.startsWith(prefix) && candidate !== folderPath)
            .map(candidate => {
                const entry = entries.get(candidate)!;
                return [
                    candidate.slice(prefix.length),
                    entry.fileType,
                    entry.hash,
                ].join('\0');
            });
        folder.treeHash = createHash('sha256').update(descendants.join('\n')).digest('hex');
    }
}

export function findReplicaMove(
    entries: Record<string, BaselineEntry>,
    local: Map<string, ReplicaSnapshot>,
    remote: Map<string, ReplicaSnapshot>,
): ReplicaMoveDecision | undefined {
    const baselineByEntity = new Map(
        Object.entries(entries).map(([path, entry]) => [entry.entityId, { path, entry }]),
    );
    const remoteByEntity = new Map(
        [...remote].map(([path, entry]) => [entry.entityId, { path, entry }]),
    );

    for (const [entityId, baseline] of baselineByEntity) {
        const remoteEntity = remoteByEntity.get(entityId);
        if (remoteEntity && remoteEntity.path !== baseline.path) {
            const atOldPath = local.get(baseline.path);
            const atRemotePath = local.get(remoteEntity.path);
            if (!atOldPath && atRemotePath && sameLocalEntity(baseline.entry, atRemotePath)) {
                return {
                    action: 'adopt-move',
                    oldPath: baseline.path,
                    newPath: remoteEntity.path,
                    entityId,
                };
            }
            if (atOldPath && !atRemotePath) {
                return {
                    action: 'move-local',
                    oldPath: baseline.path,
                    newPath: remoteEntity.path,
                    entityId,
                };
            }
            return {
                action: 'conflict',
                oldPath: baseline.path,
                reason: `Remote entity moved to ${remoteEntity.path}, but the local path cannot be moved there safely`,
            };
        }

        if (!remoteEntity || !remote.has(baseline.path) || local.has(baseline.path)) {
            continue;
        }
        const candidates = [...local]
            .filter(([path, candidate]) => (
                entries[path] === undefined
                && sameLocalEntity(baseline.entry, candidate)
            ));
        if (candidates.length === 1) {
            return {
                action: 'move-remote',
                oldPath: baseline.path,
                newPath: candidates[0][0],
                entityId,
            };
        }
        if (candidates.length > 1) {
            return {
                action: 'conflict',
                oldPath: baseline.path,
                reason: 'A local move has multiple possible destinations',
            };
        }
    }
    return undefined;
}

export function refreshConvergedBaseline(
    state: ReplicaState,
    local: Map<string, ReplicaSnapshot>,
    remote: Map<string, ReplicaSnapshot>,
): void {
    const nextEntries: Record<string, BaselineEntry> = {};
    for (const [path, remoteEntry] of remote) {
        const localEntry = local.get(path);
        if (
            !localEntry
            || isPathConflicted(path, state)
            || localEntry.fileType !== remoteEntry.fileType
            || localEntry.hash !== remoteEntry.hash
            || (
                remoteEntry.fileType === 'folder'
                && localEntry.treeHash !== remoteEntry.treeHash
            )
        ) {
            const previous = state.entries[path];
            if (previous) {
                nextEntries[path] = previous;
            }
            continue;
        }
        nextEntries[path] = {
            entityId: remoteEntry.entityId ?? '',
            fileType: remoteEntry.fileType,
            hash: remoteEntry.hash,
            text: remoteEntry.fileType === 'doc' ? remoteEntry.text : undefined,
            treeHash: remoteEntry.fileType === 'folder' ? remoteEntry.treeHash : undefined,
            localIdentity: localEntry.localIdentity,
        };
    }
    for (const [path, previous] of Object.entries(state.entries)) {
        if (isPathConflicted(path, state) && nextEntries[path] === undefined) {
            nextEntries[path] = previous;
        }
    }
    state.entries = nextEntries;
}

export function validatePendingRecovery(
    pending: PendingReplicaAction,
    local: Map<string, ReplicaSnapshot>,
    remote: Map<string, ReplicaSnapshot>,
): void {
    const localHash = local.get(pending.path)?.hash;
    const remoteHash = remote.get(pending.path)?.hash;
    const targetLocalHash = pending.targetPath
        ? local.get(pending.targetPath)?.hash
        : localHash;
    const targetRemoteHash = pending.targetPath
        ? remote.get(pending.targetPath)?.hash
        : remoteHash;

    const allowed = (
        actual: string | undefined,
        allowAbsent: boolean,
        ...hashes: Array<string | undefined>
    ) => (
        (actual === undefined && allowAbsent)
        || hashes.some(hash => hash !== undefined && actual === hash)
    );
    switch (pending.kind) {
        case 'push':
            if (!allowed(
                localHash,
                pending.expectedLocalHash === undefined,
                pending.expectedLocalHash,
                pending.targetHash,
            ) || !allowed(
                remoteHash,
                pending.expectedRemoteHash === undefined,
                pending.expectedRemoteHash,
                pending.targetHash,
            )) {
                throw new Error(`Cannot prove recovery ancestry for pending push of ${pending.path}`);
            }
            break;
        case 'pull':
            if (!allowed(
                remoteHash,
                pending.expectedRemoteHash === undefined,
                pending.expectedRemoteHash,
                pending.targetHash,
            ) || !allowed(
                localHash,
                pending.expectedLocalHash === undefined,
                pending.expectedLocalHash,
                pending.targetHash,
            )) {
                throw new Error(`Cannot prove recovery ancestry for pending pull of ${pending.path}`);
            }
            break;
        case 'delete-local':
        case 'delete-remote':
            if (!allowed(localHash, true, pending.expectedLocalHash)
                || !allowed(remoteHash, true, pending.expectedRemoteHash)) {
                throw new Error(`Cannot prove recovery ancestry for pending deletion of ${pending.path}`);
            }
            break;
        case 'move-local':
        case 'move-remote':
            if (!pending.targetPath
                || !allowed(localHash, true, pending.expectedLocalHash)
                || !allowed(remoteHash, true, pending.expectedRemoteHash)
                || !allowed(targetLocalHash, true, pending.targetHash, pending.expectedLocalHash)
                || !allowed(targetRemoteHash, true, pending.targetHash, pending.expectedRemoteHash)) {
                throw new Error(`Cannot prove recovery ancestry for pending move of ${pending.path}`);
            }
            break;
        case 'converge-local':
            if (!allowed(localHash, false, pending.expectedLocalHash, pending.targetHash)
                || !allowed(remoteHash, true, pending.expectedRemoteHash)) {
                throw new Error(`Cannot prove recovery ancestry for local convergence at ${pending.path}`);
            }
            break;
        case 'resolve-conflict':
            if (!allowed(localHash, true, pending.expectedLocalHash, pending.targetHash)
                || !allowed(remoteHash, true, pending.expectedRemoteHash, pending.targetHash)) {
                throw new Error(`Cannot prove recovery ancestry for conflict resolution at ${pending.path}`);
            }
            break;
    }
}

export function isPathConflicted(path: string, state: ReplicaState): boolean {
    return Object.keys(state.conflicts).some(conflictPath => (
        path === conflictPath
        || path.startsWith(`${conflictPath}/`)
        || conflictPath.startsWith(`${path}/`)
    ));
}

function sameLocalEntity(baseline: BaselineEntry, candidate: ReplicaSnapshot): boolean {
    if (candidate.fileType !== baseline.fileType) {
        return false;
    }
    if (baseline.localIdentity && candidate.localIdentity) {
        return baseline.localIdentity === candidate.localIdentity;
    }
    return baseline.fileType === 'folder'
        ? baseline.treeHash !== undefined && candidate.treeHash === baseline.treeHash
        : candidate.hash === baseline.hash;
}
