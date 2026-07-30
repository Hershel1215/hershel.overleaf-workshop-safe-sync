# Safe-sync design

## Safety invariants

1. Timestamps never select a content winner.
2. Every text write is derived from a proven common ancestor and is complete
   only after an authoritative server acknowledgement.
3. At most one unacknowledged text operation and one composed pending operation
   exist per document and client.
4. Replica tree or binary mutations are journaled and fsynced before execution.
5. A path with an unresolved conflict is not uploaded. Protocol, state, or
   ancestry failures pause the whole project.
6. `.overleaf/**` is a local metadata namespace and cannot be uploaded even if
   user ignore settings are empty.
7. No initial common baseline means preview-only operation and zero source-tree
   or remote mutation until explicit selection.

## Text protocol

`TextOperation` implements OT.js-compatible UTF-16 retain/insert/delete,
composition, and two-way transform. Snapshot writers are supported by diffing
their previous visible content to the new snapshot. Remote operations already
assigned a server revision receive ordering priority.

`DocumentSession` persists the acknowledged snapshot, visible snapshot,
revision, outstanding operation, composed pending operation, previously used
socket source IDs, and last submitted revision. Persistence happens before a
network send. A completion promise resolves only after `otUpdateApplied`.

After reconnect, the client requests `joinDoc(documentId, savedRevision, ...)`.
It replays the exact returned update sequence, identifies a previously accepted
operation through its durable `meta.source`, and verifies that replay reproduces
the full joined snapshot. If catch-up history is unavailable, recovery is only
allowed when the operation is provably unapplied or the exact desired snapshot
is already authoritative. All other cases pause instead of attempting a fuzzy
three-way merge.

## Local-replica reconciliation

The replica baseline is stored outside the project in VS Code global storage.
Each entry records remote entity ID/type/hash, base text for documents, folder
tree hash, and local filesystem identity. A full debounced scan overlays a dirty
VS Code text buffer only when it is the sole local change. If both the editor
buffer and the disk changed, their text operations are transformed from the
baseline; all pre-merge variants are retained under the resolved conflict area.

One mutation is active at a time. Its journal contains action, source and target
paths, expected endpoint hashes, target hash, and entity ID. After restart,
observed states must be an allowed pre- or post-state. An unrelated state pauses
the project.

Text/text overlap is merged with remote-first OT and sent as a new operation
against the scanned remote ancestor. If more local text arrives before local
write-back, that delta is transformed again behind the acknowledged result.

For resources, a local upload requires stable bytes, unchanged remote entity ID
and hash, and post-upload read-back. Concurrent binary content is archived; the
remote bytes are written locally and the path stays paused for manual choice.

## Tree operations and conflicts

Remote rename/move detection uses stable entity IDs. Local rename detection uses
persisted device/inode identity, with a unique content/tree hash fallback for
older state. Conflicting destinations and delete/edit overlaps create an
unresolved record and pause the path or subtree.

Artifacts live at:

```text
.overleaf/conflicts/
  unresolved/<timestamp>-<uuid>/
  resolved/<timestamp>-<uuid>/
```

Resolution can select current Overleaf, the latest archived local variant, or
keep both. Resolution moves artifacts to `resolved`; cleanup is explicit only.

## Capability and scope

The current deployment `10.10.5.4:8888` is in the validated writable policy.
Fallback/invisible connections and unadvertised servers are read-only. A future
server can opt in with project feature `hershelSafeSyncProtocol=legacy-ot-v1`.
Documents reporting another OT type are rejected.

Presence, chat, comments, compilation, PDF preview, and other collaboration
features retain their upstream paths. This rewrite changes content consistency,
local replica state, credential storage, and related UI only.
