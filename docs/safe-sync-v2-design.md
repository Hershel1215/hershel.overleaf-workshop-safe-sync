# Hershel Safe Sync v2

## Contract

Safe Sync v2 uses one durable per-project coordinator and one ordered OT
session per editable Overleaf document. Wall-clock timestamps never select a
winner.

An edit moves through these states:

```
buffered -> journaled -> remote-acknowledged -> converged
```

`journaled` is the durability boundary. Continuous editor input is composed in
memory and committed to the WAL as a batch. A journaled operation may be
transformed, acknowledged, replayed, or placed into recoverable isolation; it
must never be silently discarded.

## Recovery before observation

Checkpoint and WAL recovery, Overleaf catch-up, duplicate detection, and OT
replay complete before filesystem or editor events acquire mutation semantics.
Events observed during recovery are queued as observations and revalidated
after the replica becomes ready.

## Projection is not mutation

VS Code buffers, local files, and the Overleaf document are projections of the
coordinator state. Every plugin-originated projection records its expected
identity and content hash. Matching watcher events consume that projection
receipt and never re-enter the operation stream as user edits.

The remote projection path must not call `TextDocument.save()`. A dirty editor
may temporarily lead the disk projection without weakening the journaled
operation guarantee.

A directory scan is only the last server-acknowledged snapshot. Replica attach
and reconnect therefore use the coordinator's visible document projection,
which also contains journaled-but-unacknowledged operations, after verifying
that its entity identity still matches the scan. The older scan snapshot must
never reset that projection.

## Text concurrency

Each document follows Overleaf's centralized, versioned OT model: one
outstanding operation, one composed pending operation, ordered remote versions,
and at-least-once transport with source-based duplicate detection. Concurrent
text operations with a proved common base and complete operation history are
transformed and do not create conflicts.

Missing ancestry, an unavailable operation range, a mismatched delete, or an
invalid OT invariant isolates only the affected document.

## AI writer boundary

At most one AI writer may hold a lease for a document. The lease covers
`read -> edit -> commit`, carries a fencing token, and does not block VS Code or
Overleaf collaborators. A second AI writer is rejected immediately. Guaranteed
AI concurrency uses the gateway; uncoordinated filesystem snapshots are a
compatibility input and are isolated when their base cannot be proved.

## Non-text resources

Binary and tree operations use stable resource identity and a common content
hash. One-sided changes propagate. Concurrent distinct binary changes,
delete/edit, divergent moves, and same-path identity collisions preserve all
candidates in `.overleaf/conflicts` and isolate only the affected path.

Every confirmed file baseline is also retained as a content-addressed snapshot
outside the replica tree. An unresolved conflict manifest records the baseline
hash/entity, current remote hash/entity/revision, local candidate hashes, and
the reason, and archives `base/`, `remote/`, and available local candidates.

## Failure scope

Project-wide blocking is reserved for an invalid project binding or corrupted
project WAL/checkpoint. Document ancestry and content failures are document
states. Binary and tree conflicts are path states. Other documents and paths
continue to make progress.

## Release gate

The release gate includes deterministic seeded OT schedules, WAL crash points,
duplicate and reordered delivery, VS Code projection integration, exclusive AI
lease tests, binary/tree conflict tests, and a retained real Overleaf soak. Once
writers stop and the network is healthy, all known operations must converge in
five seconds or be represented by a complete recoverable isolation record.
