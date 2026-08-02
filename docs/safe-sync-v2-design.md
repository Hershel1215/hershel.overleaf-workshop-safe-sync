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
may temporarily lead the disk while input is still buffered, but once every
visible branch is journaled the canonical result is projected directly to both
editor and disk without changing VS Code's saved/dirty UI semantics.

A directory scan is only the last server-acknowledged snapshot. Replica attach
and reconnect therefore use the coordinator's visible document projection,
which also contains journaled-but-unacknowledged operations, after verifying
that its entity identity still matches the scan. The older scan snapshot must
never reset that projection.

Each document has one serialized task queue for editor saves, filesystem
watchers, remote OT arrivals, AI transactions, and reconciliation refreshes.
Every VS Code change event freezes both its document text and its immediately
preceding observation from the same `TextDocument` instance. VS Code may
coalesce an external file reload and skip version numbers; object identity, not
numeric adjacency, establishes the aggregate event-time transition. Only
contiguous transitions are composed into one input batch. A queued remote task
can therefore update the canonical OT head without turning an older editor
event into a destructive snapshot diff.

The input batch remains owned by that document until its journal commit
finishes. Remote OT arrivals update the canonical head but neither project over
nor prematurely flush buffered input; the later batch is transformed from its
event-time ancestor over that newer head. The batching window is 350 ms idle
with a 750 ms hard durability bound. Explicit consistency boundaries, such as
starting an AI read-edit transaction, flush first so the reader receives a
journaled snapshot.

Canonical projection is single-flight and cannot cross queued editor
observations. Before a VS Code projection is applied, the visible editor text
and document version are re-read. The plugin then applies a minimal
`TextOperation`-derived edit with that version as a compare-and-swap boundary;
a version miss imports the newer user transition instead of overwriting it.
Before a plugin disk projection is written, the current disk bytes are checked
against the last observed projection; an unexpected value is imported first.
Once a raw disk write is observed, it owns the disk projection until its
journal append completes. No canonical write may replace those bytes while
that durability transition is in flight; failure isolates the captured raw
snapshot.
Once the OT session reports those exact bytes as server-acknowledged and the
clean editor/disk projections match, the resource journal advances them as the
next common baseline before later delete or move planning.
If a scan finds a dirty editor and a distinct disk snapshot with a retained
common base, their two local branches are archived and transformed first; only
the merged local branch is then attached to the remote OT session.

## Text concurrency

Each document follows Overleaf's centralized, versioned OT model: one
outstanding operation, one composed pending operation, ordered remote versions,
and at-least-once transport with source-based duplicate detection. Concurrent
text operations with a proved common base and complete operation history are
transformed and do not create conflicts.

Missing ancestry, an unavailable operation range, a mismatched delete, or an
invalid OT invariant isolates only the affected document.

Late attachment is not a conflict condition. If both the current local text
and current Overleaf text descend from a retained common text baseline with the
same entity identity, the local snapshot delta is submitted from that baseline
and OT-rebased over the current remote projection. A scan-to-attach disk race
retries from fresh bytes. An observation isolation whose archived base, local,
and remote bytes and identities are all identical is automatically retired as
non-divergent; later endpoint changes are then reconciled normally while the
live VFS WAL and remote operation history remain intact.

Resolving an isolated document first retires its old state machine, drains the
one running task and any in-flight journal commit, and rejects queued work.
The VFS session then drains its already-running transition and closes its WAL;
a close failure leaves that journal registered and blocks deletion.
Only then is the selected snapshot restored and archived. The next reconcile
attaches a new state machine; an isolated instance is never reused after a
resolution. Concurrent attempts to isolate the same path share one artifact
creation task, so one conflict produces one recoverable record.
Restoring a selected text snapshot uses the same version-checked minimal editor
projection as normal OT convergence. A concurrent keystroke fails the
compare-and-swap and leaves the conflict recoverable; it cannot be overwritten
by an unversioned whole-document replacement.
The conflict record remains visible as a path fence through baseline refresh,
local-candidate restoration, and durable finalization, so an AI gateway cannot
reattach the document mid-resolution. Artifact relocation is idempotent across
the atomic-rename/state-save crash window, and local tree restoration deletes
only entries whose scanned hashes and directory membership still match.
A missing local path is also not a delete intent on its first observation: the
unchanged remote entity must survive a short second resource scan before the
replica may issue a remote delete. A delayed local projection therefore cannot
erase a newly created Overleaf document.

## AI writer boundary

At most one AI writer may hold a lease for a document. The lease covers
`read -> edit -> commit`, carries a fencing token, and does not block VS Code or
Overleaf collaborators. A second AI writer is rejected immediately. Guaranteed
AI concurrency uses the gateway; uncoordinated filesystem snapshots are a
compatibility input and are isolated when their base cannot be proved.

`begin` is a journal barrier, not a global writer lock: it first commits older
buffered human input, then returns that exact canonical snapshot. During the AI
lease, human and Overleaf operations continue and the AI commit is OT-rebased
from its recorded ancestor. A first request that attaches the document enters
the same scheduler after attachment instead of bypassing it. The lease remains
fenced until remote acknowledgement.

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

Reserved E2E fixture cleanup is namespace-scoped: an unrelated user conflict
remains auditable and must not prevent cleanup of an isolated test namespace.

## Release gate

The release gate includes deterministic seeded OT schedules, WAL crash points,
duplicate and reordered delivery, VS Code projection integration, exclusive AI
lease tests, binary/tree conflict tests, and a retained real Overleaf soak. The
live streaming case drives sustained remote writes, sustained VS Code key
events, sustained AI gateway transactions, and all three concurrently. It
checks exact token retention, each writer's causal order, bounded local
operation batching, editor/disk/remote byte convergence, and absence of an
active conflict. Once writers stop and the network is healthy, all known
operations must converge in five seconds or be represented by a complete
recoverable isolation record.
