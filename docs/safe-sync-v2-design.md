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

A successful remote mutation RPC is not yet proof that Overleaf's realtime
project tree exposes the mutation. The legacy `/entities` REST projection is
not a compare-and-swap authority: it can retain an old path after the
collaboration service has accepted and published a move. Before and after a
move, retain, or delete, Safe Sync therefore reads a fresh short-lived
Socket.IO `joinProject` snapshot—the same structural model used by Overleaf's
collaborative editor—and waits within a bounded window for the expected path,
immutable identity, and tree-hash transition. A genuinely occupied path,
changed tree hash, or observation timeout still fails closed and retains the
candidate.

WAL growth is bounded without truncating the selected file in place. Document
journals checkpoint after 256 records or 4 MiB; the project-state WAL
checkpoints after 32 records or 4 MiB. Compaction first fsyncs a new empty WAL
generation and then atomically publishes the checkpoint that selects it, so a
crash exposes either the complete old generation or the complete checkpoint
and new generation. A durable deletion tombstone removes every exact selected
or historical generation before removing the checkpoint and tombstone.
For pre-generation schema-1 data, a checksummed full checkpoint may be migrated
when its legacy WAL is empty or absent; a shorter non-empty WAL is ambiguous
and still fails closed rather than silently skipping possible newer events.

## Recovery before observation

Checkpoint and WAL recovery, local projection-transaction recovery, Overleaf
catch-up, duplicate detection, and OT replay complete before filesystem or
editor events acquire mutation semantics. The projection recovery runs while
the replica lock is held and before watchers are registered. Events observed
during later recovery are queued as observations and revalidated after the
replica becomes ready.

Every schema-3 document journal also carries a project-relative path binding.
Opening or moving a document durably appends that binding before a local OT
operation can be admitted; the journal rejects a local operation when no binding
exists. Recovery resolves an immutable ID from the current remote tree, replica
baseline, active conflict provenance (including retained remote identities),
durable pending document/AI intents, completed AI receipts, and the journal path. Ignore rules affect
projection only; they cannot erase recovery identity. A legacy WAL with no provable path is archived as a detached
recovery with its last acknowledged base, visible local candidate, revision,
operation IDs, and hashes. Only after that artifact and state record are durable
may its unreachable WAL be retired; unrelated paths become observable without
a project-wide pause. The live-ID absence proof is refreshed from a separate
realtime project snapshot both before preparing the WAL and again before any
detached artifact/state publication, closing the stale-tree race.

## Projection is not mutation

VS Code buffers, local files, and the Overleaf document are projections of the
coordinator state. Every plugin-originated projection records its expected
identity and content hash. Matching watcher events consume that projection
receipt and never re-enter the operation stream as user edits.
Projection receipts use a compact checksummed sidecar snapshot, so their
durability boundary never rewrites the full replica baseline WAL on the live
editor path. A dirty-editor boundary receipt stores the projected OT transition,
not only one final hash. If typing advances while that receipt is being made
durable, the same projection is transformed over the newer editor branch and
its watcher callback remains classifiable as plugin output.

No remote transaction may contain `TextDocument.save()`: durability is owned by
the journal and the disk compare-and-swap, never by VS Code's writer. A dirty
editor may temporarily lead the disk while input is still buffered, but once
every visible branch is journaled the canonical result is projected directly to
both editor and disk.

Every open editor, clean or dirty, receives canonical bytes through the
version-checked editor projection. Remote visibility may not depend on the host
reloading a clean buffer from disk: watcher and reload latency is unbounded
while a burst of revisions keeps rewriting the same file, which loses the
realtime gate below. Because `TextEditor.edit` necessarily marks a saved
document as modified, a projection that dirtied a document the user had saved
saves it again once the projection queue is idle, restoring the saved state the
user chose. That save installs the projection: it is ordered before the disk
compare-and-swap, which then finds its bytes already present and writes nothing.
Reversing the order breaks the repair — a host refuses to save a buffer over a
file that changed behind it, so the document would keep an unsaved marker for
bytes nobody typed. The write is still a plugin projection: its receipt is
recorded before the save starts, so the watcher echo and the queued will-save
callback both classify it as plugin output rather than user input. If the host
refuses the save anyway, the disk compare-and-swap installs the bytes and the
replica still converges. The repair is skipped whenever newer work exists, and
it never touches a buffer the user themselves left unsaved. A document the user
is holding unsaved therefore stays unsaved while OT rebases its visible branch. A user-initiated save is
never blocked on the document scheduler: its journal boundary is queued before
the later did-save and filesystem observations. If VS Code delivers `did-save`
before a delayed or coalesced change callback, the handler rereads the current
editor, admits only the still-unobserved forward transition, and hard-flushes
it before any canonical projection. The saved snapshot is disk evidence,
not a forward editor event: if newer input already exists, an older queued save
can never turn into an OT deletion of that input.

A directory scan is only the last server-acknowledged snapshot. Replica attach
and reconnect therefore use the coordinator's visible document projection,
which also contains journaled-but-unacknowledged operations, after verifying
that its entity identity still matches the scan. The older scan snapshot must
never reset that projection.

Baseline refresh is also a compare-and-swap. Each scan freezes the per-path
baseline heads before reading either endpoint; if a text owner advances one of
those heads while the scan is in flight, refresh preserves the newer head and
uses the scan only for unchanged paths. An older converged snapshot therefore
cannot roll a newer common ancestor back during WAL admission.

Overleaf guards `joinDoc` and `leaveDoc` with one membership epoch per Socket.IO
connection. Safe Sync serializes those RPCs on each socket. An acknowledgement
timeout makes that epoch unknowable: the socket is invalidated and disconnected,
queued membership calls are rejected, and recovery creates a new socket,
rejoins the project, then catches every live WAL up from its durable revision.
The project connection is published as ready only after that catch-up completes
on the new socket generation. It never retries a timed-out membership RPC on
the poisoned connection. A timed-out `joinProject` acknowledgement follows the
same fresh-transport rule because a late response cannot authorize a retry on
that socket generation.

Each document has one serialized task queue for editor saves, filesystem
watchers, remote OT arrivals, AI transactions, reconciliation refreshes, and
timer-driven idle/hard journal boundaries.
Every VS Code change event freezes both its document text and its immediately
preceding observation from the same `TextDocument` instance. VS Code may
coalesce an external file reload and skip version numbers; object identity, not
numeric adjacency, establishes the aggregate event-time transition. Contiguous
human snapshots are coalesced at event ingress before they can backlog the
document queue. Plugin projection snapshots remain explicit boundaries, so a
following keystroke cannot replay projected remote content as a local mutation.
A queued remote task can therefore update the canonical OT head without turning
an older editor event into a destructive snapshot diff.

`onWillSave` publishes a per-document causal fence without blocking VS Code.
Filesystem watcher invalidations observed behind that fence are coalesced and
read once after the frozen save snapshot is journaled. Save work remains a
separate fair scheduler source: causal ordering is enforced by the fence, not by
putting every watcher echo into one FIFO that can delay realtime remote OT.

The input batch remains owned by that document until its journal commit
finishes. Remote OT arrivals update the canonical head without overwriting
buffered input; the later batch is transformed from its event-time ancestor
over that newer head. The batching window is 150 ms idle with a 300 ms hard
durability bound. A hard boundary commits a finite admitted prefix rather than
chasing input forever. If newer input arrived during that commit, the remote
canonical head and buffered suffix are OT-transformed into an editor-only
preview at the boundary; the suffix is not projected to disk until it is
journaled. Explicit consistency boundaries, such as starting an AI read-edit
transaction, enter the document scheduler and flush the admitted prefix first.
When that transformed preview is installed, the remaining buffered suffix is
immediately re-anchored from the current canonical head to the installed
preview. Later keystrokes therefore extend that preview instead of replaying
its remote or already-journaled bytes. If an earlier editor event has entered
the document queue, every later editor snapshot stays behind it; the synchronous
admission path may not invert projection and user-event order. An earlier
contiguous transition that resumes during a durability cut may be prepended to
the later in-memory suffix, preserving event order without declaring a conflict.

Canonical projection is single-flight and cannot cross queued editor
observations. Before a VS Code projection is applied, the visible editor text
and document version are re-read. The plugin then applies a minimal
`TextOperation`-derived edit with that version as a compare-and-swap boundary;
a version miss imports the newer user transition instead of overwriting it.
Disk projection uses a crash-recoverable filesystem compare-and-swap, not a
separate read followed by `writeFile`. The target is atomically displaced into
a durable same-filesystem transaction, verified against the last observed
hash, and the staged projection is installed with no-replace hard-link
semantics. A filesystem writer that reaches the pathname at any point wins the
CAS and is imported as a new local operation instead of being overwritten.
A writer may instead open the newly installed hard-link and mutate that same
inode before final verification. In that case the CAS returns the observed
bytes only together with proof that the opened inode, staged target, and current
pathname have one device/inode identity. The installed target is then the
event-time ancestor of that write. Without all three identity checks, the
coordinator must not infer ancestry from the failed projection.
A refused disk CAS also rolls the editor projection back under the same
version-checked compare-and-swap, so a projection is applied to both endpoints
or to neither. A keystroke that arrives during the disk transaction wins that
rollback and becomes the reported local endpoint.
Incomplete transactions restore their displaced candidate before watcher
observation begins. If a committed target prevents restoration and the
displaced bytes no longer match the expected base, recovery writes an active
`unresolved` conflict with base, remote, local, and transaction metadata. The
projection transaction is retained until that conflict fence is durably
indexed, so another crash cannot turn the candidate into an untracked audit
file. Plugin-originated transient delete/create events are only invalidation
hints and cannot retire the document owner.
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

Every resource-level text push durably preallocates its logical operation ID
and content-addressed ancestor/target before OT admission. After admission, the
same pending action records the visible target before awaiting the server ACK.
Recovery inspects that fixed ID: an unknown prepared intent is admitted once
with the same ID, a pending intent is only observed, and a settled intent
advances projection. An accepted ID whose disposition is no longer provable is
isolated; recovery never derives a second logical edit from the snapshot.

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
Every started transport send must also settle, and every durable operation must
have a proved settled disposition before recovery state can be discarded. A
pending or unknown disposition keeps the WAL and refuses the resolution;
closing an observer is never treated as proof that Overleaf rejected its
operation.
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

One workspace process owns the replica lock. A newer workbench may request a
handoff, but the old owner only drains a durability boundary and asks VS Code to
close; it does not stop observers or release the lock first. Normal workbench
disposal performs the final drain and releases ownership. If window closure is
cancelled, the old workbench resumes as the fully observing owner and the newer
workbench remains fenced out.

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
fenced until remote acknowledgement. The accepted journal receipt is persisted
in the replica WAL before it is returned. Retrying the same lease, fencing
token, path, immutable entity, and target after a lost response or extension
restart returns that receipt and cannot submit the OT operation twice.

## Non-text resources

Binary and tree operations use stable resource identity and a common content
hash. One-sided changes propagate. Concurrent distinct binary changes,
delete/edit, divergent moves, and same-path identity collisions preserve all
candidates in `.overleaf/conflicts` and isolate only the affected path.
Binary writeback and file deletion use the same durable local compare-and-swap
as text projection, so a post-scan filesystem writer is retained and forces a
fresh reconciliation rather than being overwritten or deleted.

Moves are compare-and-swap operations too. Remote moves verify the source
immutable entity ID, type, content hash (or authoritative folder-structure
hash), vacant destination, and the same identity/hash after the move. Local
moves revalidate the source inode, tree, and target vacancy before and after
rename. Crash recovery stores those proofs in the pending action.

Remote history and trash live under `.hershel-safe-sync`. Startup only verifies
an existing namespace and never creates a missing one. The first mutation
creates a uniquely named staging folder, uploads and reads back a project-bound
ownership marker, and only then renames it to the final reserved name. A valid
crash-left staging folder can be resumed; unowned or ambiguous final/staging
folders fail closed instead of being hidden. Neither the final name nor staging
names may exist in a local replica.

Remote retention is bounded. Overleaf projects have a hard entity limit, and
every retained transaction spends folders inside it, so retention that only
ever grows ends as a project the server refuses to accept a delete or a
replacement into: exactly the operations retention exists to make recoverable.
Opening a transaction first reclaims the ones outside the retention window —
older than a day and beyond the most recent transactions — and a refusal for
the project's file limit reclaims down to the in-flight horizon and retries
once. Age is what protects correctness here rather than the count: a
transaction another endpoint is still writing into, or that a journaled action
can still be recovered from, is at most minutes old, and a folder this scheme
did not name is never reclaimed. A limit refusal that survives reclamation is
one project-wide condition to act on, not a divergence at whichever path asked
first, so it retries instead of writing a conflict for content that never
diverged.

Every confirmed file baseline is also retained as a content-addressed snapshot
outside the replica tree. An unresolved conflict manifest records the baseline
hash/entity, current remote hash/entity/revision, local candidate hashes, and
the reason, and archives `base/`, `remote/`, and available local candidates.

Folder choices are projected one resource per durable action: **Use local**
replaces the canonical subtree, while **Keep both** publishes the archived
subtree beside the Overleaf version. A crash resumes from the last proven
resource boundary rather than treating a partial directory as converged.

Pending document WALs are enumerated before observation, including legacy
checkpoints. If Overleaf deleted and recreated the same path under a new
document ID, the old acknowledged/visible WAL state, common base, replacement
bytes, IDs, revisions, reason, and pending operation IDs are archived before
the user selects a version. Choosing the replacement cannot silently discard
the old journaled edit.

Baseline refresh treats every pending document ID as a live recovery root. It
cannot garbage-collect the last ID-to-path mapping unless the same ID is already
mapped at its new path. Recursive cleanup and FileSystemProvider deletion retire
each document/resource/folder through the same recovery-fenced transaction;
they never destructively delete a folder that can still contain a pending WAL.

## Failure scope

Project-wide blocking is reserved for an invalid project binding or corrupted
project WAL/checkpoint. Document ancestry and content failures are document
states. Binary and tree conflicts are path states. Other documents and paths
continue to make progress.

An absent document path is therefore not a project failure. If no path can be
proved, the detached archive is the recovery boundary. It stays visible in the
safe-sync report until explicitly acknowledged and then moves to `resolved`.
This is an evidence predicate, not a claim that Overleaf necessarily performed
a delete-and-recreate: detached recovery requires a pending operation and the
absence of every usable explicit, live-tree, baseline, conflict, pending-intent,
completed-receipt, and journal path. A remote replacement alone is never sufficient to enter this
state.

Reserved E2E fixture cleanup is namespace-scoped: an unrelated user conflict
remains auditable and must not prevent cleanup of an isolated test namespace.
Cleanup first fences AI/text writers and rejects any pending descendant WAL;
only a fully settled namespace can be removed.

## Release gate

The release gate includes deterministic seeded OT schedules, WAL crash points,
duplicate and reordered delivery, VS Code projection integration, exclusive AI
lease tests, binary/tree conflict tests, and a retained real Overleaf soak. The
live streaming case drives sustained remote writes, sustained VS Code key
events, sustained AI gateway transactions, and all three concurrently. It
checks exact token retention, each writer's causal order, bounded local
operation batching, and requires a remote token to become visible before the
local writer stops. It then verifies editor/disk/remote byte convergence and
absence of an active conflict. Once writers stop and the network is healthy,
all known operations must converge in five seconds or be represented by a
complete recoverable isolation record.
