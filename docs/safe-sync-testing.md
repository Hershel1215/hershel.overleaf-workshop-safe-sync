# Safe-sync test matrix

## Automated suites

Run:

```bash
npm ci --ignore-scripts
npm run test:unit
npm run lint
npm run compile
```

The unit suite covers:

- OT apply, compose, transform, legacy conversion, Unicode, and randomized
  convergence/composition;
- two-client protocol ordering and composed pending edits;
- authoritative ACK completion, out-of-order delivery, catch-up replay,
  ACK-loss duplicate prevention, timeout/failure paths, and strict recovery;
- bounded document/project WAL compaction at both record and 4 MiB limits,
  including crashes before and after generation-selector publication and
  tombstoned deletion of every selected/historical WAL generation;
- migration of a complete legacy checkpoint with an empty/absent WAL, while a
  checkpoint ahead of a shorter non-empty WAL remains a hard recovery error;
- one-sided changes, causal deletes, delete/edit conflicts, remote-first text
  overlap, binary conflict classification, and local edit during ACK;
- stable entity/local identity move planning and folder tree hashes;
- durable replica state, exclusive locks, stale lease takeover, capability
  policy, ignore policy, and conservative doc/resource classification.
- presence of the patched legacy Socket.IO credential headers in both the HTTP
  handshake and WebSocket upgrade paths. `pretest` and `vscode:prepublish`
  apply `patch-package` explicitly, including after `npm ci --ignore-scripts`.
- byte-exact multipart upload framing, including the required filename field
  and `qqfile` binary part when requests are sent through `undici`.
- disk projection races before installation, same-inode writes after
  installation, and pathname replacement after installation; only the
  same-inode case may use the installed target as the raw writer's ancestor.
- project/document membership acknowledgement timeouts invalidate their socket
  generation before retry, while an explicit non-timeout rejection leaves the
  serialized membership queue usable.

## Remote E2E project

Open a retained Local Replica whose project name is `hershel-safe-sync-e2e` or
starts with `hershel-safe-sync-e2e-`. The server may use any host and port. Do
not run mutation tests against a manuscript.

The contributed **Run Bound Safe Sync E2E** command derives the exact server,
project name, and immutable project ID from the current Local Replica binding.
Its one-time `safe-sync-e2e.request.json` marker is hard-bound to those derived
values. It reads credentials only through VS Code SecretStorage, writes a
redacted `safe-sync-e2e.result.json`, isolates fixtures under a unique folder,
removes those fixtures at the end, and retains the project itself. It never
creates, restores, or selects a project by a global hard-coded address.

The executable remote suite verifies the exact loaded extension build before
any mutation. It then exercises the production Local Replica watcher for
remote-to-local text and binary pulls, local-to-remote text and resource
uploads, overlapping local/remote text edits, dirty-editor/external-writer OT
convergence, late text attachment after both local and remote advance from a
common base, a real extension-host reload of one fixed pending operation,
synchronous verification that the pending operation retained its durable path,
pathless legacy-WAL archival without a project pause,
sustained remote/VS Code/raw-filesystem/AI streams, SVG/PNG classification,
concurrent binary conflict archival and explicit resolution, ignored conflict
artifacts, byte-exact local binary moves, bidirectional document move/rename
with a barriered concurrent remote text edit,
two-client OT convergence, exact catch-up
replay, old-document-ID pending-WAL recovery after same-path replacement, and
residue-free cleanup.

The streaming suite first runs remote-only, VS Code-only, and fenced AI-only
continuous writers as separate live E2E steps. Each step must expose its first
edit to another endpoint before the writer stops, then converge the open
editor, disk, joined OT session, and a fresh authoritative Overleaf observer
without conflict or project pause; post-writer convergence is bounded to five
seconds. It then sends unique single-key VS Code tokens, a range replacement, a
range deletion, sustained whole-file raw snapshots, independent remote OT
operations, and fenced AI read-edit-commit transactions while all writers
overlap. It asserts every
retained token occurs exactly once by total UTF-16 length, rejects either old
range, preserves the causal order of each writer, proves four rapid editor
callbacks become fewer remote operations than callbacks, compares editor, disk, and authoritative
remote bytes before the explicit save, and requires exact convergence within
five seconds after all writers stop. The reload step persists the pre-reload
PID, namespace, entity, base/target hashes, revision, and pending operation ID;
the new extension-host PID must prove that same operation settled exactly once
at the next revision before the suite can continue. VS Code Auto Save may clean
the buffer before reload; recovery therefore binds the durable WAL candidate,
path, revision, and operation ID instead of treating the transient UI dirty bit
as ancestry evidence.
The streaming convergence loop reads its already-joined durable OT session;
it does not create a new project socket on every polling interval and thereby
turn the test harness itself into a connection storm.
Fixture creation is also idempotent across ambiguous HTTP failures: before a
failed create is retried, a fresh project snapshot checks whether Overleaf
already committed the named entity. Retries remain bounded and retain the last
server refusal in the failed result; this hardens test provisioning without
relaxing any subsequent convergence assertion.
Before provisioning, the dedicated E2E project also invokes the production
pressure-safe retention policy. It may reclaim only recognized safe-sync
transaction folders outside the protected in-flight window; it cannot delete
an arbitrary project folder or run against a non-E2E project.
The stream also runs with the workspace's real Auto Save policy. A save that
overtakes its editor-change callback must journal the current editor exactly
once, while a stale queued save must not delete newer input; reverting to the
preceding canonical snapshot is an immediate failure, not an acceptable
clean-editor reload.
Watcher invalidations emitted by that save must remain behind its causal fence,
coalesce to one current disk read, and must not build a filesystem queue that
delays acknowledged remote tokens.
After the stress stream, a dedicated same-document scenario establishes two
existing paragraphs from one durable common baseline. The AI gateway begins
from that baseline, VS Code replaces paragraph A, and the stale AI candidate
replaces only paragraph B. The step passes only when both replacements occur
exactly once and the open editor, local disk, fresh authoritative Overleaf
observer, and persisted common baseline all equal the exact composed text
before any explicit save.
The same reload injects a legacy pending WAL whose document ID and path do not
exist in the current replica. The new host must preserve byte-exact base/local
artifacts, retire that WAL only after the archive is durable, report zero
project-pause events, and then remove only the test-owned detached record.
Focused recovery tests also hold exactly one path proof at a time: live-tree
identity (including hard-ignored paths), baseline, active/retained conflict,
pending document action, pending AI intent, completed AI receipt, or schema-3 WAL binding. None may
enter detached recovery; only the pending legacy WAL with every proof absent may
do so. Two race tests make the same immutable ID reappear immediately before and
immediately after WAL preparation; both must leave the WAL attached and publish
no detached artifact.
One deterministic race test captures an empty converged scan, advances the
same document baseline, and then commits the older scan. The newer per-path
baseline must survive while unrelated paths remain refreshable.
An acknowledgement timeout from a fresh observer is never accepted as proof
that a remote document is absent; a second project snapshot must confirm the
entity deletion.
The remote-pull bound starts at the server acknowledgement, because the stress
case deliberately holds the synthetic remote client's incoming ACKs while it
builds a pending-operation backlog. Admission-to-observation time is retained
separately in the evidence, so queue pressure remains visible without calling
time spent before server acceptance a client pull delay.
ACK-loss recovery, reverse server ordering, conflicting tree operations,
duplicate locks, and every journal action boundary are exercised by the
deterministic integration suites.

Required scenarios:

1. two text clients edit overlapping ranges in both server orders;
2. disconnect after send/before ACK, remote edit, reconnect/catch-up;
3. external disk writer and dirty editor buffer overlap;
4. new doc/resource classification, including SVG as resource;
5. stable binary local upload and concurrent binary conflict;
6. remote/local rename, folder move, conflicting move, and delete/edit;
7. duplicate local replica lock rejection;
8. process termination at every journal boundary;
9. `.overleaf/conflicts/**` watcher events never create remote entities;
10. final local bytes, remote bytes, entity types, and hashes converge with no
    unarchived losing variant.
11. sustained remote writes without a local writer;
12. sustained VS Code writes with bounded WAL batching;
13. sustained single-AI gateway writes with lease fencing;
14. concurrent remote, VS Code, raw-filesystem, and AI writes with exact
    retention and per-writer order preservation, including range replacement
    and deletion;
15. unsaved old-ID local WAL, remote delete/recreate at the same path, complete
    recovery manifest, remote resolution, then exact bidirectional sync on the
    replacement ID;
16. a real extension-host reload preserves the same namespace, immutable
    document ID, fixed pending operation ID, exact target, and one-revision ACK.
17. after an explicit user save, two consecutive remote revisions reach disk
    and the open editor within five seconds while the editor remains clean and
    the plugin emits no synthetic save.
18. from one common baseline, a user replaces paragraph A while an AI
    transaction based on that same baseline replaces paragraph B; both edits
    persist exactly once in VS Code, on local disk, and in Overleaf without a
    conflict, text isolation, or project pause.

## Acceptance gate for a real project

Before enabling writes for a manuscript:

- all automated and remote E2E cases pass;
- VSIX identity is exactly `hershel.overleaf-workshop-safe-sync`;
- original upload extensions and helper bridges remain disabled;
- the first report was produced read-only;
- a human reviewed the report and explicitly selected the baseline.
