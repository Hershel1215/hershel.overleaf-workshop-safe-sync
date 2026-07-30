# Safe-sync coverage audit

This file records release evidence, not implementation intent. A scenario is
marked complete only when a deterministic integration test or the retained
remote E2E project exercises its failure boundary.

## Release candidate

Evidence captured on 2026-07-30 for:

```text
extension       hershel.overleaf-workshop-safe-sync@1.0.0-hershel.19
build id        20260730T223000Z-safe-sync-rc19
runner SHA-256  a892e597448754bd7d86087037af6fb444f3cf40035769e5961753a1fcd06876
VSIX SHA-256    b7b5fb6353adec3fa3b63390714ad0ec1e18999b6234e2d751c91b9ff7040489
unit/integration tests  88 passing
lint            0 errors, 2 pre-existing warnings
compile/package pass
```

The release command is `npm run verify:safe-sync-release`. It applies the
Socket.IO transport patch, runs the unit/integration suite and lint, compiles
the extension and chat view, packages the VSIX, and prints the immutable VSIX
digest.

## Live E2E evidence

The approved retained project is `hershel-safe-sync-e2e` on
`10.10.5.4:8888`. Two consecutive runs of the exact installed `.19` build
passed. The final run was:

```text
nonce       rc19-repeat-20260730T2220Z
started     2026-07-30T22:21:00.531Z
finished    2026-07-30T22:21:37.404Z
steps       17 passed, 0 failed
```

The result records exact extension path/version/build/runner digest before any
remote mutation. It also proves:

- remote text and PNG bytes reached the Local Replica;
- local text, SVG, and PNG reached Overleaf with byte/type checks;
- overlapping local/remote text and dirty-editor/external-disk edits converged;
- distinct local and remote PNG variants were both archived, then explicitly
  resolved to the remote bytes without silently discarding the local variant;
- `.overleaf/conflicts/**` never appeared in the remote project;
- remote and local move/rename retained the same Overleaf entity id;
- two live OT clients produced exactly two revisions, and a fresh observer
  replayed the exact catch-up history to the same hash;
- cleanup proved remote absence, local absence, no pending action, no active
  conflicts, no fixture baseline entries, and no E2E conflict artifacts.

## Required-scenario matrix

| # | Required scenario | Evidence | Status |
|---|---|---|---|
| 1 | Overlapping text in both server orders | Randomized/reverse-order protocol integration plus live Local Replica and two-client OT | Pass |
| 2 | Send before ACK, disconnect, catch-up | Deterministic `DocumentSession` ACK-loss, source-id deduplication, retry, and catch-up tests; live exact catch-up replay | Pass |
| 3 | External disk writer plus dirty editor | Live Local Replica editor/disk merge, archive, remote convergence, and delete | Pass |
| 4 | Document/resource classification including SVG | Policy tests plus live doc, SVG, and PNG type/readback assertions | Pass |
| 5 | Stable binary upload and concurrent binary conflict | Live stable PNG upload plus two-variant archive, explicit resolution, and final byte checks | Pass |
| 6 | Remote/local move, conflicting move, delete/edit | Live bidirectional move/rename with stable entity id; deterministic conflicting move and delete/edit decisions | Pass |
| 7 | Duplicate replica lock | Exclusive-lock and stale-lease takeover tests | Pass |
| 8 | Every journal boundary | Pre/post crash-state matrix for push, pull, both deletes, both moves, local convergence, and conflict resolution | Pass |
| 9 | Conflict artifacts ignored | Live watcher writes under `.overleaf/conflicts` plus repeated remote-tree absence checks | Pass |
| 10 | Final bytes/types/hashes and no unarchived losing variant | Live result evidence plus empty journal/conflict/fixture/artifact audit | Pass |

Scenario 2 uses deterministic connection-failure injection for the exact
send-before-ACK boundary rather than terminating the production socket during
the remote run. The live run independently verifies authoritative two-client
revision ordering and catch-up replay. Scenario 8 likewise injects every
durable pre/post state rather than killing the extension host eight times; the
installed build was separately restarted repeatedly during E2E iteration and
recovered retained pending/conflict state before proceeding.

## Final environment audit

- only `hershel.overleaf-workshop-safe-sync@1.0.0-hershel.19` is installed as
  an Overleaf sync extension; the upstream upload extension is absent;
- `overleafMcpHelper.autoStart` is `false`, with no helper process or listener
  on ports 23119/23120;
- the retained E2E replica state has no pending action, active conflict, or
  `hershel-e2e-*` baseline entry;
- the local replica contains no `hershel-e2e-*` namespace or E2E conflict
  artifact after the final run.
