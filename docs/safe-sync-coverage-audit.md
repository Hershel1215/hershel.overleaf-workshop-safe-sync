# Safe-sync coverage audit

This file records release evidence, not implementation intent. A scenario is
marked complete only when a deterministic integration test or the retained
remote E2E project exercises its failure boundary.

## Release candidate

Evidence captured on 2026-08-02 for:

```text
extension       hershel.overleaf-workshop-safe-sync@1.0.0-hershel.60
build id        20260802T045315Z-safe-sync-v2-rc60
runner SHA-256  448c07e60e0d22e992356403ec2139557fcd59fb049628af326287cf0417b776
VSIX SHA-256    bf080ec42ce05aa6d06f721cc484543e097553449fb124dd1a1f84a96968756b
unit/integration tests  201 passing
lint            0 errors, 2 pre-existing warnings
compile/package pass
```

The release command is `npm run verify:safe-sync-release`. It applies the
Socket.IO transport patch, runs the unit/integration suite and lint, compiles
the extension and chat view, packages the VSIX, and prints the immutable VSIX
digest.

## Live E2E evidence

The approved retained project is `hershel-safe-sync-e2e` on
`10.10.5.4:8888`. The exact installed `.60` release build passed its final
review run:

```text
nonce       rc60-review-20260802T0501Z
started     2026-08-02T05:02:46.176Z
finished    2026-08-02T05:03:52.720Z
steps       19 passed, 0 failed
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
- after a forced late detach, independent local-disk and remote edits from the
  same durable text baseline OT-rebased during attachment without a conflict;
- 52 VS Code change events, 8 remote operations, and 6 fenced AI transactions
  ran as overlapping streams; all tokens and each writer's order were retained;
- the 52 local edits were journaled as 10 local operations, while a controlled
  four-edit burst produced exactly one remote operation;
- cleanup proved remote absence, local absence, no pending action, no active
  conflicts, no fixture baseline entries, and no E2E conflict artifacts.

The 2026-08-01 `main.tex` isolation was proved non-divergent because its
archived base, local, and remote hashes and entity identities were identical.
The extension retired that false conflict without discarding the VFS WAL, then
rebased the later local snapshot over remote revisions 151--155. Local bytes,
the authoritative Overleaf document, and the durable baseline converged to
SHA-256
`87e69a44459f17331a4a18cbd74ffea2497403b12d2f6ff5ad5908eaab78fada`;
the observation artifact remains preserved under `.overleaf/conflicts/resolved`.

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
| 11 | Sustained remote writes | Deterministic continuous-writer suite plus live 8-operation remote stream | Pass |
| 12 | Sustained VS Code writes | Deterministic suite plus live 52-event editor stream with 10 durable operations | Pass |
| 13 | Sustained AI writes | Deterministic lease suite plus live 6-transaction fenced AI stream | Pass |
| 14 | Remote/VS Code/AI concurrent writes | Live exact-token, per-writer-order, editor/disk/remote convergence check | Pass |
| 15 | Late text attachment after both endpoints advance | Forced machine detach plus live local-disk/remote OT rebase from one durable baseline | Pass |

Scenario 2 uses deterministic connection-failure injection for the exact
send-before-ACK boundary rather than terminating the production socket during
the remote run. The live run independently verifies authoritative two-client
revision ordering and catch-up replay. Scenario 8 likewise injects every
durable pre/post state rather than killing the extension host eight times; the
installed build was separately restarted repeatedly during E2E iteration and
recovered retained pending/conflict state before proceeding.

## Final environment audit

- only `hershel.overleaf-workshop-safe-sync@1.0.0-hershel.60` is installed as
  an Overleaf sync extension; the upstream upload extension is absent;
- the MCP helper extension is absent, with no helper process or listener on
  ports 23119/23120;
- the retained E2E replica state has no pending action, active conflict, or
  `hershel-e2e-*` baseline entry;
- the local replica contains no `hershel-e2e-*` namespace or E2E conflict
  artifact after the final run.
