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

## Remote E2E project

Use the authorized retained project `hershel-safe-sync-e2e` on
`10.10.5.4:8888`. Do not run mutation tests against a manuscript.

The contributed **Run Approved Safe Sync E2E** command and the one-time
`safe-sync-e2e.request.json` global-storage marker are hard-bound to that exact
server and project. They read credentials only through VS Code SecretStorage,
write a redacted `safe-sync-e2e.result.json`, isolate fixtures under a unique
folder, remove those fixtures at the end, and retain the project itself.

The executable remote suite verifies the exact loaded extension build before
any mutation. It then exercises the production Local Replica watcher for
remote-to-local text and binary pulls, local-to-remote text and resource
uploads, overlapping local/remote text edits, dirty-editor/external-writer OT
convergence, late text attachment after both local and remote advance from a
common base, sustained remote/VS Code/AI streams, SVG/PNG classification,
concurrent binary conflict archival and explicit resolution, ignored conflict
artifacts, bidirectional move/rename, two-client OT convergence, exact catch-up
replay, and residue-free cleanup.

The streaming step is not a final-snapshot smoke test. It sends unique
single-key VS Code tokens, independent remote OT operations, and fenced AI
read-edit-commit transactions while all writers overlap. It asserts every
token exactly once by total UTF-16 length, preserves the causal order of each
writer, proves four rapid editor callbacks become one remote operation, proves
the full editor stream uses fewer journal operations than change events, saves
the converged editor, and compares editor, disk, and authoritative remote
bytes.
An acknowledgement timeout from a fresh observer is never accepted as proof
that a remote document is absent; a second project snapshot must confirm the
entity deletion.
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
14. concurrent remote, VS Code, and AI writes with exact retention and
    per-writer order preservation.

## Acceptance gate for a real project

Before enabling writes for a manuscript:

- all automated and remote E2E cases pass;
- VSIX identity is exactly `hershel.overleaf-workshop-safe-sync`;
- original upload extensions and helper bridges remain disabled;
- the first report was produced read-only;
- a human reviewed the report and explicitly selected the baseline.
