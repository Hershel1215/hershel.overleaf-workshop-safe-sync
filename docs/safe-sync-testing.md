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

## Remote E2E project

Use the authorized retained project `hershel-safe-sync-e2e` on
`10.10.5.4:8888`. Do not run mutation tests against a manuscript.

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

## Acceptance gate for a real project

Before enabling writes for a manuscript:

- all automated and remote E2E cases pass;
- VSIX identity is exactly `hershel.overleaf-workshop-safe-sync`;
- original upload extensions and helper bridges remain disabled;
- the first report was produced read-only;
- a human reviewed the report and explicitly selected the baseline.
