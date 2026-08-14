# Safe-sync operations

## Install and first use

1. Disable the upstream `iamhyc.overleaf-workshop` extension.
2. Install the VSIX for `hershel.overleaf-workshop-safe-sync`.
3. Add the Overleaf server and log in. Cookies and CSRF data go to VS Code
   SecretStorage. Existing plaintext state from an earlier build of this fork is
   migrated on activation. Reload the window once after the first login so an
   already-open local binding can prefetch its project.

All commands, views, settings, diagnostics, and the PDF custom-editor type use
the `hershel.overleaf-workshop-safe-sync` namespace. The Activity Bar container
uses the equivalent VS Code-compatible ID `hershel-overleaf-workshop-safe-sync`
because container IDs cannot contain dots. Only the existing
`overleaf-workshop://` URI scheme is retained so old `.overleaf/settings.json`
bindings continue to open; this prevents contribution collisions with an
installed upstream extension.
4. Open a local replica. A pre-existing `.overleaf/settings.json` binding is
   detected automatically.
5. Open the cloud status item, choose **Show safe-sync report**, and review all
   local-only, remote-only, and different paths.
6. Choose **Initialize safe-sync baseline** and explicitly trust Overleaf or the
   local replica. The losing side is archived before replacement.

Until step 6, the replica is read-only and no source endpoint is modified.

`.hershel-safe-sync` and `.hershel-safe-sync.staging-<uuid>` are reserved remote
transaction namespaces. They are never projected locally or uploaded as user
content. If either name already exists at the local replica root, startup stops
before watchers observe it; rename that local item rather than deleting it.

## Normal status

- Cloud: converged.
- Upload/download icon: one durable action is in progress.
- Shield/attention message: baseline selection, read-only capability, path
  conflicts, or a project-wide pause needs review.

Text edits are normally batched for 150 ms with a 300 ms hard cut. A binary must remain unchanged for
at least 500 ms before upload or conflict capture.

## Codex and Claude writes

For a write that may overlap VS Code or Overleaf editing, run **Copy AI Gateway
Command** from the Command Palette and give that command to the AI agent. The
`begin` response supplies the lease ID, fencing token, and exact base content;
the agent returns its candidate with the same CLI using:

```text
... commit <relative-path> <lease-id> <fencing-token> <candidate-file>
```

Only one gateway AI lease may own a document, while human and Overleaf edits
continue through OT. Direct Codex/Claude filesystem writes are still imported
as ordinary on-disk snapshots, but the filesystem does not identify which
process wrote them; therefore the extension cannot reject a second raw AI
writer or preserve an intermediate snapshot overwritten before its watcher
event is observed.

## Resolve a conflict

Use **Resolve safe-sync conflict** from the source-control status menu, then
choose:

- current Overleaf version;
- latest archived local version; or
- Overleaf at the original path plus a timestamped local sibling.

Artifacts are moved from `unresolved` to `resolved`. Use **Clean up resolved
conflict artifacts** only when they are no longer needed.

For folders, **latest archived local version** replaces the whole subtree one
durable resource at a time. **Keep Overleaf version and restore local copy**
publishes the complete archived subtree under a sibling name. For an old
document ID with pending WAL, inspect the manifest's base, acknowledged,
visible local, replacement remote, revisions, and pending operation IDs before
cleaning the archive.

Never move conflict artifacts into the normal source tree unless intentionally
restoring them. `.overleaf/**` is hard ignored by synchronization.

## Project pause and recovery

A corrupt project state file, lock loss, or unverified project action pauses the
project. Missing catch-up ancestry, a malformed OT update, or an invalid OT
invariant isolates only that document with full recovery evidence while other
paths continue. Read the **Hershel Overleaf Safe Sync** output channel first.
Preserve global storage and `.overleaf/conflicts/`. After correcting a
project-wide condition, choose **Retry paused safe sync**.

Do not delete durable state merely to make the warning disappear. If state must
be abandoned, export all local/remote content, retain conflict artifacts, and
perform a new explicit baseline selection.

## Rollback

1. Disable this fork.
2. Keep `.overleaf/conflicts/` and the VSIX.
3. Re-enable the original extension only after confirming no other extension
   host owns the replica lock.
4. The original extension ignores this fork's global state. Do not let it upload
   `.overleaf/conflicts/`; configure that ignore before any legacy sync.
