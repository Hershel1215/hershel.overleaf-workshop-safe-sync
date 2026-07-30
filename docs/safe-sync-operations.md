# Safe-sync operations

## Install and first use

1. Disable the upstream `iamhyc.overleaf-workshop` extension.
2. Install the VSIX for `hershel.overleaf-workshop-safe-sync`.
3. Add the Overleaf server and log in. Cookies and CSRF data go to VS Code
   SecretStorage. Existing plaintext state from an earlier build of this fork is
   migrated on activation. Reload the window once after the first login so an
   already-open local binding can prefetch its project.
4. Open a local replica. A pre-existing `.overleaf/settings.json` binding is
   detected automatically.
5. Open the cloud status item, choose **Show safe-sync report**, and review all
   local-only, remote-only, and different paths.
6. Choose **Initialize safe-sync baseline** and explicitly trust Overleaf or the
   local replica. The losing side is archived before replacement.

Until step 6, the replica is read-only and no source endpoint is modified.

## Normal status

- Cloud: converged.
- Upload/download icon: one durable action is in progress.
- Shield/attention message: baseline selection, read-only capability, path
  conflicts, or a project-wide pause needs review.

Text edits are normally batched for 350 ms. A binary must remain unchanged for
at least 500 ms before upload or conflict capture.

## Resolve a conflict

Use **Resolve safe-sync conflict** from the source-control status menu, then
choose:

- current Overleaf version;
- latest archived local version; or
- Overleaf at the original path plus a timestamped local sibling.

Artifacts are moved from `unresolved` to `resolved`. Use **Clean up resolved
conflict artifacts** only when they are no longer needed.

Never move conflict artifacts into the normal source tree unless intentionally
restoring them. `.overleaf/**` is hard ignored by synchronization.

## Project pause and recovery

A corrupt state file, lock loss, missing catch-up ancestry, malformed OT update,
or unverified pending action pauses the project. Read the **Hershel Overleaf Safe
Sync** output channel first. Preserve global storage and `.overleaf/conflicts/`.
After correcting the underlying condition, choose **Retry paused safe sync**.

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
