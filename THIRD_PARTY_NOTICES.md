# Third-party and provenance notices

This fork is based on `overleaf-workshop/Overleaf-Workshop` tag `v0.15.10`,
commit `72e8f4d4753081fe44c817ef5bb98d2b7c27d2a7`, and remains licensed under
AGPL-3.0 as described in `LICENSE`.

The new text primitive is an independent implementation of the conventional
OT.js-compatible retain/insert/delete model. No proprietary
`overleaf-editor-core` source was copied. Public Overleaf Community Edition
source was consulted for wire-level behavior such as `joinDoc(fromVersion)`,
`applyOtUpdate`, `otUpdateApplied`, `dupIfSource`, `lastV`, and retry timing.

Snapshot-to-operation conversion uses the existing `diff-match-patch`
dependency (Apache-2.0). Packaged dependencies retain their own license files
inside `node_modules` where supplied.
