const assert = require('assert');
const {createHash} = require('crypto');
const { TextReplicaStateMachine } = require('../out/sync/textReplicaStateMachine');
const { DocumentSessionRetiredError } = require('../out/sync/durableDocumentSession');
const { TextOperation } = require('../out/sync/textOperation');

function merge(base, local, remote) {
    const remoteOperation = TextOperation.fromDiff(base, remote);
    const localOperation = TextOperation.fromDiff(base, local);
    const [, localPrime] = TextOperation.transform(remoteOperation, localOperation);
    return localPrime.apply(remote);
}

describe('text replica projection state machine', () => {
    function harness(initial = {}) {
        let canonical = initial.canonicalContent ?? 'ab';
        const commits = [];
        const editorProjections = [];
        const diskProjections = [];
        const isolations = [];
        const convergences = [];
        let editorActual = Object.prototype.hasOwnProperty.call(initial, 'editorContent')
            ? initial.editorContent
            : canonical;
        let diskActual = Object.prototype.hasOwnProperty.call(initial, 'diskContent')
            ? initial.diskContent
            : canonical;
        let rejectNextEditorProjectionWith;
        let machine;
        // The document session stamps every content state it exposes with a
        // monotonic ordinal. Model it here so a test can publish a projection
        // that was read before a commit and delivered after it.
        let sessionSequence = initial.canonicalSequence ?? 0;
        const port = {
            commit: async (ancestor, target, barrier) => {
                canonical = canonical === ancestor ? target : merge(ancestor, target, canonical);
                sessionSequence += 1;
                commits.push({ ancestor, target, barrier, result: canonical });
                return {
                    content: canonical,
                    acknowledgement: Promise.resolve(canonical),
                    sequence: initial.canonicalSequence === undefined
                        ? undefined
                        : sessionSequence,
                };
            },
            projectEditor: async content => {
                if (rejectNextEditorProjectionWith !== undefined) {
                    editorActual = rejectNextEditorProjectionWith;
                    rejectNextEditorProjectionWith = undefined;
                    throw new Error('VS Code rejected internal projection for main.tex');
                }
                editorProjections.push(content);
                editorActual = content;
                await machine.onEditorChanged(content);
            },
            projectDisk: async content => {
                diskProjections.push(content);
                diskActual = content;
                await machine.onDiskObserved(content);
            },
            readEditor: async () => editorActual,
            readDisk: async () => diskActual,
            onConverged: async content => { convergences.push(content); },
            isolate: async record => { isolations.push(record); },
        };
        machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            canonicalSequence: initial.canonicalSequence,
            editorContent: initial.editorContent ?? canonical,
            editorDirty: initial.editorDirty ?? false,
            diskContent: Object.prototype.hasOwnProperty.call(initial, 'diskContent')
                ? initial.diskContent
                : canonical,
        }, port);
        return {
            machine,
            commits,
            editorProjections,
            diskProjections,
            isolations,
            convergences,
            setCanonical(value) { canonical = value; },
            setEditor(value) { editorActual = value; },
            async editEditor(value) {
                editorActual = value;
                await machine.onEditorChanged(value);
            },
            setDisk(value) { diskActual = value; },
            rejectNextEditorProjection(value) { rejectNextEditorProjectionWith = value; },
            get sessionSequence() { return sessionSequence; },
        };
    }

    it('does not buffer an already-journaled callback ahead of a newer editor batch', async () => {
        const state = harness({canonicalContent: 'a', canonicalSequence: 0});
        try {
            state.setEditor('ab');
            assert(state.machine.tryBufferEditorChange('ab', 'a'));
            await state.machine.flushInput(true);
            state.setEditor('abc');
            assert(state.machine.tryBufferEditorChange('abc', 'ab'));
            // A delayed callback for the first batch follows the next key.
            if (!state.machine.tryBufferEditorChange('ab', 'a')) {
                await state.machine.onEditorChanged('ab', 'a', true);
            }
            await state.machine.flushInput(true);
            assert.strictEqual(state.machine.canonical, 'abc');
            assert.deepStrictEqual(state.isolations, []);
        } finally {
            state.machine.dispose();
        }
    });

    it('still journals a real undo back to the last submitted editor text', async () => {
        const state = harness({canonicalContent: 'a', canonicalSequence: 0});
        try {
            state.setEditor('ab');
            assert(state.machine.tryBufferEditorChange('ab', 'a'));
            await state.machine.flushInput(true);
            state.setEditor('abc');
            assert(state.machine.tryBufferEditorChange('abc', 'ab'));
            state.setEditor('ab');
            if (!state.machine.tryBufferEditorChange('ab', 'abc')) {
                await state.machine.onEditorChanged('ab', 'abc', true);
            }
            await state.machine.flushInput(true);
            assert.strictEqual(state.machine.canonical, 'ab');
            assert.deepStrictEqual(state.isolations, []);
        } finally {
            state.machine.dispose();
        }
    });

    it('ignores a projection published before a commit it already applied', async () => {
        const state = harness({canonicalContent: 'BASE\n', canonicalSequence: 10});
        // The session state a projection was read from, before the local
        // commit below moved the session past it.
        const staleProjection = state.machine.canonical;
        const staleSequence = state.sessionSequence;

        await state.editEditor('LOCAL\nBASE\n');
        await state.machine.flushInput(true);
        assert.strictEqual(state.commits.length, 1);
        const committed = state.machine.canonical;
        assert(committed.includes('LOCAL\n'));
        assert(state.sessionSequence > staleSequence);

        // The projection was queued behind that commit and is only delivered
        // now. Its bytes predate the local edit; adopting them would project
        // the local text back out of the editor and disk.
        await state.machine.onCanonicalChanged(staleProjection, 1, staleSequence);

        assert.strictEqual(state.machine.canonical, committed);
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('adopts a projection published after the commit it follows', async () => {
        const state = harness({canonicalContent: 'BASE\n', canonicalSequence: 10});
        await state.editEditor('LOCAL\nBASE\n');
        await state.machine.flushInput(true);
        const committed = state.machine.canonical;

        const remote = `REMOTE\n${committed}`;
        state.setCanonical(remote);
        await state.machine.onCanonicalChanged(remote, 2, state.sessionSequence + 1);

        assert.strictEqual(state.machine.canonical, remote);
        assert.strictEqual(state.editorProjections.at(-1), remote);
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('still projects the session state it was attached on', async () => {
        // Attaching seeds canonical from a session read and then republishes
        // that same state to drive the first projection. Refusing an ordinal
        // merely because it is not newer would leave the document unprojected.
        const state = harness({
            canonicalContent: 'BASE\n',
            canonicalSequence: 10,
            editorContent: '',
            diskContent: '',
        });
        await state.machine.onCanonicalChanged('BASE\n', 1, 10);

        assert.strictEqual(state.machine.canonical, 'BASE\n');
        assert.strictEqual(state.editorProjections.at(-1), 'BASE\n');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('adopts a projection whose session state cannot be named', async () => {
        // A caller that cannot supply an ordinal — a recovery read, or a build
        // whose session predates the stamp — must still be able to advance the
        // machine, or a document would freeze at its attach-time content.
        const state = harness({canonicalContent: 'BASE\n', canonicalSequence: 10});
        state.setCanonical('REMOTE\nBASE\n');
        await state.machine.onCanonicalChanged('REMOTE\nBASE\n');

        assert.strictEqual(state.machine.canonical, 'REMOTE\nBASE\n');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('hands a first-sight filesystem snapshot back to the resource layer', async () => {
        // A remote-only document: no local file and no durable base, so this
        // actor holds no disk history. A resource action — applying a remote
        // move — then materializes the path with the pre-edit bytes. The actor
        // cannot name an ancestor for them, but neither has it proof of a
        // fork, so it must escalate rather than archive a conflict.
        const state = harness({canonicalContent: 'REMOTE EDIT\n', diskContent: undefined});
        await assert.rejects(
            () => state.machine.onDiskObserved('MOVED BASE\n'),
            /no attributable disk history/,
        );

        assert.deepStrictEqual(state.isolations, []);
        assert.deepStrictEqual(state.commits, []);
        state.machine.dispose();
    });

    it('hands a file that vanished under a projection back to the resource layer', async () => {
        // A remote move arrives as a delete and a create. A projection that
        // re-reads disk between the two sees no file, and isolating there
        // archives a conflict whose local side is the untouched baseline.
        const state = harness({canonicalContent: 'BASE\n'});
        state.setDisk(undefined);
        await assert.rejects(
            () => state.machine.onCanonicalChanged('REMOTE EDIT\n'),
            /disappeared while its text actor was active/,
        );
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('hands a vanished file back to the resource layer', async () => {
        // Applying a remote move deletes the old path and creates the new one.
        // An actor that re-reads disk between the two sees nothing there. The
        // file is not forked, it has moved, and only the resource scan holds
        // the entity identity that says so.
        const state = harness({canonicalContent: 'REMOTE EDIT\n', diskContent: 'MOVED BASE\n'});
        await assert.rejects(
            () => state.machine.onDiskObserved(undefined),
            /disappeared while its text actor was active/,
        );

        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('journals editor input observed before a remote projection instead of overwriting it', async () => {
        const state = harness({canonicalContent: 'BASE\n'});
        // The document changed, but VS Code has not delivered its change event
        // to the document queue yet.
        state.setEditor('LOCAL\nBASE\n');
        state.setCanonical('REMOTE\nBASE\n');
        await state.machine.onCanonicalChanged('REMOTE\nBASE\n');

        assert.strictEqual(state.commits.length, 1);
        assert(state.machine.canonical.includes('LOCAL\n'));
        assert(state.machine.canonical.includes('REMOTE\n'));
        assert.strictEqual(state.editorProjections.at(-1), state.machine.canonical);
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('rebases a delayed editor event from its event-time ancestor', async () => {
        const state = harness();
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');

        // This observation was emitted from "ab" before the remote task
        // advanced the machine's editor head to "aRb".
        state.setEditor('aLb');
        await state.machine.onEditorChanged('aLb', 'ab');
        await state.machine.flushInput(true);

        assert.strictEqual(state.commits.at(-1).ancestor, 'ab');
        assert.strictEqual(state.commits.at(-1).target, 'aLb');
        assert.strictEqual(state.machine.canonical, 'aRLb');
        assert.strictEqual(state.editorProjections.at(-1), 'aRLb');
        state.machine.dispose();
    });

    it('does not project across a queued editor observation', async () => {
        let canonical = 'BASE\n';
        let editorActual = 'LOCAL\nBASE\n';
        let pendingEditorObservations = true;
        const projections = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: 'BASE\n',
            diskContent: 'BASE\n',
            editorDirty: true,
        }, {
            commit: async (ancestor, target) => {
                canonical = merge(ancestor, target, canonical);
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            readEditor: async () => editorActual,
            projectEditor: async content => {
                projections.push(content);
                editorActual = content;
            },
            projectDisk: async () => {},
            hasPendingEditorObservations: () => pendingEditorObservations,
            isolate: async () => {},
        });

        canonical = 'REMOTE\nBASE\n';
        await machine.onCanonicalChanged(canonical);
        assert.deepStrictEqual(projections, []);
        pendingEditorObservations = false;
        await machine.onEditorChanged(editorActual, 'BASE\n');
        await machine.flushInput(true);

        assert(machine.canonical.includes('LOCAL\n'));
        assert(machine.canonical.includes('REMOTE\n'));
        assert.strictEqual(editorActual, machine.canonical);
        machine.dispose();
    });

    it('isolates a mutation whose event-time ancestor is unavailable', async () => {
        const state = harness();
        state.setEditor('unproved edit');
        await state.machine.onEditorChanged('unproved edit', undefined, true);

        assert.strictEqual(state.machine.isIsolated, true);
        assert.match(state.isolations[0].reason, /event-time ancestor/);
        assert.strictEqual(state.isolations[0].editorContent, 'unproved edit');
        assert.deepStrictEqual(state.commits, []);
        state.machine.dispose();
    });

    it('does not import a cross-window save twice after its editor reload was journaled', async () => {
        const state = harness({canonicalContent: 'BASE\n'});
        state.setEditor('LOCAL\nBASE\n');
        await state.machine.onEditorChanged('LOCAL\nBASE\n', 'BASE\n', true);
        await state.machine.onDiskObserved('LOCAL\nBASE\n');

        assert.strictEqual(state.commits.length, 1);
        assert.deepStrictEqual(state.commits[0], {
            ancestor: 'BASE\n',
            target: 'LOCAL\nBASE\n',
            barrier: true,
            result: 'LOCAL\nBASE\n',
        });
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('journals saved bytes when save overtakes the editor change callback', async () => {
        const state = harness();
        state.setEditor('aLb');
        await state.machine.beforeEditorSave('aLb');
        state.setDisk('aLb');

        await state.machine.onEditorSaved('aLb');

        assert.deepStrictEqual(state.commits, [{
            ancestor: 'ab',
            target: 'aLb',
            barrier: true,
            result: 'aLb',
        }]);
        assert.strictEqual(state.machine.canonical, 'aLb');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('OT-rebases a saved callback that overtakes both its change event and a remote edit', async () => {
        const state = harness();
        state.setCanonical('aRb');
        state.setEditor('aLb');
        await state.machine.beforeEditorSave('aLb');
        state.setDisk('aLb');

        await state.machine.onEditorSaved('aLb');

        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'ab');
        assert.strictEqual(state.commits[0].target, 'aLb');
        assert.strictEqual(state.commits[0].result, 'aRLb');
        assert.strictEqual(state.machine.canonical, 'aRLb');
        assert.strictEqual(state.editorProjections.at(-1), 'aRLb');
        assert.strictEqual(state.diskProjections.at(-1), 'aRLb');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('does not turn a stale save callback into deletion of newer editor input', async () => {
        const state = harness();
        await state.machine.beforeEditorSave('ab');
        state.setEditor('abX');
        assert.strictEqual(
            state.machine.tryBufferEditorChange('abX', 'ab', true),
            true,
        );
        state.setDisk('ab');

        await state.machine.onEditorSaved('ab');

        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'ab');
        assert.strictEqual(state.commits[0].target, 'abX');
        assert.strictEqual(state.machine.canonical, 'abX');
        assert(!state.commits.some(commit => commit.target === 'ab'));
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('uses a coalesced save snapshot as the base of a following raw writer', async () => {
        const state = harness();
        state.setEditor('aLb');
        assert.strictEqual(
            state.machine.tryBufferEditorChange('aLb', 'ab', true),
            true,
        );
        await state.machine.beforeEditorSave('aLb');
        assert.strictEqual(state.machine.canonical, 'aLb');

        state.setEditor('aLXYb');
        assert.strictEqual(
            state.machine.tryBufferEditorChange('aLXYb', 'aLb', true),
            true,
        );
        // The watcher missed the exact save echo and first observes a raw
        // append derived from that saved inode.
        state.setDisk('aLbR');
        await state.machine.onDiskObserved('aLbR');

        assert.strictEqual(state.commits.length, 3);
        assert.deepStrictEqual(state.commits.map(commit => ({
            ancestor: commit.ancestor,
            target: commit.target,
        })), [
            {ancestor: 'ab', target: 'aLb'},
            {ancestor: 'aLb', target: 'aLXYb'},
            {ancestor: 'aLb', target: 'aLbR'},
        ]);
        assert.strictEqual(state.machine.canonical, 'aLXYbR');
        assert.strictEqual(state.machine.canonical.indexOf('L'), 1);
        assert.strictEqual(state.machine.canonical.lastIndexOf('L'), 1);
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('does not reuse a skipped save base after a newer disk projection', async () => {
        const state = harness();
        state.setEditor('aLb');
        assert.strictEqual(
            state.machine.tryBufferEditorChange('aLb', 'ab', true),
            true,
        );
        await state.machine.beforeEditorSave('aLb');

        const projected = 'aLbP';
        state.machine.projections.record('disk:main.tex', {
            kind: 'change',
            hash: createHash('sha256').update(projected).digest('hex'),
        });
        state.setDisk(projected);
        await state.machine.onDiskObserved(projected);

        state.setDisk(`${projected}R`);
        await state.machine.onDiskObserved(`${projected}R`);

        assert.strictEqual(state.commits.at(-1).ancestor, projected);
        assert.strictEqual(state.commits.at(-1).target, `${projected}R`);
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('does not isolate a missing-ancestor observation of canonical bytes', async () => {
        const state = harness({
            canonicalContent: 'CANONICAL\n',
            editorContent: 'OLDER EDITOR\n',
            diskContent: 'CANONICAL\n',
        });
        state.setEditor('CANONICAL\n');

        await state.machine.onEditorChanged('CANONICAL\n', undefined, true);

        assert.strictEqual(state.machine.isIsolated, false);
        assert.deepStrictEqual(state.isolations, []);
        assert.deepStrictEqual(state.commits, []);
        state.machine.dispose();
    });

    it('does not lose a second edit that returns a buffered batch to canonical', async () => {
        const state = harness({canonicalContent: 'B'});
        state.setEditor('A');
        await state.machine.onEditorChanged('A', 'B', true);
        state.setEditor('B');
        await state.machine.onEditorChanged('B', 'A', true);
        await state.machine.flushInput(true);

        assert.strictEqual(state.machine.canonical, 'B');
        assert.strictEqual(state.commits.at(-1).target, 'B');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('drops a frozen observation whose transition is already journaled', async () => {
        const state = harness({canonicalContent: 'ab', editorContent: 'ab', diskContent: 'ab'});
        state.setEditor('abc');
        await state.machine.onEditorChanged('abc', 'ab', true);
        await state.machine.flushInput(true);
        assert.strictEqual(state.machine.canonical, 'abc');

        // Newer input is still batched when the coalesced event that already
        // reached canonical is finally delivered from the observation queue.
        state.setEditor('abcd');
        await state.machine.onEditorChanged('abcd', 'abc', true);
        await state.machine.onEditorChanged('abc', 'ab', true);
        await state.machine.flushInput(true);

        assert.strictEqual(state.machine.canonical, 'abcd');
        assert.strictEqual(state.commits.at(-1).ancestor, 'abc');
        assert.strictEqual(state.commits.at(-1).target, 'abcd');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('keeps projecting remote revisions after a clean cross-window save reload', async () => {
        const state = harness({canonicalContent: 'BASE\n'});
        state.setEditor('EXTERNAL\n');
        await state.machine.onEditorChanged('EXTERNAL\n', 'BASE\n', true, false);
        state.setDisk('EXTERNAL\n');
        await state.machine.onDiskObserved('EXTERNAL\n');

        state.setCanonical('REMOTE-1\nEXTERNAL\n');
        await state.machine.onCanonicalChanged('REMOTE-1\nEXTERNAL\n');
        state.setCanonical('REMOTE-2\nREMOTE-1\nEXTERNAL\n');
        await state.machine.onCanonicalChanged('REMOTE-2\nREMOTE-1\nEXTERNAL\n');

        assert.strictEqual(
            state.diskProjections.at(-1),
            'REMOTE-2\nREMOTE-1\nEXTERNAL\n',
        );
        assert.strictEqual(
            state.editorProjections.at(-1),
            'REMOTE-2\nREMOTE-1\nEXTERNAL\n',
        );
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    function savedEditorHarness(editorDirty) {
        const state = {
            canonical: 'BASE\n',
            editor: 'BASE\n',
            disk: 'BASE\n',
            editorDirty,
            editorProjections: [],
            diskProjections: [],
            saveRestores: [],
            pendingEditorObservations: false,
        };
        state.machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: state.canonical,
            editorContent: state.editor,
            editorDirty,
            diskContent: state.disk,
        }, {
            commit: async (_ancestor, target) => ({
                content: target,
                acknowledgement: Promise.resolve(target),
            }),
            readEditor: async () => state.editor,
            projectEditor: async (target, expected) => {
                assert.strictEqual(state.editor, expected);
                state.editor = target;
                state.editorDirty = true;
                state.editorProjections.push(target);
                // A host reports the edit this projection just made as a
                // document change, so the observation queue is non-empty from
                // here until the document task that drains it runs.
                if (state.observeProjectedEditor) {
                    state.pendingEditorObservations = true;
                }
                return true;
            },
            readDisk: async () => state.disk,
            projectDisk: async (target, expected) => {
                assert.strictEqual(state.disk, expected);
                state.disk = target;
                state.diskProjections.push(target);
                return true;
            },
            restoreSavedEditor: async expected => {
                assert.strictEqual(state.editor, expected);
                state.saveRestores.push(expected);
                if (state.refuseSaveRestore) {
                    return false;
                }
                // A host save writes the file. The receipt recorded before it
                // started is what classifies that write as this projection.
                state.disk = expected;
                state.editorDirty = false;
                return true;
            },
            hasPendingEditorObservations: () => state.pendingEditorObservations,
            isolate: async record => assert.fail(record.reason),
        });
        return state;
    }

    it('holds the disk write while the buffer it dirtied still owes a save', async () => {
        const state = savedEditorHarness(false);
        const target = 'REMOTE\nBASE\n';
        state.observeProjectedEditor = true;

        state.canonical = target;
        await state.machine.onCanonicalChanged(target);

        // The buffer carries the revision and still has to save it. Writing
        // the file now is the change behind a dirty editor that makes the host
        // refuse that save, stranding an unsaved marker on the document.
        assert.strictEqual(state.editor, target);
        assert.strictEqual(state.editorDirty, true);
        assert.strictEqual(state.disk, 'BASE\n');
        assert.deepStrictEqual(state.diskProjections, []);
        assert.deepStrictEqual(state.saveRestores, []);
        assert.strictEqual(state.machine.isSettled, false);

        state.observeProjectedEditor = false;
        state.pendingEditorObservations = false;
        await state.machine.resumeProjection();

        assert.deepStrictEqual(state.saveRestores, [target]);
        assert.strictEqual(state.disk, target);
        assert.strictEqual(state.editorDirty, false);
        assert.deepStrictEqual(state.diskProjections, []);
        assert.strictEqual(state.machine.isSettled, true);
        state.machine.dispose();
    });

    it('makes consecutive remote revisions visible without waiting for a reload', async () => {
        const state = savedEditorHarness(false);
        const targets = ['REMOTE-1\nBASE\n', 'REMOTE-2\nREMOTE-1\nBASE\n'];

        for (const target of targets) {
            state.canonical = target;
            await state.machine.onCanonicalChanged(target);

            // Visibility is proven by the buffer itself, not by a filesystem
            // reload the host may deliver seconds later.
            assert.strictEqual(state.editor, target);
            assert.strictEqual(state.disk, target);
            // A remote revision may not leave the user holding an unsaved
            // document they never edited.
            assert.strictEqual(state.editorDirty, false);
            assert.strictEqual(state.machine.isSettled, true);
        }

        assert.deepStrictEqual(state.editorProjections, targets);
        assert.deepStrictEqual(state.saveRestores, targets);
        // The save that cleared the marker is the write. Installing the same
        // bytes behind that buffer is the dirty write a host refuses to save
        // over, which would leave the marker on the document forever.
        assert.deepStrictEqual(state.diskProjections, []);
        state.machine.dispose();
    });

    it('never saves a buffer the user left unsaved before the projection', async () => {
        const state = savedEditorHarness(true);

        state.canonical = 'REMOTE-1\nBASE\n';
        await state.machine.onCanonicalChanged('REMOTE-1\nBASE\n');

        assert.strictEqual(state.editor, 'REMOTE-1\nBASE\n');
        assert.strictEqual(state.disk, 'REMOTE-1\nBASE\n');
        assert.strictEqual(state.editorDirty, true);
        assert.deepStrictEqual(state.saveRestores, []);
        assert.deepStrictEqual(state.diskProjections, ['REMOTE-1\nBASE\n']);
        state.machine.dispose();
    });

    it('projects to disk itself when the host refuses the restoring save', async () => {
        const state = savedEditorHarness(false);
        state.refuseSaveRestore = true;

        state.canonical = 'REMOTE-1\nBASE\n';
        await state.machine.onCanonicalChanged('REMOTE-1\nBASE\n');

        assert.deepStrictEqual(state.saveRestores, ['REMOTE-1\nBASE\n']);
        assert.deepStrictEqual(state.diskProjections, ['REMOTE-1\nBASE\n']);
        assert.strictEqual(state.disk, 'REMOTE-1\nBASE\n');
        // A marker this replica cannot clear may not also stop it converging.
        assert.strictEqual(state.machine.isSettled, true);
        state.machine.dispose();
    });

    it('clears logical dirtiness when Revert File returns to canonical bytes', async () => {
        const state = harness({
            canonicalContent: 'BASE\n',
            editorContent: 'UNSAVED\n',
            diskContent: 'BASE\n',
            editorDirty: true,
        });
        state.setEditor('BASE\n');
        await state.machine.onEditorChanged('BASE\n', 'UNSAVED\n', true, false);

        state.setCanonical('REMOTE\nBASE\n');
        await state.machine.onCanonicalChanged('REMOTE\nBASE\n');

        assert.strictEqual(state.diskProjections.at(-1), 'REMOTE\nBASE\n');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('recovers a rejected editor projection when the editor changed during applyEdit', async () => {
        const state = harness({canonicalContent: 'BASE\n'});
        state.rejectNextEditorProjection('LOCAL\nBASE\n');
        state.setCanonical('REMOTE\nBASE\n');
        await state.machine.onCanonicalChanged('REMOTE\nBASE\n');

        assert.strictEqual(state.commits.length, 1);
        assert(state.machine.canonical.includes('LOCAL\n'));
        assert(state.machine.canonical.includes('REMOTE\n'));
        assert.strictEqual(state.editorProjections.at(-1), state.machine.canonical);
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('does not treat the target of an aborted projection as a future plugin edit', async () => {
        let canonical = 'BASE';
        let editor = 'BASE';
        const commits = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: editor,
            diskContent: canonical,
        }, {
            commit: async (ancestor, target, barrier) => {
                canonical = merge(ancestor, target, canonical);
                commits.push({ancestor, target, barrier});
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            readEditor: async () => editor,
            projectEditor: async () => false,
            projectDisk: async () => {},
            persistProjectionReceipts: async () => {},
            isolate: async record => assert.fail(record.reason),
        });

        canonical = 'REMOTE-ONE';
        await machine.onCanonicalChanged(canonical);
        assert.strictEqual(machine.isExpectedEditorProjection('REMOTE-ONE'), false);
        canonical = 'REMOTE-TWO';
        await machine.onCanonicalChanged(canonical);
        editor = 'REMOTE-ONE';
        assert.strictEqual(machine.tryBufferEditorChange(editor, 'BASE', true), true);
        await machine.flushInput(true);
        assert.strictEqual(commits.length, 1);
        machine.dispose();
    });

    it('isolates a genuine editor projection failure with all recoverable snapshots', async () => {
        const isolations = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE\n',
            editorContent: 'BASE\n',
            diskContent: 'BASE\n',
        }, {
            commit: async (_ancestor, target) => ({
                content: target,
                acknowledgement: Promise.resolve(target),
            }),
            readEditor: async () => 'BASE\n',
            projectEditor: async () => {
                throw new Error('genuine applyEdit failure');
            },
            projectDisk: async () => {},
            isolate: async record => { isolations.push(record); },
        });

        await assert.rejects(
            () => machine.onCanonicalChanged('REMOTE\nBASE\n'),
            /genuine applyEdit failure/,
        );
        assert.deepStrictEqual(isolations, [{
            path: 'main.tex',
            reason: 'genuine applyEdit failure',
            canonicalContent: 'REMOTE\nBASE\n',
            editorContent: 'BASE\n',
            diskContent: 'BASE\n',
        }]);
        assert.strictEqual(machine.isIsolated, true);
        machine.dispose();
    });

    it('serializes overlapping canonical projections and applies the newest target last', async () => {
        let editorActual = 'BASE\n';
        let activeProjections = 0;
        let maximumActiveProjections = 0;
        let releaseFirst;
        let announceFirst;
        const firstStarted = new Promise(resolve => { announceFirst = resolve; });
        const firstGate = new Promise(resolve => { releaseFirst = resolve; });
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE\n',
            editorContent: 'BASE\n',
            diskContent: 'BASE\n',
            editorDirty: true,
        }, {
            commit: async (_ancestor, target) => ({
                content: target,
                acknowledgement: Promise.resolve(target),
            }),
            readEditor: async () => editorActual,
            projectEditor: async content => {
                activeProjections++;
                maximumActiveProjections = Math.max(maximumActiveProjections, activeProjections);
                if (activeProjections === 1) {
                    announceFirst();
                    await firstGate;
                }
                editorActual = content;
                activeProjections--;
            },
            projectDisk: async () => {},
            isolate: async () => {},
        });

        const first = machine.onCanonicalChanged('REMOTE-1\n');
        await firstStarted;
        const second = machine.onCanonicalChanged('REMOTE-2\n');
        await new Promise(resolve => setImmediate(resolve));
        releaseFirst();
        await Promise.all([first, second]);

        assert.strictEqual(maximumActiveProjections, 1);
        assert.strictEqual(editorActual, 'REMOTE-2\n');
        machine.dispose();
    });

    it('does not project a remote snapshot over input whose journal commit is in flight', async () => {
        let serverContent = 'BASE\n';
        let editorActual = serverContent;
        const projections = [];
        let releaseCommit;
        let announceCommit;
        const commitStarted = new Promise(resolve => { announceCommit = resolve; });
        const commitGate = new Promise(resolve => { releaseCommit = resolve; });
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: serverContent,
            editorContent: serverContent,
            diskContent: serverContent,
            editorDirty: true,
        }, {
            commit: async (ancestor, target) => {
                announceCommit();
                await commitGate;
                serverContent = serverContent === ancestor
                    ? target
                    : merge(ancestor, target, serverContent);
                return {
                    content: serverContent,
                    acknowledgement: Promise.resolve(serverContent),
                };
            },
            readEditor: async () => editorActual,
            projectEditor: async content => {
                projections.push(content);
                editorActual = content;
            },
            projectDisk: async () => {},
            isolate: async () => {},
        });
        editorActual = 'LOCAL\nBASE\n';
        await machine.onEditorChanged(editorActual);
        const localCommit = machine.flushInput(true);
        await commitStarted;

        serverContent = 'REMOTE\nBASE\n';
        const remoteArrival = machine.onCanonicalChanged(serverContent);
        await new Promise(resolve => setImmediate(resolve));
        assert.deepStrictEqual(projections, []);
        assert.strictEqual(editorActual, 'LOCAL\nBASE\n');

        releaseCommit();
        await Promise.all([localCommit, remoteArrival]);
        assert(machine.canonical.includes('LOCAL\n'));
        assert(machine.canonical.includes('REMOTE\n'));
        assert.strictEqual(editorActual, machine.canonical);
        machine.dispose();
    });

    it('rechecks buffered input after an asynchronous editor preflight', async () => {
        let serverContent = 'REMOTE\nBASE\n';
        let editorActual = 'BASE\n';
        const projections = [];
        let releaseRead;
        let announceRead;
        const readStarted = new Promise(resolve => { announceRead = resolve; });
        const readGate = new Promise(resolve => { releaseRead = resolve; });
        let releaseCommit;
        let announceCommit;
        const commitStarted = new Promise(resolve => { announceCommit = resolve; });
        const commitGate = new Promise(resolve => { releaseCommit = resolve; });
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE\n',
            editorContent: 'BASE\n',
            diskContent: 'BASE\n',
            editorDirty: true,
        }, {
            commit: async (ancestor, target) => {
                announceCommit();
                await commitGate;
                serverContent = merge(ancestor, target, serverContent);
                return {
                    content: serverContent,
                    acknowledgement: Promise.resolve(serverContent),
                };
            },
            readEditor: async () => {
                announceRead();
                await readGate;
                return editorActual;
            },
            projectEditor: async content => {
                projections.push(content);
                editorActual = content;
            },
            projectDisk: async () => {},
            isolate: async () => {},
        });
        const remoteArrival = machine.onCanonicalChanged(serverContent);
        await readStarted;
        editorActual = 'LOCAL\nBASE\n';
        await machine.onEditorChanged(editorActual);
        releaseRead();
        await commitStarted;
        assert.deepStrictEqual(projections, []);
        releaseCommit();
        await remoteArrival;
        assert(machine.canonical.includes('LOCAL\n'));
        assert(machine.canonical.includes('REMOTE\n'));
        assert.strictEqual(editorActual, machine.canonical);
        machine.dispose();
    });

    it('does not overwrite a raw disk write before its journal commit completes', async () => {
        let serverContent = 'BASE\n';
        let diskActual = serverContent;
        const diskProjections = [];
        const commitReleases = [];
        const commitAnnouncements = [];
        const commitStarted = index => new Promise(resolve => {
            commitAnnouncements[index] = resolve;
        });
        const firstStarted = commitStarted(0);
        const secondStarted = commitStarted(1);
        const commitGate = index => new Promise(resolve => {
            commitReleases[index] = resolve;
        });
        const firstGate = commitGate(0);
        const secondGate = commitGate(1);
        let commitCount = 0;
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: serverContent,
            editorContent: serverContent,
            diskContent: serverContent,
            editorDirty: true,
        }, {
            commit: async (ancestor, target) => {
                const index = commitCount++;
                commitAnnouncements[index]();
                await (index === 0 ? firstGate : secondGate);
                serverContent = serverContent === ancestor
                    ? target
                    : merge(ancestor, target, serverContent);
                return {
                    content: serverContent,
                    acknowledgement: Promise.resolve(serverContent),
                };
            },
            projectEditor: async () => {},
            readDisk: async () => diskActual,
            projectDisk: async content => {
                diskProjections.push(content);
                diskActual = content;
            },
            isolate: async () => {},
        });
        const editorContent = 'LOCAL\nBASE\n';
        await machine.onEditorChanged(editorContent);
        const editorCommit = machine.flushInput(true);
        await firstStarted;
        diskActual = editorContent;
        const editorSave = machine.onEditorSaved(editorContent);
        const rawContent = `${editorContent}RAW\n`;
        diskActual = rawContent;
        const rawCommit = machine.onDiskObserved(rawContent);
        commitReleases[0]();
        await secondStarted;

        assert.deepStrictEqual(diskProjections, []);
        assert.strictEqual(diskActual, rawContent);
        commitReleases[1]();
        await Promise.all([editorCommit, editorSave, rawCommit]);
        assert.strictEqual(diskActual, machine.canonical);
        assert(machine.canonical.includes('RAW\n'));
        machine.dispose();
    });

    it('stops accepting observations after the replica is disposed', async () => {
        const state = harness();
        state.machine.dispose();
        state.setCanonical('REMOTE\n');
        await state.machine.onCanonicalChanged('REMOTE\n');

        assert.deepStrictEqual(state.editorProjections, []);
        assert.deepStrictEqual(state.diskProjections, []);
        assert.deepStrictEqual(state.commits, []);
    });

    it('freezes buffered editor bytes for a resource conflict without pushing them', async () => {
        const state = harness({canonicalContent: 'BASE\n'});
        assert.strictEqual(
            state.machine.tryBufferEditorChange('LOCAL\nBASE\n', 'BASE\n', true),
            true,
        );

        const frozen = await state.machine.freezeForConflict('remote entity was replaced');

        assert.strictEqual(frozen.editorContent, 'LOCAL\nBASE\n');
        assert.strictEqual(frozen.canonicalContent, 'BASE\n');
        assert.strictEqual(state.commits.length, 0);
        assert.strictEqual(state.machine.isIsolated, true);
        state.machine.dispose();
    });

    it('observes an acknowledgement rejected after an active commit is retired', async () => {
        let releaseCommit;
        let announceCommit;
        let rejectAcknowledgement;
        const commitStarted = new Promise(resolve => { announceCommit = resolve; });
        const commitGate = new Promise(resolve => { releaseCommit = resolve; });
        const acknowledgement = new Promise((_, reject) => {
            rejectAcknowledgement = reject;
        });
        const isolations = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE\n',
            editorContent: 'BASE\n',
            diskContent: 'BASE\n',
            editorDirty: false,
        }, {
            commit: async (_ancestor, target) => {
                announceCommit();
                await commitGate;
                return {content: target, acknowledgement};
            },
            projectEditor: async () => {},
            projectDisk: async () => {},
            isolate: async record => { isolations.push(record); },
        });

        await machine.onEditorChanged('LOCAL\nBASE\n', 'BASE\n');
        const flush = machine.flushInput(true);
        await commitStarted;
        const retirement = machine.disposeAndWait();
        releaseCommit();
        await Promise.all([flush, retirement]);
        rejectAcknowledgement(new Error('session closed'));
        await new Promise(resolve => setImmediate(resolve));

        assert.deepStrictEqual(isolations, []);
    });

    it('waits for an in-flight projection before disposal completes', async () => {
        let releaseProjection;
        let announceProjection;
        const projectionStarted = new Promise(resolve => { announceProjection = resolve; });
        const projectionGate = new Promise(resolve => { releaseProjection = resolve; });
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE\n',
            editorContent: 'BASE\n',
            diskContent: 'BASE\n',
            editorDirty: true,
        }, {
            commit: async (_ancestor, target) => ({
                content: target,
                acknowledgement: Promise.resolve(target),
            }),
            readEditor: async () => 'BASE\n',
            projectEditor: async () => {
                announceProjection();
                await projectionGate;
            },
            projectDisk: async () => {},
            isolate: async () => {},
        });
        const projection = machine.onCanonicalChanged('REMOTE\nBASE\n');
        await projectionStarted;
        let retired = false;
        const retirement = machine.disposeAndWait().then(() => { retired = true; });
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(retired, false);
        releaseProjection();
        await Promise.all([projection, retirement]);
        assert.strictEqual(retired, true);
    });

    it('imports a raw disk write before projecting a concurrent remote edit', async () => {
        const state = harness({canonicalContent: 'BASE\n', editorContent: undefined});
        // This models a completed filesystem write whose watcher callback has
        // not reached the document queue yet.
        state.setDisk('LOCAL\nBASE\n');
        state.setCanonical('REMOTE\nBASE\n');
        await state.machine.onCanonicalChanged('REMOTE\nBASE\n');

        assert.strictEqual(state.commits.length, 1);
        assert(state.machine.canonical.includes('LOCAL\n'));
        assert(state.machine.canonical.includes('REMOTE\n'));
        assert(state.diskProjections.at(-1).includes('LOCAL\n'));
        assert(state.diskProjections.at(-1).includes('REMOTE\n'));
        state.machine.dispose();
    });

    it('keeps a journaled edit recoverable when its session retires on disconnect', async () => {
        let rejectAcknowledgement;
        const acknowledgement = new Promise((_resolve, reject) => {
            rejectAcknowledgement = reject;
        });
        const isolations = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE',
            editorContent: 'BASE',
            diskContent: 'BASE',
        }, {
            commit: async (_ancestor, target) => ({content: target, acknowledgement}),
            projectEditor: async () => {},
            projectDisk: async () => {},
            isolate: async record => { isolations.push(record); },
        });

        await machine.onEditorChanged('BASE-LOCAL', 'BASE', true);
        await machine.flushInput(true);
        rejectAcknowledgement(new DocumentSessionRetiredError(
            'Document session disposed before acknowledgement',
        ));
        await new Promise(resolve => setImmediate(resolve));

        assert.strictEqual(machine.isIsolated, false);
        assert.strictEqual(machine.canonical, 'BASE-LOCAL');
        assert.deepStrictEqual(isolations, []);
        machine.dispose();
    });

    it('drains an in-flight journal before freezing isolation evidence', async () => {
        let releaseCommit;
        let commitStarted;
        const commitGate = new Promise(resolve => { releaseCommit = resolve; });
        const started = new Promise(resolve => { commitStarted = resolve; });
        const isolations = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE',
            editorContent: 'BASE',
            diskContent: 'BASE',
        }, {
            commit: async (_ancestor, target) => {
                commitStarted();
                await commitGate;
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
            projectEditor: async () => {},
            projectDisk: async () => {},
            isolate: async record => { isolations.push(record); },
        });

        await machine.onEditorChanged('A', 'BASE', true);
        const flush = machine.flushInput(true);
        await started;
        const isolation = machine.onEditorChanged('B', undefined, true);
        releaseCommit();
        await Promise.all([flush, isolation]);

        assert.strictEqual(isolations.length, 1);
        assert.strictEqual(isolations[0].canonicalContent, 'A');
        assert.strictEqual(isolations[0].editorContent, 'B');
        machine.dispose();
    });

    it('freezes pending input behind an active commit into the latest isolation candidate', async () => {
        let releaseCommit;
        let commitStarted;
        const commitGate = new Promise(resolve => { releaseCommit = resolve; });
        const started = new Promise(resolve => { commitStarted = resolve; });
        const isolations = [];
        const commits = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: 'BASE',
            editorContent: 'BASE',
            diskContent: 'BASE',
        }, {
            commit: async (ancestor, target) => {
                commits.push({ancestor, target});
                commitStarted();
                await commitGate;
                return {content: target, acknowledgement: Promise.resolve(target)};
            },
            projectEditor: async () => {},
            projectDisk: async () => {},
            isolate: async record => { isolations.push(record); },
        });

        await machine.onEditorChanged('A', 'BASE', true);
        const flush = machine.flushInput(true);
        await started;
        await machine.onEditorChanged('AB', 'A', true);
        const isolation = machine.onEditorChanged('ABC', undefined, true);
        releaseCommit();
        await Promise.all([flush, isolation]);

        assert.deepStrictEqual(commits, [{ancestor: 'BASE', target: 'A'}]);
        assert.strictEqual(isolations[0].canonicalContent, 'A');
        assert.strictEqual(isolations[0].editorContent, 'ABC');
        machine.dispose();
    });

    it('journals editor bytes that arrived before their callback during handoff', async () => {
        const state = harness({canonicalContent: 'BASE\n'});
        state.setEditor('LOCAL\nBASE\n');

        await state.machine.prepareForHandoff();

        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'BASE\n');
        assert.strictEqual(state.commits[0].target, 'LOCAL\nBASE\n');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('does not re-import an editor or disk projection as a mutation', async () => {
        const state = harness();
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');
        assert.deepStrictEqual(state.editorProjections, ['aRb']);
        assert.deepStrictEqual(state.diskProjections, ['aRb']);
        assert.deepStrictEqual(state.commits, []);
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('announces convergence only after clean local projections match canonical text', async () => {
        const state = harness();
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');

        assert(state.convergences.length >= 1);
        assert(state.convergences.every(content => content === 'aRb'));
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('projects journaled remote text to disk without requiring an editor save', async () => {
        const state = harness({editorDirty: true});
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');

        assert.deepStrictEqual(state.diskProjections, ['aRb']);
        assert(state.convergences.includes('aRb'));
        state.machine.dispose();
    });

    it('converges dirty editor and raw disk branches through the journal', async () => {
        const state = harness({canonicalContent: 'BASE\n', editorDirty: true});
        await state.editEditor('EDITOR\nBASE\n');
        state.setDisk('BASE\nDISK\n');
        await state.machine.onDiskObserved('BASE\nDISK\n');

        assert.strictEqual(state.commits.length, 2);
        assert(state.machine.canonical.includes('EDITOR\n'));
        assert(state.machine.canonical.includes('DISK\n'));
        assert.strictEqual(state.diskProjections.at(-1), state.machine.canonical);
        assert.strictEqual(state.editorProjections.at(-1), state.machine.canonical);
        state.machine.dispose();
    });

    it('journals continuous editor input as one operation batch', async () => {
        const state = harness();
        await state.editEditor('aLb');
        await state.editEditor('aLXb');
        await state.editEditor('aLXYb');
        await state.machine.flushInput();
        assert.strictEqual(state.commits.length, 1);
        assert.deepStrictEqual(state.commits[0], {
            ancestor: 'ab',
            target: 'aLXYb',
            barrier: true,
            result: 'aLXYb',
        });
        assert.strictEqual(state.machine.canonical, 'aLXYb');
        state.machine.dispose();
    });

    it('rebases a buffered batch without flushing once per remote arrival', async () => {
        const state = harness();
        await state.editEditor('aLb');
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');
        assert.strictEqual(state.commits.length, 0);
        assert.deepStrictEqual(state.editorProjections, []);

        await state.machine.flushInput(true);
        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].barrier, true);
        assert.strictEqual(state.commits[0].result, 'aRLb');
        assert.deepStrictEqual(state.editorProjections, ['aRLb']);
        assert.strictEqual(state.machine.canonical, 'aRLb');
        state.machine.dispose();
    });

    it('projects remote OT at a hard boundary before continuous local input stops', async () => {
        let canonical = 'ab';
        let editorActual = 'ab';
        let releaseFirst;
        let announceFirst;
        const firstStarted = new Promise(resolve => { announceFirst = resolve; });
        const firstGate = new Promise(resolve => { releaseFirst = resolve; });
        const commits = [];
        const projections = [];
        const diskProjections = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: editorActual,
            editorDirty: true,
            diskContent: 'ab',
        }, {
            commit: async (ancestor, target, barrier) => {
                commits.push({ancestor, target, barrier});
                if (commits.length === 1) {
                    announceFirst();
                    await firstGate;
                }
                canonical = merge(ancestor, target, canonical);
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            readEditor: async () => editorActual,
            projectEditor: async content => {
                projections.push(content);
                editorActual = content;
            },
            projectDisk: async content => { diskProjections.push(content); },
            isolate: async record => assert.fail(record.reason),
        });

        editorActual = 'aLb';
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'ab', true), true);
        canonical = 'aRb';
        await machine.onCanonicalChanged(canonical);
        const boundary = machine.flushInput(true);
        await firstStarted;
        editorActual = 'aLXb';
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'aLb', true), true);
        releaseFirst();
        await boundary;

        assert.strictEqual(machine.hasBufferedInput, true);
        assert(editorActual.includes('R'));
        assert(editorActual.includes('L'));
        assert(editorActual.includes('X'));
        assert.strictEqual(projections.at(-1), editorActual);
        assert.strictEqual(commits.length, 1);
        assert.deepStrictEqual(diskProjections, [], 'unjournaled suffix reached disk');

        const projectedHead = editorActual;
        editorActual = `${projectedHead}Y`;
        assert.strictEqual(
            machine.tryBufferEditorChange(editorActual, projectedHead, true),
            true,
            'post-projection input did not continue from the installed preview',
        );

        await machine.flushInput(true);
        assert.strictEqual(machine.hasBufferedInput, false);
        assert(machine.canonical.includes('R'));
        assert(machine.canonical.includes('L'));
        assert(machine.canonical.includes('X'));
        assert(machine.canonical.includes('Y'));
        assert.strictEqual(diskProjections.at(-1), machine.canonical);
        machine.dispose();
    });

    it('records only the frozen save snapshot while newer editor input stays buffered', async () => {
        let canonical = 'ab';
        let editorActual = 'aLb';
        let releaseFirst;
        let announceFirst;
        const firstStarted = new Promise(resolve => { announceFirst = resolve; });
        const firstGate = new Promise(resolve => { releaseFirst = resolve; });
        let commitCount = 0;
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: 'ab',
            editorDirty: true,
            diskContent: 'ab',
        }, {
            commit: async (ancestor, target) => {
                commitCount++;
                if (commitCount === 1) {
                    announceFirst();
                    await firstGate;
                }
                canonical = merge(ancestor, target, canonical);
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            readEditor: async () => editorActual,
            projectEditor: async content => { editorActual = content; },
            projectDisk: async () => {},
            isolate: async record => assert.fail(record.reason),
        });
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'ab', true), true);

        const saving = machine.beforeEditorSave(editorActual);
        await firstStarted;
        assert(!machine.serializeProjectionReceipts().some(receipt => receipt.path === 'disk:main.tex'));
        editorActual = 'aLXb';
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'aLb', true), true);
        releaseFirst();
        await saving;

        assert.strictEqual(machine.hasBufferedInput, true);
        assert(machine.serializeProjectionReceipts().some(receipt => receipt.path === 'disk:main.tex'));
        await machine.beforeEditorSave(editorActual);
        assert.strictEqual(machine.hasBufferedInput, false);
        assert(machine.serializeProjectionReceipts().some(receipt => receipt.path === 'disk:main.tex'));
        machine.dispose();
    });

    it('rebases rather than installing an older boundary target during receipt persistence', async () => {
        let canonical = 'ab';
        let editorActual = 'aLb';
        let releasePersistence;
        let announcePersistence;
        const persistenceStarted = new Promise(resolve => { announcePersistence = resolve; });
        const persistenceGate = new Promise(resolve => { releasePersistence = resolve; });
        const projections = [];
        let persistCount = 0;
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: 'ab',
            editorDirty: true,
            diskContent: 'ab',
        }, {
            commit: async (ancestor, target) => {
                canonical = merge(ancestor, target, canonical);
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            readEditor: async () => editorActual,
            projectEditor: async (content, expected) => {
                projections.push({content, expected});
                editorActual = content;
            },
            projectDisk: async () => {},
            persistProjectionReceipts: async () => {
                persistCount++;
                if (persistCount === 1) {
                    announcePersistence();
                    await persistenceGate;
                }
            },
            isolate: async record => assert.fail(record.reason),
        });
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'ab', true), true);
        canonical = 'aRb';
        await machine.onCanonicalChanged(canonical);

        const boundary = machine.projectBufferedBoundary();
        await persistenceStarted;
        editorActual = 'aLXb';
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'aLb', true), true);
        releasePersistence();
        await boundary;

        assert.deepStrictEqual(projections, [{content: 'aRLXb', expected: 'aLXb'}]);
        assert.strictEqual(editorActual, 'aRLXb');
        assert.strictEqual(machine.hasBufferedInput, true);
        await machine.flushInput(true);
        assert(editorActual.includes('R'));
        assert(editorActual.includes('L'));
        assert(editorActual.includes('X'));
        machine.dispose();
    });

    it('rebases a persisted boundary projection over input arriving during persistence', async () => {
        let canonical = 'ab';
        let editorActual = 'aLb';
        let releasePersistence;
        let announcePersistence;
        const persistenceStarted = new Promise(resolve => { announcePersistence = resolve; });
        const persistenceGate = new Promise(resolve => { releasePersistence = resolve; });
        const projections = [];
        let persistCount = 0;
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: 'ab',
            editorDirty: true,
            diskContent: 'ab',
        }, {
            commit: async (ancestor, target) => {
                canonical = merge(ancestor, target, canonical);
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            readEditor: async () => editorActual,
            projectEditor: async (content, expected) => {
                assert.strictEqual(editorActual, expected);
                projections.push({content, expected});
                editorActual = content;
                return true;
            },
            projectDisk: async () => {},
            persistProjectionReceipts: async receipts => {
                if (
                    ++persistCount === 1
                    && receipts.some(receipt => receipt.mergeBase !== undefined)
                ) {
                    announcePersistence();
                    await persistenceGate;
                }
            },
            isolate: async record => assert.fail(record.reason),
        });
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'ab', true), true);
        canonical = 'aRb';
        await machine.onCanonicalChanged(canonical);

        const boundary = machine.projectBufferedBoundary();
        await persistenceStarted;
        editorActual = 'aLXb';
        assert.strictEqual(machine.tryBufferEditorChange(editorActual, 'aLb', true), true);
        releasePersistence();
        await boundary;

        assert.strictEqual(projections.length, 1);
        assert(editorActual.includes('R'));
        assert(editorActual.includes('L'));
        assert(editorActual.includes('X'));
        await machine.onEditorChanged(editorActual, projections[0].expected, false, true);
        assert.strictEqual(machine.hasBufferedInput, true);
        await machine.flushInput(true);
        assert(machine.canonical.includes('R'));
        assert(machine.canonical.includes('L'));
        assert(machine.canonical.includes('X'));
        machine.dispose();
    });

    it('imports a raw disk write only when the disk base is still canonical', async () => {
        const state = harness();
        await state.machine.onDiskObserved('aDb');
        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'ab');
        assert.strictEqual(state.commits[0].target, 'aDb');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('OT-imports a filesystem writer that wins the disk projection CAS', async () => {
        let canonical = 'BASE\n';
        let disk = 'BASE\n';
        let changedDuringReceipt = false;
        const commits = [];
        const isolations = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: undefined,
            diskContent: disk,
        }, {
            commit: async (ancestor, target, barrier) => {
                canonical = merge(ancestor, target, canonical);
                commits.push({ancestor, target, barrier, result: canonical});
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            projectEditor: async () => {},
            readDisk: async () => disk,
            persistProjectionReceipts: async receipts => {
                if (
                    !changedDuringReceipt
                    && receipts.some(receipt => receipt.path === 'disk:main.tex')
                ) {
                    changedDuringReceipt = true;
                    disk = 'LOCAL\nBASE\n';
                }
            },
            projectDisk: async (target, expected) => {
                if (disk !== expected) {
                    return false;
                }
                disk = target;
                return true;
            },
            isolate: async record => { isolations.push(record); },
        });

        canonical = 'REMOTE\nBASE\n';
        await machine.onCanonicalChanged(canonical);

        assert.strictEqual(commits.length, 1);
        assert.strictEqual(commits[0].ancestor, 'BASE\n');
        assert.strictEqual(commits[0].target, 'LOCAL\nBASE\n');
        assert(canonical.includes('REMOTE\n'));
        assert(canonical.includes('LOCAL\n'));
        assert.strictEqual(disk, canonical);
        assert.deepStrictEqual(isolations, []);
        machine.dispose();
    });

    it('does not resubmit editor bytes changed on the installed projection inode', async () => {
        let canonical = 'BASE';
        let disk = 'BASE';
        const commits = [];
        const isolations = [];
        const machine = new TextReplicaStateMachine({
            path: 'main.tex',
            canonicalContent: canonical,
            editorContent: undefined,
            diskContent: disk,
        }, {
            commit: async (ancestor, target, barrier) => {
                canonical = merge(ancestor, target, canonical);
                commits.push({ancestor, target, barrier, result: canonical});
                return {content: canonical, acknowledgement: Promise.resolve(canonical)};
            },
            projectEditor: async () => {},
            readDisk: async () => disk,
            projectDisk: async target => {
                disk = `${target}+RAW`;
                return {
                    applied: false,
                    actualContent: disk,
                    installedAncestor: target,
                };
            },
            isolate: async record => { isolations.push(record); },
        });

        canonical = 'BASE+EDITOR';
        await machine.onCanonicalChanged(canonical);

        assert.strictEqual(commits.length, 1);
        assert.strictEqual(commits[0].ancestor, 'BASE+EDITOR');
        assert.strictEqual(commits[0].target, 'BASE+EDITOR+RAW');
        assert.strictEqual(canonical, 'BASE+EDITOR+RAW');
        assert.strictEqual(disk, canonical);
        assert.deepStrictEqual(isolations, []);
        machine.dispose();
    });

    it('imports a disk writer against the latest no-save disk projection', async () => {
        const state = harness({ editorDirty: true });
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');
        state.setDisk('aDb');
        await state.machine.onDiskObserved('aDb');
        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'aRb');
        assert.strictEqual(state.commits[0].result, 'aDb');
        assert.strictEqual(state.isolations.length, 0);
        state.machine.dispose();
    });

    it('establishes a disk base by projecting a remote-only document before later writes', async () => {
        const state = harness({ editorDirty: true, diskContent: undefined });
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');
        await state.machine.onDiskObserved('aDb');
        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'aRb');
        assert.strictEqual(state.commits[0].result, 'aDb');
        assert.strictEqual(state.isolations.length, 0);
        state.machine.dispose();
    });

});
