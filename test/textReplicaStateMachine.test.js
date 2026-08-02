const assert = require('assert');
const { TextReplicaStateMachine } = require('../out/sync/textReplicaStateMachine');
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
        const port = {
            commit: async (ancestor, target, barrier) => {
                canonical = canonical === ancestor ? target : merge(ancestor, target, canonical);
                commits.push({ ancestor, target, barrier, result: canonical });
                return { content: canonical, acknowledgement: Promise.resolve(canonical) };
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
        };
    }

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
        assert.deepStrictEqual(state.commits, []);
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
        await machine.onEditorSaved(editorContent);
        const rawContent = `${editorContent}RAW\n`;
        diskActual = rawContent;
        const rawCommit = machine.onDiskObserved(rawContent);
        commitReleases[0]();
        await secondStarted;

        assert.deepStrictEqual(diskProjections, []);
        assert.strictEqual(diskActual, rawContent);
        commitReleases[1]();
        await Promise.all([editorCommit, rawCommit]);
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

    it('does not overwrite disk for a remote-only change behind a dirty editor', async () => {
        const state = harness({editorDirty: true});
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');

        assert.deepStrictEqual(state.diskProjections, []);
        assert.deepStrictEqual(state.convergences, []);
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

    it('imports a raw disk write only when the disk base is still canonical', async () => {
        const state = harness();
        await state.machine.onDiskObserved('aDb');
        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'ab');
        assert.strictEqual(state.commits[0].target, 'aDb');
        assert.deepStrictEqual(state.isolations, []);
        state.machine.dispose();
    });

    it('imports a disk writer against its durable previous canonical base while the editor is dirty', async () => {
        const state = harness({ editorDirty: true });
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');
        state.setDisk('aDb');
        await state.machine.onDiskObserved('aDb');
        assert.strictEqual(state.commits.length, 1);
        assert.strictEqual(state.commits[0].ancestor, 'ab');
        assert.strictEqual(state.commits[0].result, 'aRDb');
        assert.strictEqual(state.isolations.length, 0);
        state.machine.dispose();
    });

    it('isolates a disk overwrite when no previous disk base is known', async () => {
        const state = harness({ editorDirty: true, diskContent: undefined });
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');
        await state.machine.onDiskObserved('aDb');
        assert.strictEqual(state.commits.length, 0);
        assert.strictEqual(state.isolations.length, 1);
        assert.match(state.isolations[0].reason, /provable canonical base/);
        assert.strictEqual(state.machine.isIsolated, true);
        state.machine.dispose();
    });
});
