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
        let diskActual = Object.prototype.hasOwnProperty.call(initial, 'diskContent')
            ? initial.diskContent
            : canonical;
        let machine;
        const port = {
            commit: async (ancestor, target, barrier) => {
                canonical = canonical === ancestor ? target : merge(ancestor, target, canonical);
                commits.push({ ancestor, target, barrier, result: canonical });
                return { content: canonical, acknowledgement: Promise.resolve(canonical) };
            },
            projectEditor: async content => {
                editorProjections.push(content);
                await machine.onEditorChanged(content);
            },
            projectDisk: async content => {
                diskProjections.push(content);
                diskActual = content;
                await machine.onDiskObserved(content);
            },
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
            setDisk(value) { diskActual = value; },
        };
    }

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
        await state.machine.onEditorChanged('EDITOR\nBASE\n');
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
        await state.machine.onEditorChanged('aLb');
        await state.machine.onEditorChanged('aLXb');
        await state.machine.onEditorChanged('aLXYb');
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

    it('commits buffered local input before projecting a concurrent remote edit', async () => {
        const state = harness();
        await state.machine.onEditorChanged('aLb');
        state.setCanonical('aRb');
        await state.machine.onCanonicalChanged('aRb');
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
