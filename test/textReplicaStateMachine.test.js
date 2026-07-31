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
                await machine.onDiskObserved(content);
            },
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
            setCanonical(value) { canonical = value; },
        };
    }

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
