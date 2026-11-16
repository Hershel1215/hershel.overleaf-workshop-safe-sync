const assert = require('assert');
const { TextReplicaStateMachine } = require('../out/sync/textReplicaStateMachine');
const { DurableDocumentSession } = require('../out/sync/durableDocumentSession');
const { FairTaskScheduler } = require('../out/sync/fairTaskScheduler');

// Regression: an external tool rewrites a document that is open (clean) in
// VS Code. The watcher event commits old -> new through onDiskObserved while
// VS Code reloads the editor with the same bytes. If the reload is buffered as
// a fresh old -> new edit before the disk commit adopts its canonical result,
// the same change is submitted twice and transform(A, A) duplicates every
// insert (seen in the wild as a 917-line appendix becoming 1751 lines).
describe('external write racing a VS Code editor reload', () => {
    const OLD = '\\section{A}\nalpha\n\\section{B}\nbeta\n';
    const NEW = '\\section{B}\nbeta\n\\section{A}\nalpha gamma\n';
    const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

    function harness(walDelayMs, editorDirty) {
        const journal = {
            content: OLD,
            operationDisposition: () => 'unknown',
            async commitLocalOperation(operation, opId) {
                await sleep(walDelayMs);
                this.content = operation.apply(this.content);
                return { id: opId };
            },
        };
        const session = new DurableDocumentSession({
            documentId: 'doc',
            sendUpdate: async () => { },
            getSourceId: () => 'source',
            onChange: () => { },
            onError: () => { },
        }, journal);
        const files = { editor: OLD, disk: NEW };
        const isolations = [];
        const port = {
            commit: (ancestor, target, barrier) => session.acceptSnapshotAgainstAncestor(ancestor, target, barrier),
            projectEditor: async (content, expected) => {
                if (expected === undefined || files.editor === expected) {
                    files.editor = content;
                    return true;
                }
                return false;
            },
            readEditor: async () => files.editor,
            restoreSavedEditor: async () => true,
            projectDisk: async (content, expected) => {
                if (expected === undefined || files.disk === expected) {
                    files.disk = content;
                    return true;
                }
                return { applied: false, actualContent: files.disk, installedAncestor: content };
            },
            readDisk: async () => files.disk,
            hasPendingEditorObservations: () => false,
            persistProjectionReceipts: async () => { },
            onConverged: () => { },
            isolate: async record => { isolations.push(record); },
            scheduleProjectionRetry: () => { },
        };
        const machine = new TextReplicaStateMachine({
            path: 'sec/appendix.tex',
            canonicalContent: OLD,
            canonicalSequence: 0,
            editorContent: OLD,
            editorDirty,
            diskContent: OLD,
        }, port);
        return { journal, files, isolations, machine, scheduler: new FairTaskScheduler() };
    }

    async function race(state, editorContent, editorAtMs, editorDirty) {
        const disk = state.scheduler.run('filesystem', () => state.machine.onDiskObserved(NEW));
        await sleep(editorAtMs);
        state.files.editor = editorContent;
        let editor = Promise.resolve();
        if (!state.machine.tryBufferEditorChange(editorContent, OLD, editorDirty)) {
            editor = state.scheduler.run('editor', () => state.machine.onEditorChanged(editorContent, OLD, false, editorDirty));
        }
        await Promise.all([disk, editor]);
        await state.scheduler.run('editor', () => state.machine.flushInput(true));
        await sleep(400);
    }

    for (const [walDelayMs, editorAtMs] of [[100, 20], [300, 5], [10, 60]]) {
        it(`does not duplicate a reload of the written bytes (commit ${walDelayMs}ms, reload at ${editorAtMs}ms)`, async () => {
            const state = harness(walDelayMs, false);
            await race(state, NEW, editorAtMs, false);
            assert.strictEqual(state.journal.content, NEW);
            assert.deepStrictEqual(state.isolations, []);
            state.machine.dispose();
        });

        it(`still merges concurrent typing exactly once (commit ${walDelayMs}ms, keystroke at ${editorAtMs}ms)`, async () => {
            const state = harness(walDelayMs, true);
            await race(state, `${OLD}TYPED`, editorAtMs, true);
            const content = state.journal.content;
            assert.strictEqual((content.match(/TYPED/g) ?? []).length, 1);
            assert.strictEqual((content.match(/\\section/g) ?? []).length, 2);
            assert.ok(content.includes('alpha gamma'));
            state.machine.dispose();
        });
    }

    it('treats a snapshot whose target is already current as a no-op', async () => {
        const journal = {
            content: NEW,
            commits: 0,
            operationDisposition: () => 'unknown',
            async commitLocalOperation(operation, opId) {
                this.commits += 1;
                this.content = operation.apply(this.content);
                return { id: opId };
            },
        };
        const session = new DurableDocumentSession({
            documentId: 'doc',
            sendUpdate: async () => { },
            getSourceId: () => 'source',
            onChange: () => { },
            onError: () => { },
        }, journal);
        const accepted = await session.acceptSnapshotAgainstAncestor(OLD, NEW);
        assert.strictEqual(accepted.content, NEW);
        assert.strictEqual(journal.content, NEW);
        assert.strictEqual(journal.commits, 0);
    });
});
