const assert = require('assert');
const { DocumentSession } = require('../out/sync/documentSession');

async function flush() {
    await new Promise(resolve => setImmediate(resolve));
}

function createHarness(content = 'ab', revision = 0) {
    const sent = [];
    const persisted = [];
    const changes = [];
    const errors = [];
    return {
        sent,
        persisted,
        changes,
        errors,
        async session(extra = {}) {
            return DocumentSession.create({
                documentId: 'doc-1',
                content,
                revision,
                sendUpdate: async update => { sent.push(update); },
                persist: async state => { persisted.push(state); },
                getSourceId: () => 'source-1',
                onChange: (text, version) => changes.push([text, version]),
                onError: error => errors.push(error),
                retryDelayMs: 100000,
                fatalDelayMs: 100000,
                ...extra,
            });
        },
    };
}

describe('DocumentSession', () => {
    it('persists before sending and resolves only after an ACK update', async () => {
        const harness = createHarness();
        const session = await harness.session();
        let completed = false;
        const write = session.submitSnapshot('aLb').then(() => { completed = true; });
        await flush();

        assert.strictEqual(harness.sent.length, 1);
        assert.strictEqual(completed, false);
        assert.strictEqual(harness.sent[0].v, 0);
        assert.strictEqual(harness.sent[0].op.length > 0, true);
        assert.strictEqual(harness.persisted.at(-1).localContent, 'aLb');

        await session.receiveUpdate({ doc: 'doc-1', v: 0 });
        await write;
        assert.strictEqual(completed, true);
        assert.strictEqual(session.acknowledgedContent, 'aLb');
        assert.strictEqual(session.revision, 1);
        session.dispose();
    });

    it('rebases local content when a remote operation wins the server revision', async () => {
        const harness = createHarness();
        const session = await harness.session();
        const write = session.submitSnapshot('aLb');
        await flush();

        await session.receiveUpdate({
            doc: 'doc-1',
            v: 0,
            op: [{ p: 1, i: 'R' }],
            meta: { source: 'other-client' },
        });
        assert.strictEqual(session.content, 'aLRb');
        assert.strictEqual(session.acknowledgedContent, 'aRb');

        await session.receiveUpdate({ doc: 'doc-1', v: 1 });
        await write;
        assert.strictEqual(session.content, 'aLRb');
        assert.strictEqual(session.acknowledgedContent, 'aLRb');
        session.dispose();
    });

    it('rebases a scanned snapshot behind content newer than its ancestor', async () => {
        const harness = createHarness('aRb');
        const session = await harness.session();
        const write = session.submitSnapshotAgainstAncestor('ab', 'aLb');
        await flush();

        assert.strictEqual(harness.sent.length, 1);
        await session.receiveUpdate({ doc: 'doc-1', v: 0 });
        assert.strictEqual(await write, 'aRLb');
        assert.strictEqual(session.acknowledgedContent, 'aRLb');
        session.dispose();
    });

    it('buffers out-of-order remote updates until the missing revision arrives', async () => {
        const harness = createHarness();
        const session = await harness.session();
        await session.receiveUpdate({
            doc: 'doc-1',
            v: 1,
            op: [{ p: 3, i: '2' }],
            meta: { source: 'remote' },
        });
        assert.strictEqual(session.content, 'ab');

        await session.receiveUpdate({
            doc: 'doc-1',
            v: 0,
            op: [{ p: 2, i: '1' }],
            meta: { source: 'remote' },
        });
        assert.strictEqual(session.content, 'ab12');
        assert.strictEqual(session.revision, 2);
        session.dispose();
    });

    it('fails closed on a mismatched delete', async () => {
        const harness = createHarness();
        const session = await harness.session();
        await session.receiveUpdate({
            doc: 'doc-1',
            v: 0,
            op: [{ p: 0, d: 'wrong' }],
            meta: { source: 'remote' },
        });
        assert.strictEqual(session.isFailed, true);
        assert.match(harness.errors[0].message, /does not match/);
        session.dispose();
    });

    it('uses catch-up operation sources to avoid duplicating an ACK-lost edit', async () => {
        const beforeCrash = createHarness();
        const firstSession = await beforeCrash.session();
        const abandonedWrite = firstSession.submitSnapshot('aLb');
        abandonedWrite.catch(() => {});
        await flush();
        const savedState = structuredClone(beforeCrash.persisted.at(-1));
        assert.deepStrictEqual(savedState.submittedSources, ['source-1']);
        firstSession.dispose();
        await assert.rejects(abandonedWrite, /disposed/);

        const recovered = createHarness('aLRb', 2);
        const session = await recovered.session({
            savedState,
            catchUpUpdates: [
                { doc: 'doc-1', v: 0, op: [{ p: 1, i: 'L' }], meta: { source: 'source-1' } },
                { doc: 'doc-1', v: 1, op: [{ p: 2, i: 'R' }], meta: { source: 'other' } },
            ],
        });
        assert.strictEqual(session.content, 'aLRb');
        assert.strictEqual(session.acknowledgedContent, 'aLRb');
        assert.strictEqual(recovered.sent.length, 0);
        session.dispose();
    });

    it('fails closed when a moved snapshot cannot prove whether a saved edit was ACKed', async () => {
        const beforeCrash = createHarness();
        const firstSession = await beforeCrash.session();
        const abandonedWrite = firstSession.submitSnapshot('aLb');
        abandonedWrite.catch(() => {});
        await flush();
        const savedState = structuredClone(beforeCrash.persisted.at(-1));
        firstSession.dispose();
        await assert.rejects(abandonedWrite, /disposed/);

        const recovered = createHarness('aLRb', 2);
        await assert.rejects(
            () => recovered.session({ savedState }),
            /Cannot prove whether the saved local operation/,
        );
    });

    it('retries a provably unapplied operation with its durable source ids', async () => {
        const beforeCrash = createHarness();
        const firstSession = await beforeCrash.session();
        const abandonedWrite = firstSession.submitSnapshot('aLb');
        abandonedWrite.catch(() => {});
        await flush();
        const savedState = structuredClone(beforeCrash.persisted.at(-1));
        firstSession.dispose();
        await assert.rejects(abandonedWrite, /disposed/);

        const recovered = createHarness();
        const session = await recovered.session({ savedState });
        assert.strictEqual(recovered.sent.length, 1);
        assert.deepStrictEqual(recovered.sent[0].dupIfSource, ['source-1']);
        await session.receiveUpdate({ doc: 'doc-1', v: 0 });
        session.dispose();
    });

    it('rejects a server revision rollback even when no local operation is pending', async () => {
        const original = createHarness('stable', 4);
        const session = await original.session();
        const savedState = structuredClone(original.persisted.at(-1));
        session.dispose();

        const rolledBack = createHarness('stable', 3);
        await assert.rejects(
            () => rolledBack.session({ savedState }),
            /revision moved backwards/,
        );
    });
});
