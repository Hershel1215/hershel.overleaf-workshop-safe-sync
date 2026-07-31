const assert = require('assert');
const { mkdtemp, rm } = require('fs/promises');
const os = require('os');
const path = require('path');
const { DocumentJournal } = require('../out/sync/documentJournal');
const { DurableDocumentSession } = require('../out/sync/durableDocumentSession');

async function flush() {
    await new Promise(resolve => setImmediate(resolve));
}

describe('durable Overleaf document session', () => {
    let directory;

    beforeEach(async () => {
        directory = await mkdtemp(path.join(os.tmpdir(), 'hershel-session-v2-'));
    });

    afterEach(async () => {
        await rm(directory, { recursive: true, force: true });
    });

    async function createHarness(content = 'ab', revision = 0, extra = {}) {
        const sent = [];
        const changes = [];
        const errors = [];
        let nextId = 0;
        const journal = extra.journal ?? await DocumentJournal.open({
            directory,
            documentId: 'doc-1',
            initialRevision: revision,
            initialContent: content,
            walOptions: { idleCommitMs: 1, maxCommitMs: 5 },
        });
        const session = await DurableDocumentSession.create({
            documentId: 'doc-1',
            authoritativeContent: content,
            authoritativeRevision: revision,
            journal,
            sendUpdate: async update => { sent.push(update); },
            getSourceId: () => 'source-1',
            createOperationId: () => `replica:doc-1:${++nextId}`,
            onChange: (text, version) => changes.push([text, version]),
            onError: error => errors.push(error),
            retryDelayMs: 100000,
            fatalDelayMs: 100000,
            ...extra,
        });
        return { session, journal, sent, changes, errors };
    }

    it('does not send until the local operation is journaled', async () => {
        const harness = await createHarness();
        let acknowledged = false;
        const write = harness.session.submitSnapshot('aLb').then(() => { acknowledged = true; });
        await new Promise(resolve => setTimeout(resolve, 10));
        assert.strictEqual(harness.sent.length, 1);
        assert.strictEqual(acknowledged, false);
        assert.deepStrictEqual(harness.journal.currentOutgoingOperation.ids, ['replica:doc-1:1']);
        await harness.session.receiveUpdate({ doc: 'doc-1', v: 0 });
        await write;
        assert.strictEqual(harness.session.acknowledgedContent, 'aLb');
        await harness.session.close();
    });

    it('transforms an outstanding operation against a remote edit', async () => {
        const harness = await createHarness();
        const write = harness.session.submitSnapshot('aLb');
        await flush();
        await harness.session.receiveUpdate({
            doc: 'doc-1',
            v: 0,
            op: [{ p: 1, i: 'R' }],
            meta: { source: 'remote' },
        });
        assert.strictEqual(harness.session.content, 'aLRb');
        await harness.session.receiveUpdate({ doc: 'doc-1', v: 1 });
        await write;
        assert.strictEqual(harness.session.acknowledgedContent, 'aLRb');
        await harness.session.close();
    });

    it('recovers source evidence and avoids duplicate resend after ACK loss', async () => {
        const first = await createHarness();
        const abandoned = first.session.submitSnapshot('aLb');
        abandoned.catch(() => {});
        await flush();
        await first.journal.saveCheckpoint();
        first.session.dispose();
        await assert.rejects(abandoned, /disposed/);
        await first.journal.close();

        const journal = await DocumentJournal.openExisting(directory, 'doc-1', {
            idleCommitMs: 1,
            maxCommitMs: 5,
        });
        const recovered = await createHarness('aLRb', 2, {
            journal,
            catchUpUpdates: [
                { doc: 'doc-1', v: 0, op: [{ p: 1, i: 'L' }], meta: { source: 'source-1' } },
                { doc: 'doc-1', v: 1, op: [{ p: 2, i: 'R' }], meta: { source: 'remote' } },
            ],
        });
        assert.strictEqual(recovered.sent.length, 0);
        assert.strictEqual(recovered.session.content, 'aLRb');
        await recovered.session.close();
    });

    it('fails recovery when missing history cannot reproduce the server snapshot', async () => {
        const first = await createHarness();
        const abandoned = first.session.submitSnapshot('aLb');
        abandoned.catch(() => {});
        await flush();
        await first.journal.saveCheckpoint();
        first.session.dispose();
        await assert.rejects(abandoned, /disposed/);
        await first.journal.close();
        const journal = await DocumentJournal.openExisting(directory, 'doc-1');
        await assert.rejects(
            () => DurableDocumentSession.create({
                documentId: 'doc-1',
                authoritativeContent: 'aRb',
                authoritativeRevision: 1,
                journal,
                sendUpdate: async () => {},
                getSourceId: () => 'source-2',
                onChange: () => {},
                onError: () => {},
            }),
            /does not reproduce/,
        );
        await journal.close();
    });
});
