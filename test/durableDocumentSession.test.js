const assert = require('assert');
const { mkdtemp, rm } = require('fs/promises');
const os = require('os');
const path = require('path');
const { DocumentJournal } = require('../out/sync/documentJournal');
const {
    DocumentSessionRetiredError,
    DurableDocumentSession,
} = require('../out/sync/durableDocumentSession');

async function flush() {
    await new Promise(resolve => setImmediate(resolve));
}

async function waitUntil(predicate, timeoutMs = 1000) {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
        if (Date.now() >= deadline) {
            assert.fail('condition did not become true before timeout');
        }
        await new Promise(resolve => setTimeout(resolve, 5));
    }
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
            initialPath: 'main.tex',
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
        await waitUntil(() => harness.sent.length === 1);
        assert.strictEqual(harness.sent.length, 1);
        assert.strictEqual(acknowledged, false);
        assert.deepStrictEqual(harness.journal.currentOutgoingOperation.ids, ['replica:doc-1:1']);
        await harness.session.receiveUpdate({ doc: 'doc-1', v: 0 });
        await write;
        assert.strictEqual(harness.session.acknowledgedContent, 'aLb');
        await harness.session.close();
    });

    it('drains an in-flight transition before closing its journal', async () => {
        const harness = await createHarness();
        let releaseCommit;
        let announceCommit;
        const commitStarted = new Promise(resolve => { announceCommit = resolve; });
        const commitGate = new Promise(resolve => { releaseCommit = resolve; });
        const commitRemote = harness.journal.commitRemoteOperation.bind(harness.journal);
        harness.journal.commitRemoteOperation = async (...args) => {
            announceCommit();
            await commitGate;
            return commitRemote(...args);
        };
        let journalClosed = false;
        const closeJournal = harness.journal.close.bind(harness.journal);
        harness.journal.close = async () => {
            journalClosed = true;
            await closeJournal();
        };

        const receive = harness.session.receiveUpdate({
            doc: 'doc-1',
            v: 0,
            op: [{p: 1, i: 'R'}],
            meta: {source: 'remote'},
        });
        await commitStarted;
        const close = harness.session.close();
        await new Promise(resolve => setImmediate(resolve));
        assert.strictEqual(journalClosed, false);
        releaseCommit();
        await Promise.all([receive, close]);
        assert.strictEqual(journalClosed, true);
    });

    it('keeps receiving acknowledgements until an in-flight transport send settles', async () => {
        let releaseSend;
        let announceSend;
        const sendGate = new Promise(resolve => { releaseSend = resolve; });
        const sendStarted = new Promise(resolve => { announceSend = resolve; });
        const harness = await createHarness('ab', 0, {
            sendUpdate: async update => {
                harness.sent.push(update);
                announceSend();
                await sendGate;
            },
        });

        const accepted = await harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        await sendStarted;
        let closeSettled = false;
        const closing = harness.session.close().then(() => { closeSettled = true; });
        const receiving = harness.session.receiveUpdate({doc: 'doc-1', v: 0});
        await receiving;
        assert.strictEqual(await accepted.acknowledgement, 'aLb');
        await flush();
        assert.strictEqual(closeSettled, false, 'close ignored an in-flight applyOtUpdate');

        releaseSend();
        await closing;
        assert.strictEqual(harness.session.hasPendingOperations, false);
    });

    it('returns a durable receipt when close races an in-flight local WAL append', async () => {
        const harness = await createHarness();
        const originalCommit = harness.journal.commitLocalOperation.bind(harness.journal);
        let releaseCommit;
        let announceCommit;
        const commitGate = new Promise(resolve => { releaseCommit = resolve; });
        const commitStarted = new Promise(resolve => { announceCommit = resolve; });
        harness.journal.commitLocalOperation = async (...args) => {
            announceCommit();
            await commitGate;
            return originalCommit(...args);
        };

        const accepting = harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        await commitStarted;
        const closing = harness.session.close();
        releaseCommit();
        const accepted = await accepting;
        const retired = await accepted.acknowledgement.catch(error => error);
        await closing;

        assert(retired instanceof DocumentSessionRetiredError);
        const recovered = await DocumentJournal.openExisting(directory, 'doc-1');
        assert.strictEqual(recovered.content, 'aLb');
        assert(recovered.currentOutgoingOperation);
        await recovered.close();
    });

    it('journals a local task queued before close instead of dropping it', async () => {
        const harness = await createHarness();
        const originalCommit = harness.journal.commitRemoteOperation.bind(harness.journal);
        let releaseRemote;
        let announceRemote;
        const remoteGate = new Promise(resolve => { releaseRemote = resolve; });
        const remoteStarted = new Promise(resolve => { announceRemote = resolve; });
        harness.journal.commitRemoteOperation = async (...args) => {
            announceRemote();
            await remoteGate;
            return originalCommit(...args);
        };
        const receiving = harness.session.receiveUpdate({
            doc: 'doc-1',
            v: 0,
            op: [{p: 1, i: 'R'}],
            meta: {source: 'remote'},
        });
        await remoteStarted;
        const accepting = harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        const closing = harness.session.close();
        releaseRemote();
        await receiving;
        const accepted = await accepting;
        const retired = await accepted.acknowledgement.catch(error => error);
        await closing;

        assert(retired instanceof DocumentSessionRetiredError);
        assert(accepted.content.includes('L'));
        assert(accepted.content.includes('R'));
        const recovered = await DocumentJournal.openExisting(directory, 'doc-1');
        assert(recovered.currentOutgoingOperation);
        await recovered.close();
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

    it('reopens one WAL owner and resends a journaled edit after disconnect retirement', async () => {
        const first = await createHarness();
        const accepted = await first.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        const retiredAcknowledgement = accepted.acknowledgement.catch(error => error);
        await first.session.close();
        const retiredError = await retiredAcknowledgement;
        assert(retiredError instanceof DocumentSessionRetiredError);

        const journal = await DocumentJournal.openExisting(directory, 'doc-1', {
            idleCommitMs: 1,
            maxCommitMs: 5,
        });
        const recovered = await createHarness('ab', 0, {journal});
        assert.strictEqual(recovered.session.content, 'aLb');
        assert.strictEqual(recovered.sent.length, 1);
        await recovered.session.receiveUpdate({doc: 'doc-1', v: 0});
        assert.strictEqual(recovered.session.acknowledgedContent, 'aLb');
        await recovered.session.close();
    });

    it('journals repeated offline edits and replays each outgoing operation once after catch-up', async () => {
        const harness = await createHarness('ab', 0, {
            retryDelayMs: 100000,
            fatalDelayMs: 20,
        });
        harness.session.suspendTransport();

        const first = await harness.session.acceptSnapshotAgainstAncestor('ab', 'a1b', true);
        await new Promise(resolve => setTimeout(resolve, 12));
        const second = await harness.session.acceptSnapshotAgainstAncestor('a1b', 'a12b', true);
        await new Promise(resolve => setTimeout(resolve, 12));
        const third = await harness.session.acceptSnapshotAgainstAncestor('a12b', 'a123b', true);
        await new Promise(resolve => setTimeout(resolve, 30));

        assert.strictEqual(harness.session.isTransportSuspended, true);
        assert.deepStrictEqual(harness.session.pendingOperationIds, [
            first.opId,
            second.opId,
            third.opId,
        ]);
        assert.strictEqual(harness.sent.length, 0, 'offline WAL append touched the socket');
        assert.strictEqual(harness.errors.length, 0, 'offline time started the fatal ACK timer');

        await harness.session.resumeTransport('ab', 0, []);
        await waitUntil(() => harness.sent.length === 1);
        assert.strictEqual(harness.sent[0].v, 0);
        assert.deepStrictEqual(harness.sent[0].dupIfSource, ['source-1']);
        await harness.session.receiveUpdate({doc: 'doc-1', v: 0});
        await waitUntil(() => harness.sent.length === 2);
        assert.strictEqual(harness.sent[1].v, 1);
        await harness.session.receiveUpdate({doc: 'doc-1', v: 1});

        assert.strictEqual(await first.acknowledgement, 'a123b');
        assert.strictEqual(await second.acknowledgement, 'a123b');
        assert.strictEqual(await third.acknowledgement, 'a123b');
        assert.deepStrictEqual(harness.sent.map(update => update.v), [0, 1]);
        assert.strictEqual(harness.session.acknowledgedContent, 'a123b');
        assert.strictEqual(harness.session.hasPendingOperations, false);
        await harness.session.close();
    });

    it('rebases an offline operation on reconnect catch-up before sending it', async () => {
        const harness = await createHarness();
        harness.session.suspendTransport();
        const accepted = await harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);

        await harness.session.resumeTransport('aRb', 1, [{
            doc: 'doc-1',
            v: 0,
            op: [{p: 1, i: 'R'}],
            meta: {source: 'remote'},
        }]);
        await waitUntil(() => harness.sent.length === 1);
        assert.strictEqual(harness.sent[0].v, 1);
        assert.strictEqual(harness.session.content, 'aLRb');

        await harness.session.receiveUpdate({doc: 'doc-1', v: 1});
        assert.strictEqual(await accepted.acknowledgement, 'aLRb');
        assert.strictEqual(harness.session.acknowledgedContent, 'aLRb');
        await harness.session.close();
    });

    it('observes the same logical operation after reconnect without submitting a second edit', async () => {
        const first = await createHarness();
        const accepted = await first.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        const firstObserver = first.session.observeOperation(accepted.opId).catch(error => error);
        await first.session.close();
        assert(await firstObserver instanceof DocumentSessionRetiredError);

        const journal = await DocumentJournal.openExisting(directory, 'doc-1', {
            idleCommitMs: 1,
            maxCommitMs: 5,
        });
        const recovered = await createHarness('ab', 0, {journal});
        const observed = recovered.session.observeOperation(accepted.opId);
        assert.strictEqual(recovered.sent.length, 1);
        await recovered.session.receiveUpdate({doc: 'doc-1', v: 0});
        assert.strictEqual(await observed, 'aLb');
        assert.strictEqual(recovered.journal.operationDisposition(accepted.opId), 'settled');
        await recovered.session.close();
    });

    it('reuses a caller-fixed operation id instead of deriving a second edit after a crash', async () => {
        const harness = await createHarness();
        const opId = 'replica:pending-action';
        const first = await harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true, opId);
        const recoveredCall = await harness.session.acceptSnapshotAgainstAncestor(
            'ab',
            'aLb',
            true,
            opId,
        );

        assert.strictEqual(first.opId, opId);
        assert.strictEqual(recoveredCall.opId, opId);
        assert.deepStrictEqual(harness.session.inspectOperation(opId), {
            disposition: 'pending',
            content: 'aLb',
            sequence: harness.session.sequence,
        });
        assert.strictEqual(harness.sent.length, 1);

        await harness.session.receiveUpdate({doc: 'doc-1', v: 0});
        assert.strictEqual(await first.acknowledgement, 'aLb');
        assert.strictEqual(await recoveredCall.acknowledgement, 'aLb');
        assert.deepStrictEqual(harness.session.inspectOperation(opId), {
            disposition: 'settled',
            content: 'aLb',
            sequence: harness.session.sequence,
        });
        assert.strictEqual(harness.sent.length, 1);
        await harness.session.close();
    });

    it('recognizes an operation already acknowledged before its session reopens', async () => {
        const first = await createHarness();
        const accepted = await first.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        await first.session.receiveUpdate({doc: 'doc-1', v: 0});
        await accepted.acknowledgement;
        await first.session.close();

        const journal = await DocumentJournal.openExisting(directory, 'doc-1');
        const recovered = await createHarness('aLb', 1, {journal});
        assert.strictEqual(await recovered.session.observeOperation(accepted.opId), 'aLb');
        assert.strictEqual(recovered.sent.length, 0);
        await assert.rejects(
            recovered.session.observeOperation('unknown-op'),
            /Cannot prove disposition/,
        );
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

    it('keeps a journaled operation alive when the first transport send fails', async () => {
        let attempts = 0;
        const harness = await createHarness('ab', 0, {
            retryDelayMs: 10,
            fatalDelayMs: 1000,
            sendUpdate: async update => {
                attempts++;
                if (attempts === 1) {
                    throw new Error('ECONNRESET');
                }
                harness.sent.push(update);
            },
        });
        const accepted = await harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb');
        assert.strictEqual(accepted.content, 'aLb');
        assert.strictEqual(harness.session.isFailed, false);
        for (let index = 0; index < 20 && attempts < 2; index++) {
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        assert(attempts >= 2, 'the durable inflight operation was not retried');
        await harness.session.receiveUpdate({ doc: 'doc-1', v: 0 });
        assert.strictEqual(await accepted.acknowledgement, 'aLb');
        assert.strictEqual(harness.errors.length, 0);
        await harness.session.close();
    });

    it('processes an ACK while a detached retry transport Promise is hung', async () => {
        let sendCount = 0;
        let announceRetry;
        let releaseRetry;
        const retryStarted = new Promise(resolve => { announceRetry = resolve; });
        const retryGate = new Promise(resolve => { releaseRetry = resolve; });
        const harness = await createHarness('ab', 0, {
            retryDelayMs: 5,
            fatalDelayMs: 1000,
            sendUpdate: async update => {
                harness.sent.push(update);
                sendCount++;
                if (sendCount === 1) {
                    throw new Error('initial transport failed');
                }
                announceRetry();
                await retryGate;
            },
        });

        const accepted = await harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        await retryStarted;
        await harness.session.receiveUpdate({doc: 'doc-1', v: 0});
        assert.strictEqual(await accepted.acknowledgement, 'aLb');

        releaseRetry();
        await harness.session.close();
    });

    it('returns the durable opId when transport-state persistence fails after the local append', async () => {
        const harness = await createHarness();
        harness.journal.commitTransportState = async () => {
            throw new Error('transport state failed');
        };

        const accepted = await harness.session.acceptSnapshotAgainstAncestor('ab', 'aLb', true);
        assert.strictEqual(accepted.opId, 'replica:doc-1:1');
        assert.strictEqual(accepted.content, 'aLb');
        await assert.rejects(accepted.acknowledgement, /transport state failed/);
        assert.strictEqual(harness.journal.operationDisposition(accepted.opId), 'pending');
        assert.strictEqual(harness.errors.length, 1);
        await harness.session.close();
    });
});
