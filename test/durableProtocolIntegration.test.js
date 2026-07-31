const assert = require('assert');
const { mkdtemp, rm } = require('fs/promises');
const os = require('os');
const path = require('path');
const { DocumentJournal } = require('../out/sync/documentJournal');
const { DurableDocumentSession } = require('../out/sync/durableDocumentSession');
const { TextOperation } = require('../out/sync/textOperation');

describe('durable OT protocol integration', () => {
    it('converges two journal-backed clients without a snapshot overwrite', async () => {
        const directory = await mkdtemp(path.join(os.tmpdir(), 'hershel-durable-protocol-'));
        const sent = [];
        const errors = [];
        const journals = await Promise.all(['a', 'b'].map(client => DocumentJournal.open({
            directory: path.join(directory, client),
            documentId: 'doc-1',
            initialRevision: 0,
            initialContent: 'base',
            walOptions: { idleCommitMs: 1, maxCommitMs: 5 },
        })));
        const sessions = [];
        let nextOperationId = 0;
        for (const [index, client] of ['a', 'b'].entries()) {
            sessions.push(await DurableDocumentSession.create({
                documentId: 'doc-1',
                authoritativeContent: 'base',
                authoritativeRevision: 0,
                journal: journals[index],
                sendUpdate: async update => sent.push({ client, update }),
                getSourceId: () => `socket-${client}`,
                createOperationId: () => `${client}-${++nextOperationId}`,
                onChange: () => {},
                onError: error => errors.push(error),
                retryDelayMs: 100000,
                fatalDelayMs: 100000,
            }));
        }

        const writes = [
            sessions[0].submitSnapshot('base-A'),
            sessions[1].submitSnapshot('base-B'),
        ];
        await new Promise(resolve => setTimeout(resolve, 20));
        const history = [];
        const snapshots = ['base'];
        let serverContent = 'base';
        while (sent.length > 0) {
            const {client, update} = sent.shift();
            const sender = client === 'a' ? sessions[0] : sessions[1];
            const other = client === 'a' ? sessions[1] : sessions[0];
            const baseContent = snapshots[update.v];
            let operation = TextOperation.fromLegacyEdits(baseContent, update.op);
            for (let revision = update.v; revision < history.length; revision++) {
                const [transformed] = TextOperation.transform(operation, history[revision]);
                operation = transformed;
            }
            const currentContent = serverContent;
            serverContent = operation.apply(serverContent);
            const version = history.length;
            history.push(operation);
            snapshots.push(serverContent);
            await sender.receiveUpdate({ doc: 'doc-1', v: version });
            await other.receiveUpdate({
                doc: 'doc-1',
                v: version,
                op: operation.toLegacyEdits(currentContent),
                meta: { source: `socket-${client}` },
            });
        }
        await Promise.all(writes);
        assert.strictEqual(errors.length, 0);
        assert.strictEqual(sessions[0].content, sessions[1].content);
        assert.strictEqual(sessions[0].acknowledgedContent, sessions[1].acknowledgedContent);
        for (const session of sessions) {
            await session.close();
        }
        await rm(directory, { recursive: true, force: true });
    });
});
