const assert = require('assert');
const { mkdtemp, readFile, rm } = require('fs/promises');
const os = require('os');
const path = require('path');
const { DocumentJournal } = require('../out/sync/documentJournal');
const { TextOperation } = require('../out/sync/textOperation');

function edit(base, start, remove, insert) {
    return new TextOperation()
        .retain(start)
        .remove(remove)
        .insert(insert)
        .retain(base.length - start - remove);
}

describe('document operation journal', () => {
    let directory;

    beforeEach(async () => {
        directory = await mkdtemp(path.join(os.tmpdir(), 'hershel-doc-journal-'));
    });

    afterEach(async () => {
        await rm(directory, { recursive: true, force: true });
    });

    async function open() {
        return DocumentJournal.open({
            directory,
            documentId: 'doc/one',
            initialRevision: 0,
            initialContent: 'ab',
            walOptions: { idleCommitMs: 1, maxCommitMs: 5 },
        });
    }

    it('replays journaled local operations after a restart', async () => {
        const first = await open();
        const operation = edit('ab', 1, 0, 'L');
        const submission = await first.commitLocalOperation(operation, 'replica:doc:1', true);
        assert.strictEqual(submission.send.operation.apply('ab'), 'aLb');
        await first.close();

        const recovered = await open();
        assert.strictEqual(recovered.content, 'aLb');
        assert.strictEqual(recovered.acknowledgedContent, 'ab');
        assert.deepStrictEqual(recovered.currentOutgoingOperation.ids, ['replica:doc:1']);
        await recovered.close();
    });

    it('replays remote transforms over outstanding and pending operations', async () => {
        const first = await open();
        await first.commitLocalOperation(edit('ab', 1, 0, 'L'), 'local-1', true);
        await first.commitLocalOperation(edit('aLb', 2, 0, 'X'), 'local-2', true);
        await first.commitRemoteOperation(edit('ab', 1, 0, 'R'), 0, false);
        assert.strictEqual(first.content, 'aLXRb');
        await first.close();

        const recovered = await open();
        assert.strictEqual(recovered.content, 'aLXRb');
        assert.strictEqual(recovered.acknowledgedContent, 'aRb');
        const acknowledgement = recovered.currentOutgoingOperation.operation;
        await recovered.commitRemoteOperation(acknowledgement, 1, true);
        assert.strictEqual(recovered.currentOutgoingOperation.ids[0], 'local-2');
        await recovered.close();
    });

    it('persists transport source evidence before retry', async () => {
        const first = await open();
        await first.commitLocalOperation(edit('ab', 1, 0, 'L'), 'local-1', true);
        await first.commitTransportState(['socket-old', 'socket-new'], 0);
        await first.close();

        const recovered = await open();
        assert.deepStrictEqual(recovered.transportSources, ['socket-old', 'socket-new']);
        assert.strictEqual(recovered.previousSubmittedRevision, 0);
        await recovered.close();
    });

    it('stores compact operation events rather than full content per edit', async () => {
        const large = 'a'.repeat(100000);
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'large',
            initialRevision: 0,
            initialContent: large,
            walOptions: { idleCommitMs: 1, maxCommitMs: 5 },
        });
        await journal.commitLocalOperation(edit(large, 50000, 0, 'x'), 'local-1', true);
        await journal.commitLocalOperation(edit(`${large.slice(0, 50000)}x${large.slice(50000)}`, 50001, 0, 'y'), 'local-2', true);
        await journal.close();

        const walPath = path.join(directory, 'documents', 'large.wal');
        const walBytes = (await readFile(walPath)).length;
        assert(walBytes < 5000, `operation WAL unexpectedly used ${walBytes} bytes`);
    });
});
