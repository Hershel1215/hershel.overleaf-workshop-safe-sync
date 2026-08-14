const assert = require('assert');
const { access, mkdtemp, readFile, readdir, rm } = require('fs/promises');
const os = require('os');
const path = require('path');
const {
    DocumentJournal,
    MAX_DOCUMENT_WAL_BYTES,
    MAX_DOCUMENT_WAL_RECORDS,
    MAX_SETTLED_OPERATION_IDS,
} = require('../out/sync/documentJournal');
const {AtomicCheckpointStore, DurableWal} = require('../out/sync/durableWal');
const { OtClientState } = require('../out/sync/otClientState');
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
            initialPath: 'main.tex',
            walOptions: { idleCommitMs: 1, maxCommitMs: 5 },
        });
    }

    async function walGenerations(documentId = 'doc/one') {
        const prefix = `${encodeURIComponent(documentId)}.wal.`;
        return (await readdir(path.join(directory, 'documents')))
            .filter(entry => entry.startsWith(prefix));
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

    it('durably binds the project path before a local operation can survive restart', async () => {
        const first = await DocumentJournal.open({
            directory,
            documentId: 'path-bound',
            initialRevision: 0,
            initialContent: 'ab',
            initialPath: '/sections/main.tex',
            walOptions: {idleCommitMs: 1, maxCommitMs: 5},
        });
        await first.commitLocalOperation(edit('ab', 1, 0, 'L'), 'path-op', true);
        await first.close();

        const recovered = await DocumentJournal.openExisting(directory, 'path-bound');
        assert(recovered);
        assert.strictEqual(recovered.lastKnownPath, 'sections/main.tex');
        assert.strictEqual(recovered.operationDisposition('path-op'), 'pending');
        await recovered.close();
    });

    it('replays a moved path binding from WAL before the next checkpoint', async () => {
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'path-moved',
            initialRevision: 0,
            initialContent: 'ab',
            initialPath: 'old/main.tex',
            walOptions: {idleCommitMs: 1, maxCommitMs: 5},
        });
        await journal.bindPath('/new/main.tex');
        await journal.commitLocalOperation(edit('ab', 2, 0, '!'), 'move-op', true);
        // Simulate process death without publishing a fresh checkpoint.
        await journal.wal.close();

        const recovered = await DocumentJournal.openExisting(directory, 'path-moved');
        assert(recovered);
        assert.strictEqual(recovered.lastKnownPath, 'new/main.tex');
        assert.strictEqual(recovered.content, 'ab!');
        await recovered.close();
    });

    it('rejects path traversal before it reaches the document WAL', async () => {
        await assert.rejects(
            () => DocumentJournal.open({
                directory,
                documentId: 'invalid-path',
                initialRevision: 0,
                initialContent: '',
                initialPath: '../main.tex',
            }),
            /Invalid document path binding/,
        );
    });

    it('rejects a local operation until its project path is durably bound', async () => {
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'unbound-path',
            initialRevision: 0,
            initialContent: 'ab',
        });
        await assert.rejects(
            () => journal.commitLocalOperation(edit('ab', 2, 0, '!'), 'unbound-op', true),
            /requires a durable project path binding/,
        );
        assert.strictEqual(journal.operationDisposition('unbound-op'), 'unknown');
        await journal.close();
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
            initialPath: 'large.tex',
            walOptions: { idleCommitMs: 1, maxCommitMs: 5 },
        });
        await journal.commitLocalOperation(edit(large, 50000, 0, 'x'), 'local-1', true);
        await journal.commitLocalOperation(edit(`${large.slice(0, 50000)}x${large.slice(50000)}`, 50001, 0, 'y'), 'local-2', true);
        await journal.close();

        const generations = await walGenerations('large');
        assert.strictEqual(generations.length, 1);
        const walPath = path.join(directory, 'documents', generations[0]);
        const walBytes = (await readFile(walPath)).length;
        assert(walBytes < 5000, `operation WAL unexpectedly used ${walBytes} bytes`);
    });

    it('compacts one oversized edit immediately at the WAL byte bound', async () => {
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'large-edit',
            initialRevision: 0,
            initialContent: 'a',
            initialPath: 'large-edit.tex',
        });
        const insertion = 'x'.repeat(MAX_DOCUMENT_WAL_BYTES + 1);
        await journal.commitLocalOperation(
            edit('a', 1, 0, insertion),
            'large-edit-1',
            true,
        );

        const generations = await walGenerations('large-edit');
        assert.strictEqual(generations.length, 1);
        assert.strictEqual(
            (await readFile(path.join(directory, 'documents', generations[0]))).length,
            0,
        );
        assert.strictEqual(journal.content.length, insertion.length + 1);
        await journal.close();

        const recovered = await DocumentJournal.openExisting(directory, 'large-edit');
        assert.strictEqual(recovered.content.length, insertion.length + 1);
        assert.strictEqual(recovered.operationDisposition('large-edit-1'), 'pending');
        await recovered.close();
    });

    it('recovers the old generation when compaction crashes before checkpoint publication', async () => {
        const initial = await open();
        await initial.close();
        const firstGeneration = (await walGenerations())[0];
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'doc/one',
            initialRevision: 0,
            initialContent: 'ab',
            compactionHooks: {
                afterGenerationSync: async () => {
                    throw new Error('crash-before-checkpoint');
                },
            },
        });
        await journal.commitLocalOperation(edit('ab', 1, 0, 'L'), 'before-cut', true);
        await assert.rejects(() => journal.saveCheckpoint(), /crash-before-checkpoint/);

        const recovered = await open();
        assert.strictEqual(recovered.content, 'aLb');
        assert.strictEqual(recovered.operationDisposition('before-cut'), 'pending');
        assert.strictEqual((await walGenerations())[0], firstGeneration);
        assert.strictEqual((await walGenerations()).length, 1);
        await recovered.close();
    });

    it('recovers the new generation when compaction crashes after checkpoint publication', async () => {
        const initial = await open();
        await initial.close();
        const firstGeneration = (await walGenerations())[0];
        const journal = await DocumentJournal.open({
            directory,
            documentId: 'doc/one',
            initialRevision: 0,
            initialContent: 'ab',
            compactionHooks: {
                afterCheckpointPublish: async () => {
                    throw new Error('crash-after-checkpoint');
                },
            },
        });
        await journal.commitLocalOperation(edit('ab', 1, 0, 'L'), 'after-cut', true);
        await assert.rejects(() => journal.saveCheckpoint(), /crash-after-checkpoint/);

        const recovered = await open();
        assert.strictEqual(recovered.content, 'aLb');
        assert.strictEqual(recovered.operationDisposition('after-cut'), 'pending');
        assert.notStrictEqual((await walGenerations())[0], firstGeneration);
        assert.strictEqual((await walGenerations()).length, 1);
        await recovered.close();
    });

    it('bounds WAL records and settled disposition evidence across thousands of edits', async () => {
        const journal = await open();
        let content = 'ab';
        const operationCount = 1200;
        for (let index = 0; index < operationCount; index++) {
            const operation = edit(content, content.length, 0, 'x');
            await journal.commitLocalOperation(operation, `stress-${index}`, true);
            await journal.commitRemoteOperation(
                journal.currentOutgoingOperation.operation,
                index,
                true,
            );
            content += 'x';
        }

        const generations = await walGenerations();
        assert.strictEqual(generations.length, 1);
        const wal = await readFile(path.join(directory, 'documents', generations[0]), 'utf8');
        const recordCount = wal.length === 0 ? 0 : wal.trimEnd().split('\n').length;
        assert(recordCount < MAX_DOCUMENT_WAL_RECORDS);
        assert(Buffer.byteLength(wal) < MAX_DOCUMENT_WAL_BYTES);
        assert.strictEqual(journal.content, content);
        assert.strictEqual(journal.operationDisposition('stress-0'), 'unknown');
        assert.strictEqual(journal.operationDisposition(`stress-${operationCount - 1}`), 'settled');
        await journal.close();

        const checkpoint = JSON.parse(await readFile(
            path.join(directory, 'documents', 'doc%2Fone.checkpoint.json'),
            'utf8',
        ));
        assert(checkpoint.state.settledOperationIds.length <= MAX_SETTLED_OPERATION_IDS);
        assert(checkpoint.state.knownOperationIds.length <= MAX_SETTLED_OPERATION_IDS);
        const recovered = await open();
        assert.strictEqual(recovered.content, content);
        assert.strictEqual(recovered.revision, operationCount);
        assert.strictEqual(recovered.operationDisposition('stress-0'), 'unknown');
        assert.strictEqual(
            recovered.operationDisposition(`stress-${operationCount - 1}`),
            'settled',
        );
        await recovered.close();
    });

    it('preserves all ids when importing a composed legacy pending operation', async () => {
        const legacy = new OtClientState(0, 'ab');
        await legacy.submitOperation(edit('ab', 1, 0, 'L'), 'legacy-1');
        await legacy.submitOperation(edit('aLb', 2, 0, 'X'), 'legacy-2');
        await legacy.submitOperation(edit('aLXb', 3, 0, 'Y'), 'legacy-3');
        const journal = await open();
        await journal.seedLegacyState(legacy.serialize());
        assert.deepStrictEqual(journal.currentOutgoingOperation.ids, ['legacy-1']);
        await journal.commitRemoteOperation(
            journal.currentOutgoingOperation.operation,
            0,
            true,
        );
        assert.deepStrictEqual(journal.currentOutgoingOperation.ids, ['legacy-2', 'legacy-3']);
        await journal.close();
    });

    it('migrates legacy settled and pending operation dispositions', async () => {
        const documentId = 'legacy-dispositions';
        const documents = path.join(directory, 'documents');
        const walPath = path.join(documents, `${documentId}.wal`);
        const checkpointPath = path.join(documents, `${documentId}.checkpoint.json`);
        const state = new OtClientState(0, 'ab');
        const settled = edit('ab', 1, 0, 'L');
        await state.submitOperation(settled, 'legacy-settled');
        state.receive(settled, 0, true);
        const pending = edit('aLb', 2, 0, 'P');
        await state.submitOperation(pending, 'legacy-pending');

        const wal = await DurableWal.open(walPath, {idleCommitMs: 1, maxCommitMs: 5});
        await wal.append({
            kind: 'local-operation',
            opId: 'legacy-settled',
            opIds: ['legacy-settled'],
            operation: settled.toJSON(),
        }, true);
        await wal.append({
            kind: 'remote-operation',
            revision: 0,
            ownOperation: true,
            operation: settled.toJSON(),
        }, true);
        await wal.append({
            kind: 'local-operation',
            opId: 'legacy-pending',
            opIds: ['legacy-pending'],
            operation: pending.toJSON(),
        }, true);
        await wal.close();
        const checkpoint = new AtomicCheckpointStore(checkpointPath);
        await checkpoint.save({
            walSequence: 3,
            state: {
                schemaVersion: 1,
                otState: state.serialize(),
                submittedSources: [],
            },
        });

        const migrated = await DocumentJournal.openExisting(directory, documentId);
        assert(migrated, 'legacy checkpoint was not migrated');
        assert.strictEqual(migrated.operationDisposition('legacy-settled'), 'settled');
        assert.strictEqual(migrated.operationDisposition('legacy-pending'), 'pending');
        await migrated.close();
    });

    it('migrates a complete legacy checkpoint after its WAL was cleared', async () => {
        const documentId = 'legacy-checkpoint-only';
        const documents = path.join(directory, 'documents');
        const walPath = path.join(documents, `${documentId}.wal`);
        const checkpointPath = path.join(documents, `${documentId}.checkpoint.json`);
        const state = new OtClientState(4, 'base');
        const pending = edit('base', 4, 0, '-pending');
        await state.submitOperation(pending, 'legacy-checkpoint-pending');

        const emptyWal = await DurableWal.open(walPath, {idleCommitMs: 1, maxCommitMs: 5});
        await emptyWal.close();
        const checkpoint = new AtomicCheckpointStore(checkpointPath);
        await checkpoint.save({
            walSequence: 7,
            state: {
                schemaVersion: 1,
                otState: state.serialize(),
                submittedSources: ['legacy-source'],
                lastSubmittedRevision: 4,
            },
        });

        const migrated = await DocumentJournal.openExisting(directory, documentId);
        assert(migrated, 'checkpoint-only legacy journal was not migrated');
        assert.strictEqual(migrated.content, 'base-pending');
        assert.strictEqual(migrated.acknowledgedContent, 'base');
        assert.strictEqual(migrated.revision, 4);
        assert.deepStrictEqual(migrated.pendingOperationIds, ['legacy-checkpoint-pending']);
        assert.strictEqual(
            migrated.operationDisposition('legacy-checkpoint-pending'),
            'pending',
        );
        assert.deepStrictEqual(migrated.transportSources, ['legacy-source']);
        assert.strictEqual(migrated.previousSubmittedRevision, 4);
        const published = JSON.parse(await readFile(checkpointPath, 'utf8'));
        assert.strictEqual(published.walSequence, 0);
        assert.strictEqual(published.state.schemaVersion, 3);
        assert.match(published.state.walGeneration, /^[0-9a-f-]{36}$/);
        assert.strictEqual((await walGenerations(documentId)).length, 1);
        await assert.rejects(() => access(walPath), error => error.code === 'ENOENT');
        await migrated.close();
    });

    it('fails closed for a legacy checkpoint ahead of a non-empty restarted WAL', async () => {
        const documentId = 'ambiguous-legacy-wal';
        const documents = path.join(directory, 'documents');
        const walPath = path.join(documents, `${documentId}.wal`);
        const checkpointPath = path.join(documents, `${documentId}.checkpoint.json`);
        const state = new OtClientState(3, 'checkpoint');
        const checkpoint = new AtomicCheckpointStore(checkpointPath);
        await checkpoint.save({
            walSequence: 7,
            state: {
                schemaVersion: 1,
                otState: state.serialize(),
                submittedSources: [],
            },
        });
        const restarted = await DurableWal.open(walPath, {idleCommitMs: 1, maxCommitMs: 5});
        await restarted.append({
            kind: 'transport-state',
            submittedSources: ['ambiguous-new-generation'],
            lastSubmittedRevision: 3,
        }, true);
        await restarted.close();

        await assert.rejects(
            () => DocumentJournal.openExisting(directory, documentId),
            /Document checkpoint is ahead of its WAL/,
        );
    });

    it('persists pending and settled operation disposition across reopen', async () => {
        const first = await open();
        await first.commitLocalOperation(edit('ab', 1, 0, 'L'), 'logical-1', true);
        await first.commitLocalOperation(edit('aLb', 2, 0, 'X'), 'logical-2', true);
        await first.commitLocalOperation(edit('aLXb', 2, 1, ''), 'logical-3', true);
        assert.strictEqual(first.operationDisposition('logical-1'), 'pending');
        assert.strictEqual(first.operationDisposition('logical-2'), 'pending');
        assert.strictEqual(first.operationDisposition('logical-3'), 'pending');

        await first.commitRemoteOperation(first.currentOutgoingOperation.operation, 0, true);
        for (const id of ['logical-1', 'logical-2', 'logical-3']) {
            assert.strictEqual(first.operationDisposition(id), 'settled');
        }
        assert.strictEqual(first.hasPendingOperations, false);
        await first.close();

        const recovered = await open();
        for (const id of ['logical-1', 'logical-2', 'logical-3']) {
            assert.strictEqual(recovered.operationDisposition(id), 'settled');
        }
        assert.strictEqual(recovered.operationDisposition('never-seen'), 'unknown');
        await recovered.close();
    });

    it('replays settled disposition when an ACK follows a pending checkpoint', async () => {
        const first = await open();
        await first.commitLocalOperation(edit('ab', 1, 0, 'L'), 'checkpointed-pending', true);
        await first.saveCheckpoint();
        assert.strictEqual(first.operationDisposition('checkpointed-pending'), 'pending');

        await first.commitRemoteOperation(first.currentOutgoingOperation.operation, 0, true);
        assert.strictEqual(first.operationDisposition('checkpointed-pending'), 'settled');
        // Simulate process death: close only the selected WAL handle, without
        // giving DocumentJournal.close() a chance to publish another checkpoint.
        await first.wal.close();

        const recovered = await open();
        assert.strictEqual(recovered.content, 'aLb');
        assert.strictEqual(recovered.operationDisposition('checkpointed-pending'), 'settled');
        await recovered.close();
    });

    it('removes WAL and checkpoint state behind one durable directory boundary', async () => {
        const journal = await open();
        await journal.commitLocalOperation(edit('ab', 1, 0, 'L'), 'remove-1', true);
        await journal.close();
        assert.strictEqual((await walGenerations()).length, 1);
        const walPath = path.join(directory, 'documents', 'doc%2Fone.wal');
        const checkpointPath = path.join(
            directory,
            'documents',
            'doc%2Fone.checkpoint.json',
        );

        await DocumentJournal.remove(directory, 'doc/one');

        await assert.rejects(() => access(walPath), error => error.code === 'ENOENT');
        await assert.rejects(() => access(checkpointPath), error => error.code === 'ENOENT');
        assert.deepStrictEqual(await walGenerations(), []);
    });

    it('does not delete a different document whose id extends the WAL prefix', async () => {
        const first = await open();
        await first.commitLocalOperation(edit('ab', 1, 0, 'L'), 'first-op', true);
        await first.close();
        const siblingId = 'doc/one.wal.123e4567-e89b-42d3-a456-426614174000';
        const sibling = await DocumentJournal.open({
            directory,
            documentId: siblingId,
            initialRevision: 0,
            initialContent: 'sibling',
            initialPath: 'sibling.tex',
            walOptions: {idleCommitMs: 1, maxCommitMs: 5},
        });
        await sibling.commitLocalOperation(
            edit('sibling', 7, 0, '!'),
            'sibling-op',
            true,
        );
        await sibling.close();

        await DocumentJournal.remove(directory, 'doc/one');

        const recovered = await DocumentJournal.openExisting(directory, siblingId, {
            idleCommitMs: 1,
            maxCommitMs: 5,
        });
        assert(recovered, 'prefix-sharing sibling journal was deleted');
        assert.strictEqual(recovered.content, 'sibling!');
        await recovered.close();
    });

    it('finishes deletion after a crash between WAL and checkpoint unlink', async () => {
        const journal = await open();
        await journal.commitLocalOperation(edit('ab', 1, 0, 'L'), 'remove-wal-cut', true);
        await journal.close();
        const documents = path.join(directory, 'documents');
        const walPath = path.join(documents, 'doc%2Fone.wal');
        const checkpointPath = path.join(documents, 'doc%2Fone.checkpoint.json');
        const tombstonePath = path.join(documents, 'doc%2Fone.delete.json');

        await assert.rejects(
            DocumentJournal.remove(directory, 'doc/one', {
                afterWalRemove: async () => { throw new Error('crash-after-wal-unlink'); },
            }),
            /crash-after-wal-unlink/,
        );
        await assert.rejects(() => access(walPath), error => error.code === 'ENOENT');
        assert.deepStrictEqual(await walGenerations(), []);
        await access(checkpointPath);
        await access(tombstonePath);

        assert.deepStrictEqual(await DocumentJournal.listExisting(directory), []);
        for (const target of [walPath, checkpointPath, tombstonePath]) {
            await assert.rejects(() => access(target), error => error.code === 'ENOENT');
        }
    });

    it('finishes deletion after a crash immediately after publishing the tombstone', async () => {
        const journal = await open();
        await journal.commitLocalOperation(edit('ab', 1, 0, 'L'), 'remove-tombstone-cut', true);
        await journal.close();
        const documents = path.join(directory, 'documents');
        const checkpointPath = path.join(documents, 'doc%2Fone.checkpoint.json');
        const tombstonePath = path.join(documents, 'doc%2Fone.delete.json');

        await assert.rejects(
            DocumentJournal.remove(directory, 'doc/one', {
                afterTombstone: async () => { throw new Error('crash-after-tombstone'); },
            }),
            /crash-after-tombstone/,
        );
        assert.strictEqual((await walGenerations()).length, 1);
        await access(checkpointPath);
        await access(tombstonePath);

        assert.deepStrictEqual(await DocumentJournal.listExisting(directory), []);
        assert.deepStrictEqual(await walGenerations(), []);
        for (const target of [checkpointPath, tombstonePath]) {
            await assert.rejects(() => access(target), error => error.code === 'ENOENT');
        }
    });

    it('finishes deletion after a crash following checkpoint unlink', async () => {
        const journal = await open();
        await journal.commitLocalOperation(edit('ab', 1, 0, 'L'), 'remove-checkpoint-cut', true);
        await journal.close();
        const documents = path.join(directory, 'documents');
        const walPath = path.join(documents, 'doc%2Fone.wal');
        const checkpointPath = path.join(documents, 'doc%2Fone.checkpoint.json');
        const tombstonePath = path.join(documents, 'doc%2Fone.delete.json');

        await assert.rejects(
            DocumentJournal.remove(directory, 'doc/one', {
                afterCheckpointRemove: async () => {
                    throw new Error('crash-after-checkpoint-unlink');
                },
            }),
            /crash-after-checkpoint-unlink/,
        );
        await assert.rejects(() => access(walPath), error => error.code === 'ENOENT');
        await assert.rejects(() => access(checkpointPath), error => error.code === 'ENOENT');
        await access(tombstonePath);

        assert.strictEqual(await DocumentJournal.openExisting(directory, 'doc/one'), undefined);
        await assert.rejects(() => access(tombstonePath), error => error.code === 'ENOENT');
    });
});
