const assert = require('assert');
const {createHash} = require('crypto');
const {mkdtemp, mkdir, open, readFile, readdir, rm, writeFile} = require('fs/promises');
const os = require('os');
const path = require('path');
const {LocalProjectionCas} = require('../out/sync/localProjectionCas');

const bytes = value => Buffer.from(value);
const hash = value => createHash('sha256').update(value).digest('hex');

describe('local projection compare-and-swap', () => {
    let root;

    beforeEach(async () => {
        root = await mkdtemp(path.join(os.tmpdir(), 'hershel-projection-cas-'));
        await mkdir(path.join(root, '.overleaf', 'projections'), {recursive: true});
    });

    afterEach(async () => {
        await rm(root, {recursive: true, force: true});
    });

    it('installs a projection only while the expected local snapshot still owns the path', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const cas = new LocalProjectionCas(root);
        const result = await cas.replace('main.tex', hash(bytes('BASE')), bytes('REMOTE'));

        assert.strictEqual(result.applied, true);
        assert.strictEqual((await readFile(target, 'utf8')), 'REMOTE');
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('asks whether a writer holds the file before the path leaves the filesystem', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const probes = [];
        let cas;
        cas = new LocalProjectionCas(root, {
            hasWritableHandle: async filePath => {
                probes.push({filePath, duringWindow: cas.hasActiveTransition});
                return false;
            },
        });

        assert.strictEqual(
            (await cas.replace('main.tex', hash(bytes('BASE')), bytes('REMOTE'))).applied,
            true,
        );
        await writeFile(target, bytes('REMOTE'));
        assert.strictEqual((await cas.remove('main.tex', hash(bytes('REMOTE')))).applied, true);

        // The probe walks every descriptor on the machine. Inside the window
        // the pathname does not exist, so paying for it there is what makes a
        // projected file look deleted to whoever is watching it.
        assert.deepStrictEqual(probes, [
            {filePath: target, duringWindow: false},
            {filePath: target, duringWindow: false},
        ]);
    });

    it('durably creates the projection directory chain on its first transaction', async () => {
        await rm(path.join(root, '.overleaf'), {recursive: true, force: true});
        const target = path.join(root, 'chapters', 'main.tex');
        await mkdir(path.dirname(target), {recursive: true});
        await writeFile(target, bytes('BASE'));

        const result = await new LocalProjectionCas(root).replace(
            'chapters/main.tex',
            hash(bytes('BASE')),
            bytes('REMOTE'),
        );

        assert.strictEqual(result.applied, true);
        assert.strictEqual(await readFile(target, 'utf8'), 'REMOTE');
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('recovers cleanup after the live transaction was atomically tombstoned', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const interrupted = new LocalProjectionCas(root, {
            afterCleanupRename: async () => {
                throw new Error('crash-after-cleanup-rename');
            },
        });

        await assert.rejects(
            interrupted.replace('main.tex', hash(bytes('BASE')), bytes('REMOTE')),
            /crash-after-cleanup-rename/,
        );
        const transactionEntries = await readdir(
            path.join(root, '.overleaf', 'projections'),
        );
        assert.strictEqual(transactionEntries.length, 1);
        assert.match(transactionEntries[0], /^\.completed-/);
        assert.strictEqual(await readFile(target, 'utf8'), 'REMOTE');

        assert.deepStrictEqual(await new LocalProjectionCas(root).recover(), []);
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
        assert.strictEqual(await readFile(target, 'utf8'), 'REMOTE');
    });

    it('never interprets partially unlinked cleanup children as a live transaction', async () => {
        const transactionRoot = path.join(root, '.overleaf', 'projections');
        const partialStates = [
            ['displaced'],
            ['manifest.json', 'displaced'],
            ['target', 'COMMITTED'],
        ];
        for (let index = 0; index < partialStates.length; index++) {
            const completed = path.join(transactionRoot, `.completed-cut-${index}`);
            await mkdir(completed, {recursive: true});
            for (const child of partialStates[index]) {
                await writeFile(path.join(completed, child), bytes(child));
            }
            assert.deepStrictEqual(await new LocalProjectionCas(root).recover(), []);
            assert.deepStrictEqual(await readdir(transactionRoot), []);
        }
    });

    it('preserves a filesystem writer that arrives after the expected file is displaced', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const cas = new LocalProjectionCas(root, {
            onPhase: async phase => {
                if (phase === 'displaced') {
                    await writeFile(target, bytes('LOCAL'));
                }
            },
        });
        const result = await cas.replace('main.tex', hash(bytes('BASE')), bytes('REMOTE'));

        assert.strictEqual(result.applied, false);
        assert.strictEqual(Buffer.from(result.actual).toString(), 'LOCAL');
        assert.strictEqual((await readFile(target, 'utf8')), 'LOCAL');
    });

    it('proves when a writer mutates the target inode after it is installed', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const cas = new LocalProjectionCas(root, {
            onPhase: async phase => {
                if (phase === 'installed') {
                    await writeFile(target, bytes('REMOTE+RAW'));
                }
            },
        });

        const result = await cas.replace(
            'main.tex',
            hash(bytes('BASE')),
            bytes('REMOTE'),
        );

        assert.strictEqual(result.applied, false);
        assert.strictEqual(result.actualWasReadFromInstalledTarget, true);
        assert.strictEqual(Buffer.from(result.actual).toString(), 'REMOTE+RAW');
        assert.strictEqual(await readFile(target, 'utf8'), 'REMOTE+RAW');
    });

    it('does not claim an installed ancestor after a writer replaces the path', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const cas = new LocalProjectionCas(root, {
            onPhase: async phase => {
                if (phase === 'installed') {
                    await rm(target);
                    await writeFile(target, bytes('REPLACEMENT'));
                }
            },
        });

        const result = await cas.replace(
            'main.tex',
            hash(bytes('BASE')),
            bytes('REMOTE'),
        );

        assert.strictEqual(result.applied, false);
        assert.strictEqual(result.actualWasReadFromInstalledTarget, false);
        assert.strictEqual(Buffer.from(result.actual).toString(), 'REPLACEMENT');
        assert.strictEqual(await readFile(target, 'utf8'), 'REPLACEMENT');
    });

    it('invalidates a filesystem observation across the atomic displacement window', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, 'BASE');
        let releaseDisplacement;
        const displaced = new Promise(resolve => { releaseDisplacement = resolve; });
        let continueProjection;
        const projectionGate = new Promise(resolve => { continueProjection = resolve; });
        const cas = new LocalProjectionCas(root, {
            onPhase: async phase => {
                if (phase === 'displaced') {
                    releaseDisplacement();
                    await projectionGate;
                }
            },
        });
        const initialEpoch = cas.observationEpoch;
        const replacing = cas.replace('main.tex', hash(bytes('BASE')), bytes('REMOTE'));
        await displaced;

        assert.strictEqual(cas.hasActiveTransition, true);
        assert.notStrictEqual(cas.observationEpoch, initialEpoch);
        const activeEpoch = cas.observationEpoch;
        continueProjection();
        await replacing;

        assert.strictEqual(cas.hasActiveTransition, false);
        assert.notStrictEqual(cas.observationEpoch, activeEpoch);
        assert.strictEqual(await readFile(target, 'utf8'), 'REMOTE');
    });

    it('restores an inode that was already open for writing before displacement', async function() {
        if (process.platform !== 'linux') {
            this.skip();
        }
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const writer = await open(target, 'r+');
        try {
            const result = await new LocalProjectionCas(root).replace(
                'main.tex',
                hash(bytes('BASE')),
                bytes('REMOTE'),
            );
            assert.strictEqual(result.applied, false);
            await writer.truncate(0);
            await writer.writeFile(bytes('LOCAL-THROUGH-OPEN-FD'));
            await writer.sync();
        } finally {
            await writer.close();
        }

        assert.strictEqual(
            await readFile(target, 'utf8'),
            'LOCAL-THROUGH-OPEN-FD',
        );
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('restores a displaced file before startup exposes filesystem semantics', async () => {
        const transaction = path.join(root, '.overleaf', 'projections', 'interrupted');
        await mkdir(transaction, {recursive: true});
        await writeFile(path.join(transaction, 'target'), bytes('REMOTE'));
        await writeFile(path.join(transaction, 'displaced'), bytes('BASE'));
        await writeFile(path.join(transaction, 'manifest.json'), JSON.stringify({
            schemaVersion: 1,
            operation: 'replace',
            relativePath: 'main.tex',
            expectedHash: hash(bytes('BASE')),
            targetHash: hash(bytes('REMOTE')),
        }));

        const cas = new LocalProjectionCas(root);
        const recovered = await cas.recover();

        assert.deepStrictEqual(recovered, []);
        assert.strictEqual((await readFile(path.join(root, 'main.tex'), 'utf8')), 'BASE');
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('restores an unexpected uncommitted writer for the startup scanner', async () => {
        const transaction = path.join(root, '.overleaf', 'projections', 'interrupted-local');
        await mkdir(transaction, {recursive: true});
        await writeFile(path.join(transaction, 'target'), bytes('REMOTE'));
        await writeFile(path.join(transaction, 'displaced'), bytes('LOCAL-AFTER-DISPLACE'));
        await writeFile(path.join(transaction, 'manifest.json'), JSON.stringify({
            schemaVersion: 1,
            operation: 'replace',
            relativePath: 'main.tex',
            expectedHash: hash(bytes('BASE')),
            targetHash: hash(bytes('REMOTE')),
        }));

        const cas = new LocalProjectionCas(root);
        const recovered = await cas.recover();

        assert.deepStrictEqual(recovered, []);
        assert.strictEqual(
            await readFile(path.join(root, 'main.tex'), 'utf8'),
            'LOCAL-AFTER-DISPLACE',
        );
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('recognizes a writer already restored by an interrupted recovery pass', async () => {
        const transaction = path.join(root, '.overleaf', 'projections', 'recovery-interrupted');
        await mkdir(transaction, {recursive: true});
        await writeFile(path.join(root, 'main.tex'), bytes('LOCAL-AFTER-DISPLACE'));
        await writeFile(path.join(transaction, 'target'), bytes('REMOTE'));
        await writeFile(path.join(transaction, 'displaced'), bytes('LOCAL-AFTER-DISPLACE'));
        await writeFile(path.join(transaction, 'manifest.json'), JSON.stringify({
            schemaVersion: 1,
            operation: 'replace',
            relativePath: 'main.tex',
            expectedHash: hash(bytes('BASE')),
            targetHash: hash(bytes('REMOTE')),
        }));

        const recovered = await new LocalProjectionCas(root).recover();

        assert.deepStrictEqual(recovered, []);
        assert.strictEqual(
            await readFile(path.join(root, 'main.tex'), 'utf8'),
            'LOCAL-AFTER-DISPLACE',
        );
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('returns an active unresolved candidate when a committed target cannot restore a writer', async () => {
        const transaction = path.join(root, '.overleaf', 'projections', 'committed-local');
        await mkdir(transaction, {recursive: true});
        await writeFile(path.join(root, 'main.tex'), bytes('REMOTE'));
        await writeFile(path.join(transaction, 'target'), bytes('REMOTE'));
        await writeFile(path.join(transaction, 'displaced'), bytes('LOCAL-AFTER-DISPLACE'));
        await writeFile(path.join(transaction, 'manifest.json'), JSON.stringify({
            schemaVersion: 1,
            operation: 'replace',
            relativePath: 'main.tex',
            expectedHash: hash(bytes('BASE')),
            targetHash: hash(bytes('REMOTE')),
        }));
        await writeFile(path.join(transaction, 'COMMITTED'), '1\n');

        const cas = new LocalProjectionCas(root, {
            hasWritableHandle: async () => false,
        });
        const recovered = await cas.recover();

        assert.strictEqual(recovered.length, 1);
        assert.strictEqual(recovered[0].path, 'main.tex');
        assert.strictEqual(recovered[0].operation, 'replace');
        assert.strictEqual(recovered[0].expectedHash, hash(bytes('BASE')));
        assert.strictEqual(recovered[0].targetHash, hash(bytes('REMOTE')));
        assert.strictEqual(recovered[0].localHash, hash(bytes('LOCAL-AFTER-DISPLACE')));
        assert.match(recovered[0].artifactDirectory, /^\.overleaf\/conflicts\/unresolved\//);
        assert.strictEqual(
            await readFile(path.join(root, ...recovered[0].localFile.split('/')), 'utf8'),
            'LOCAL-AFTER-DISPLACE',
        );
        assert.strictEqual(await readFile(path.join(root, 'main.tex'), 'utf8'), 'REMOTE');
        assert.deepStrictEqual(
            await readdir(path.join(root, '.overleaf', 'conflicts')),
            ['unresolved'],
        );
        assert.deepStrictEqual(await cas.recover(), recovered);
        await cas.acknowledgeRecovery(recovered[0].transactionId);
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('keeps a retained transaction busy and refreshes its candidate after the writer closes', async () => {
        const target = path.join(root, 'main.tex');
        await writeFile(target, bytes('BASE'));
        const writer = await open(target, 'r+');
        let writable = true;
        const cas = new LocalProjectionCas(root, {
            // A writer that opens after the pre-displacement probe: only the
            // displaced inode inside the transaction is reported as held.
            hasWritableHandle: async filePath =>
                writable && filePath.includes(path.join('.overleaf', 'projections')),
            onPhase: async phase => {
                if (phase === 'displaced') {
                    await writeFile(target, bytes('CONCURRENT-PATH-WRITER'));
                }
            },
        });
        let result;
        try {
            result = await cas.replace(
                'main.tex',
                hash(bytes('BASE')),
                bytes('REMOTE'),
            );
            assert.strictEqual(result.applied, false);
            assert.strictEqual(result.recoveryCandidate.retainTransaction, true);
            assert.deepStrictEqual(
                await cas.inspectRecovery(result.recoveryCandidate.transactionId),
                {status: 'busy'},
            );

            await writer.truncate(0);
            await writer.writeFile(bytes('LATEST-OPEN-FD-WRITE'));
            await writer.sync();
        } finally {
            await writer.close();
            writable = false;
        }

        const inspected = await cas.inspectRecovery(
            result.recoveryCandidate.transactionId,
        );
        assert.strictEqual(inspected.status, 'ready');
        assert.strictEqual(inspected.candidate.retainTransaction, false);
        assert.strictEqual(
            inspected.candidate.localHash,
            hash(bytes('LATEST-OPEN-FD-WRITE')),
        );
        assert.strictEqual(
            await readFile(path.join(root, ...inspected.candidate.localFile.split('/')), 'utf8'),
            'LATEST-OPEN-FD-WRITE',
        );
        assert.strictEqual(
            (await readdir(path.join(root, '.overleaf', 'projections'))).length,
            1,
        );

        await cas.acknowledgeRecovery(result.recoveryCandidate.transactionId);
        assert.deepStrictEqual(await readdir(path.join(root, '.overleaf', 'projections')), []);
    });

    it('does not delete a writer that replaces the path after displacement', async () => {
        const target = path.join(root, 'figure.png');
        await writeFile(target, bytes('BASE'));
        const cas = new LocalProjectionCas(root, {
            onPhase: async phase => {
                if (phase === 'displaced') {
                    await writeFile(target, bytes('LOCAL'));
                }
            },
        });

        const result = await cas.remove('figure.png', hash(bytes('BASE')));

        assert.strictEqual(result.applied, false);
        assert.strictEqual((await readFile(target, 'utf8')), 'LOCAL');
    });
});
