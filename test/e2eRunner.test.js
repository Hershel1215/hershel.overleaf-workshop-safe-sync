const assert = require('assert');
const Module = require('module');
const path = require('path');
const manifest = require('../package.json');

function loadRunner() {
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return {
                workspace: {
                    getConfiguration: () => ({get: (_key, fallback) => fallback}),
                },
                l10n: {t: value => value},
            };
        }
        if (request === '../scm/localReplicaSCM') {
            return {LocalReplicaSCMProvider: {}};
        }
        return originalLoad.apply(this, arguments);
    };
    try {
        delete require.cache[require.resolve('../out/sync/e2eRunner')];
        return require('../out/sync/e2eRunner');
    } finally {
        Module._load = originalLoad;
    }
}

describe('safe-sync E2E runner lifecycle', () => {
    it('binds reload handoff constants to the packaged release identity', () => {
        const {HANDOFF_TARGET_BUILD_ID, HANDOFF_TARGET_VERSION} = loadRunner();
        const lock = require('../package-lock.json');
        const releaseNumber = manifest.version.match(/\.([0-9]+)$/)[1];
        assert.strictEqual(HANDOFF_TARGET_VERSION, manifest.version);
        assert.strictEqual(HANDOFF_TARGET_BUILD_ID, manifest.hershelBuildId);
        assert.strictEqual(lock.version, manifest.version);
        assert.strictEqual(lock.packages[''].version, manifest.version);
        assert(manifest.hershelBuildId.endsWith(`-rc${releaseNumber}`));
    });

    it('recovers an ambiguously created fixture before retrying the mutation', async () => {
        const {createOrRecoverE2EEntity} = loadRunner();
        let creates = 0;
        let lookups = 0;
        const recovered = {_id: 'folder-1', name: 'move-a'};

        const entity = await createOrRecoverE2EEntity({
            label: 'create move source folder',
            create: async () => {
                creates++;
                return {type: 'error', message: '500: response lost'};
            },
            recover: async () => {
                lookups++;
                return recovered;
            },
            delay: async () => {},
        });

        assert.strictEqual(entity, recovered);
        assert.strictEqual(creates, 1);
        assert.strictEqual(lookups, 1);
    });

    it('bounds fixture-create retries and preserves the remote refusal', async () => {
        const {createOrRecoverE2EEntity} = loadRunner();
        let creates = 0;
        let lookups = 0;

        await assert.rejects(
            createOrRecoverE2EEntity({
                label: 'create move source folder',
                create: async () => {
                    creates++;
                    return {type: 'error', message: '503: overloaded'};
                },
                recover: async () => {
                    lookups++;
                    return undefined;
                },
                attempts: 2,
                delay: async () => {},
            }),
            /create move source folder: 503: overloaded/,
        );
        assert.strictEqual(creates, 2);
        assert.strictEqual(lookups, 2);
    });
    const runner = loadRunner();
    const identity = runner.buildE2EIdentity({
        extension: {
            id: 'hershel.overleaf-workshop-safe-sync',
            packageJSON: {
                version: manifest.version,
                hershelBuildId: manifest.hershelBuildId,
            },
            extensionPath: path.resolve(__dirname, '..'),
        },
    });

    function request(nonce = 'approved_123') {
        return {
            schemaVersion: 2,
            serverName: '10.10.5.4:8888',
            projectName: 'hershel-safe-sync-e2e',
            nonce,
            expectedExtensionId: identity.extensionId,
            expectedExtensionVersion: identity.extensionVersion,
            expectedBuildId: identity.buildId,
            expectedRunnerSha256: identity.runnerSha256,
        };
    }

    it('accepts only the approved server, project, and nonce shape', () => {
        const approved = request();
        assert.deepStrictEqual(runner.validateRequest(approved), approved);
        assert.throws(
            () => runner.validateRequest({...approved, serverName: 'example.com'}),
            /unapproved server/,
        );
        assert.throws(
            () => runner.validateRequest({...approved, projectName: 'manuscript'}),
            /unapproved project/,
        );
        assert.throws(
            () => runner.validateRequest({...approved, nonce: '../unsafe'}),
            /Invalid E2E request nonce/,
        );
    });

    it('records an explicit result schema and extension build identity', () => {
        assert.strictEqual(runner.E2E_RESULT_SCHEMA_VERSION, 12);
        assert.strictEqual(identity.extensionId, 'hershel.overleaf-workshop-safe-sync');
        assert.strictEqual(identity.extensionVersion, manifest.version);
        assert.strictEqual(identity.buildId, manifest.hershelBuildId);
        assert.strictEqual(identity.extensionPath, path.resolve(__dirname, '..'));
        assert.strictEqual(identity.cachedMetadataMatches, true);
        assert.match(identity.runnerSha256, /^[a-f0-9]{64}$/);
        assert.doesNotThrow(() => runner.assertExpectedBuild(request(), identity));
        assert.throws(
            () => runner.assertExpectedBuild({...request(), expectedBuildId: 'another-build'}, identity),
            /build id/,
        );
        assert.strictEqual(manifest.publisher, 'hershel');
        assert.strictEqual(manifest.name, 'overleaf-workshop-safe-sync');
        assert.strictEqual(manifest.version, identity.extensionVersion);
        assert.strictEqual(manifest.hershelBuildId, identity.buildId);
    });

    it('uses the loaded manifest while recording stale VS Code metadata', () => {
        const stale = runner.buildE2EIdentity({
            extension: {
                id: 'hershel.overleaf-workshop-safe-sync',
                packageJSON: {
                    version: '1.0.0-hershel.60',
                    hershelBuildId: 'stale-build',
                },
                extensionPath: path.resolve(__dirname, '..'),
            },
        });
        assert.strictEqual(stale.extensionVersion, manifest.version);
        assert.strictEqual(stale.buildId, manifest.hershelBuildId);
        assert.strictEqual(stale.cachedExtensionVersion, '1.0.0-hershel.60');
        assert.strictEqual(stale.cachedBuildId, 'stale-build');
        assert.strictEqual(stale.cachedMetadataMatches, false);
    });

    it('validates resumable running markers and detects the current owner', () => {
        const marker = {
            schemaVersion: 1,
            request: request('resumable_123'),
            ownerPid: process.pid,
            startedAt: new Date().toISOString(),
        };
        assert.deepStrictEqual(runner.validateRunningMarker(marker), marker);
        assert.strictEqual(runner.isProcessAlive(process.pid), true);
        assert.strictEqual(runner.isProcessAlive(-1), false);
        assert.throws(
            () => runner.validateRunningMarker({...marker, ownerPid: 0}),
            /owner pid/,
        );
    });

    it('validates the same-fixture restart recovery marker', () => {
        const nonce = 'restart_123';
        const marker = {
            schemaVersion: 2,
            requestNonce: nonce,
            preReloadPid: 1234,
            namespace: `hershel-e2e-${nonce}`,
            runFolderId: 'folder-id',
            documentId: 'document-id',
            relativePath: `hershel-e2e-${nonce}/restart-recovery.tex`,
            baseRevision: 7,
            baseSha256: '1'.repeat(64),
            targetSha256: '2'.repeat(64),
            token: `RESTART-JOURNALED-${nonce}\n`,
            pendingOperationId: 'project:document:fixed-operation-id',
            detachedLegacyDocumentId: `hershel-e2e-detached-${nonce}`,
            detachedLegacyOperationId: 'detached-legacy-operation-id',
            detachedLegacyBaseSha256: '3'.repeat(64),
            detachedLegacyVisibleSha256: '4'.repeat(64),
            createdAt: '2026-08-11T00:00:00.000Z',
        };
        assert.deepStrictEqual(runner.validateRestartRecoveryMarker(marker, nonce), marker);
        assert.throws(
            () => runner.validateRestartRecoveryMarker(marker, 'another_nonce'),
            /another E2E request/,
        );
        assert.throws(
            () => runner.validateRestartRecoveryMarker({...marker, pendingOperationId: ''}, nonce),
            /operation id/,
        );
        assert.throws(
            () => runner.validateRestartRecoveryMarker({
                ...marker,
                relativePath: `${marker.namespace}/another.tex`,
            }, nonce),
            /path does not match/,
        );
    });

    it('rejects converged text that duplicates or drops the common base', () => {
        const base = 'COMMON-BASE\n';
        const local = 'LOCAL\n';
        const remote = 'REMOTE\n';
        assert.strictEqual(runner.countExactOccurrences(`${local}${remote}${base}`, base), 1);
        assert.strictEqual(
            runner.isExactComposition(`${local}${remote}${base}`, [base, local, remote]),
            true,
        );
        assert.strictEqual(
            runner.isExactComposition(`${local}${remote}${base}${base}`, [base, local, remote]),
            false,
        );
        assert.strictEqual(
            runner.isExactComposition(`${local}${remote}`, [base, local, remote]),
            false,
        );
        assert.throws(() => runner.countExactOccurrences('content', ''), /empty E2E fragment/);
    });

    it('redacts credential-shaped error fragments and bounds report size', () => {
        const message = runner.safeMessage(
            `cookie=session-secret csrfToken=csrf-secret access_token=access-secret\n`
            + `{"cookie":"json-secret","authorization":"Bearer json-auth"}\n`
            + `Authorization: Bearer header-secret\nCookie: first=one; session=second-secret\n`
            + 'x'.repeat(3000),
        );
        for (const secret of [
            'session-secret', 'csrf-secret', 'access-secret', 'json-secret',
            'json-auth', 'header-secret', 'second-secret',
        ]) {
            assert(!message.includes(secret), `credential leaked: ${secret}`);
        }
        assert(message.includes('[redacted]'));
        assert.strictEqual(message.length, 2000);
        assert.strictEqual(
            runner.safeMessage({error: 'missing', token: 'object-secret'}),
            '{"error":"missing","token":"[redacted]"}',
        );
    });

    it('bounds an unresolved operation without changing successful results', async () => {
        assert.strictEqual(await runner.withTimeout(Promise.resolve('ok'), 50, 'fast'), 'ok');
        await assert.rejects(
            runner.withTimeout(new Promise(() => {}), 10, 'stalled operation'),
            /stalled operation timed out/,
        );
    });

    it('requires complete old-id WAL and replacement provenance in resolved manifests', () => {
        const root = '.overleaf/conflicts/resolved/orphan-wal-test';
        const expected = {
            path: 'hershel-e2e-test/orphan-wal.tex',
            oldDocumentId: 'old-doc-id',
            replacementDocumentId: 'new-doc-id',
            baseSha256: '1'.repeat(64),
            acknowledgedSha256: '1'.repeat(64),
            visibleSha256: '2'.repeat(64),
            replacementSha256: '3'.repeat(64),
        };
        const manifest = {
            schemaVersion: 3,
            path: expected.path,
            reason: 'Recovered journaled text for a replaced document identity',
            resolvedAt: '2026-08-10T00:00:00.000Z',
            baseFile: `${root}/base/${expected.path}`,
            localFile: `${root}/resolution-local/${expected.path}`,
            remoteFile: `${root}/resolution-remote/${expected.path}`,
            baseHash: expected.baseSha256,
            localHash: expected.visibleSha256,
            remoteHash: expected.replacementSha256,
            baseType: 'doc',
            localType: 'doc',
            remoteType: 'doc',
            baselineEntityId: expected.oldDocumentId,
            remoteEntityId: expected.replacementDocumentId,
            remoteRevision: 4,
            documentRecoveries: [{
                entityId: expected.oldDocumentId,
                visibleFile: `${root}/wal-recovery/old/visible/${expected.path}`,
                acknowledgedFile: `${root}/wal-recovery/old/acknowledged/${expected.path}`,
                visibleHash: expected.visibleSha256,
                acknowledgedHash: expected.acknowledgedSha256,
                revision: 3,
                pendingOperationIds: ['op-old-id'],
                epoch: 2,
                snapshotHash: '4'.repeat(64),
            }],
        };
        assert.deepStrictEqual(
            runner.assertResolvedOrphanWalManifest(manifest, expected),
            {
                baseFile: manifest.baseFile,
                localFile: manifest.localFile,
                visibleFile: manifest.documentRecoveries[0].visibleFile,
                remoteFile: manifest.remoteFile,
                acknowledgedFile: manifest.documentRecoveries[0].acknowledgedFile,
                reason: manifest.reason,
                oldRevision: 3,
                replacementRevision: 4,
                pendingOperationCount: 1,
            },
        );
        assert.throws(
            () => runner.assertResolvedOrphanWalManifest({
                ...manifest,
                documentRecoveries: [{
                    ...manifest.documentRecoveries[0],
                    pendingOperationIds: [],
                }],
            }, expected),
            /pending operation ids/,
        );
        assert.throws(
            () => runner.assertResolvedOrphanWalManifest({
                ...manifest,
                remoteEntityId: expected.oldDocumentId,
            }, expected),
            /replacement document id/,
        );
    });
});
