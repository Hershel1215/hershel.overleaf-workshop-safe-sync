const assert = require('assert');
const Module = require('module');
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
    const runner = loadRunner();
    const identity = runner.buildE2EIdentity({
        extension: {
            id: 'hershel.overleaf-workshop-safe-sync',
            packageJSON: {
                version: '1.0.0-hershel.30',
                hershelBuildId: '20260731T145150Z-safe-sync-v2-rc30',
            },
            extensionPath: '/tmp/hershel-extension',
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
        assert.strictEqual(runner.E2E_RESULT_SCHEMA_VERSION, 4);
        assert.strictEqual(identity.extensionId, 'hershel.overleaf-workshop-safe-sync');
        assert.strictEqual(identity.extensionVersion, '1.0.0-hershel.30');
        assert.strictEqual(identity.buildId, '20260731T145150Z-safe-sync-v2-rc30');
        assert.strictEqual(identity.extensionPath, '/tmp/hershel-extension');
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
});
