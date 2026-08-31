const assert = require('assert');
const { mkdtemp, readFile, rm, writeFile, mkdir } = require('fs/promises');
const os = require('os');
const path = require('path');
const { createLocalReplicaBinding, ensureLocalReplicaBinding } = require('../out/sync/localBinding');

describe('local replica binding', () => {
    let root;
    const binding = {
        uri: 'overleaf-workshop://10.23.45.67:4567/test?user%3Duser-1%26project%3Dproject-1',
        serverName: '10.23.45.67:4567',
        enableCompileNPreview: false,
        projectName: 'test',
    };

    beforeEach(async () => {
        root = await mkdtemp(path.join(os.tmpdir(), 'hershel-binding-'));
    });

    afterEach(async () => {
        await rm(root, { recursive: true, force: true });
    });

    it('creates the missing local binding metadata', async () => {
        const generated = createLocalReplicaBinding(binding.uri, binding.serverName, binding.projectName);
        assert.deepStrictEqual(generated, binding);
        assert.strictEqual(await ensureLocalReplicaBinding(root, generated, 'project-1'), true);
        const stored = JSON.parse(await readFile(path.join(root, '.overleaf/settings.json'), 'utf8'));
        assert.deepStrictEqual(stored, binding);
    });

    it('preserves an existing matching binding', async () => {
        const existing = { ...binding, enableCompileNPreview: true, retained: 'value' };
        await mkdir(path.join(root, '.overleaf'), { recursive: true });
        await writeFile(path.join(root, '.overleaf/settings.json'), JSON.stringify(existing));

        assert.strictEqual(await ensureLocalReplicaBinding(root, binding, 'project-1'), false);
        const stored = JSON.parse(await readFile(path.join(root, '.overleaf/settings.json'), 'utf8'));
        assert.deepStrictEqual(stored, existing);
    });

    it('refuses to reuse a directory bound to another project', async () => {
        await mkdir(path.join(root, '.overleaf'), { recursive: true });
        await writeFile(path.join(root, '.overleaf/settings.json'), JSON.stringify({
            ...binding,
            uri: 'overleaf-workshop://10.23.45.67:4567/other?user%3Duser-1%26project%3Dproject-2',
        }));

        await assert.rejects(
            () => ensureLocalReplicaBinding(root, binding, 'project-1'),
            /already bound to a different Overleaf project/,
        );
    });
});
