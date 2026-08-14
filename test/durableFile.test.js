const assert = require('assert');
const {mkdtemp, readFile, readdir, rm, writeFile} = require('fs/promises');
const os = require('os');
const path = require('path');
const {writeFileDurably} = require('../out/sync/durableFile');

describe('durable file publication', () => {
    let directory;

    beforeEach(async () => {
        directory = await mkdtemp(path.join(os.tmpdir(), 'hershel-durable-file-'));
    });

    afterEach(async () => {
        await rm(directory, {recursive: true, force: true});
    });

    it('keeps the previous artifact when publication stops before rename', async () => {
        const target = path.join(directory, 'manifest.json');
        await writeFile(target, 'OLD');
        await assert.rejects(
            () => writeFileDurably(target, Buffer.from('NEW'), {
                beforeRename: async () => { throw new Error('simulated crash'); },
            }),
            /simulated crash/,
        );
        assert.strictEqual(await readFile(target, 'utf8'), 'OLD');
        assert.deepStrictEqual(await readdir(directory), ['manifest.json']);
    });

    it('publishes the complete replacement after its durable boundary', async () => {
        const target = path.join(directory, 'manifest.json');
        await writeFileDurably(target, Buffer.from('COMPLETE'));
        assert.strictEqual(await readFile(target, 'utf8'), 'COMPLETE');
    });
});
