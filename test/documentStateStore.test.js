const assert = require('assert');
const Module = require('module');
const {
    mkdir,
    mkdtemp,
    readFile,
    rename,
    rm,
    unlink,
    writeFile,
} = require('fs/promises');
const os = require('os');
const path = require('path');

class TestUri {
    constructor(fsPath) {
        this.scheme = 'file';
        this.fsPath = fsPath;
    }

    static file(fsPath) {
        return new TestUri(fsPath);
    }

    static joinPath(base, ...parts) {
        return new TestUri(path.join(base.fsPath, ...parts));
    }
}

class TestFileSystemError extends Error {
    constructor(code) {
        super(code);
        this.code = code;
    }
}

function loadStore() {
    const vscode = {
        Uri: TestUri,
        FileSystemError: TestFileSystemError,
        FileType: {File: 1, Directory: 2},
        workspace: {
            fs: {
                createDirectory: uri => mkdir(uri.fsPath, {recursive: true}),
                readFile: async uri => {
                    try {
                        return await readFile(uri.fsPath);
                    } catch (error) {
                        if (error.code === 'ENOENT') {
                            throw new TestFileSystemError('FileNotFound');
                        }
                        throw error;
                    }
                },
                readDirectory: async uri => {
                    try {
                        const entries = await require('fs/promises').readdir(
                            uri.fsPath,
                            {withFileTypes: true},
                        );
                        return entries.map(entry => [
                            entry.name,
                            entry.isDirectory() ? 2 : 1,
                        ]);
                    } catch (error) {
                        if (error.code === 'ENOENT') {
                            throw new TestFileSystemError('FileNotFound');
                        }
                        throw error;
                    }
                },
                writeFile: (uri, content) => writeFile(uri.fsPath, content),
                rename: (source, target) => rename(source.fsPath, target.fsPath),
                delete: async uri => {
                    try {
                        await unlink(uri.fsPath);
                    } catch (error) {
                        if (error.code === 'ENOENT') {
                            throw new TestFileSystemError('FileNotFound');
                        }
                        throw error;
                    }
                },
            },
        },
    };
    const originalLoad = Module._load;
    Module._load = function(request) {
        if (request === 'vscode') {
            return vscode;
        }
        return originalLoad.apply(this, arguments);
    };
    try {
        const modulePath = require.resolve('../out/sync/documentStateStore');
        delete require.cache[modulePath];
        return require(modulePath).DocumentStateStore;
    } finally {
        Module._load = originalLoad;
    }
}

describe('legacy document state store', () => {
    let root;

    beforeEach(async () => {
        root = await mkdtemp(path.join(os.tmpdir(), 'hershel-legacy-state-'));
    });

    afterEach(async () => {
        await rm(root, {recursive: true, force: true});
    });

    it('removes published state and remains idempotent after the durable boundary', async () => {
        const DocumentStateStore = loadStore();
        const store = new DocumentStateStore(
            {globalStorageUri: TestUri.file(root)},
            'server',
            'project',
        );
        const state = {
            schemaVersion: 1,
            revision: 0,
            serverContent: 'BASE',
            localContent: 'BASE',
        };
        await store.save('doc-1', state);
        assert.strictEqual((await store.load('doc-1')).state.serverContent, 'BASE');

        await store.remove('doc-1');
        assert.strictEqual(await store.load('doc-1'), undefined);
        await store.remove('doc-1');

        const files = [];
        async function collect(directory) {
            const entries = await require('fs/promises').readdir(directory, {withFileTypes: true});
            for (const entry of entries) {
                const item = path.join(directory, entry.name);
                if (entry.isDirectory()) {
                    await collect(item);
                } else {
                    files.push(item);
                }
            }
        }
        await collect(root);
        assert.deepStrictEqual(files, []);
    });

    it('enumerates every canonical legacy state for startup recovery', async () => {
        const DocumentStateStore = loadStore();
        const store = new DocumentStateStore(
            {globalStorageUri: TestUri.file(root)},
            'server',
            'project',
        );
        await store.save('doc-b', {
            schemaVersion: 1,
            revision: 2,
            serverContent: 'B',
            localContent: 'B-local',
            outstanding: {ids: ['op-b'], operation: [
                {kind: 'retain', count: 1},
                {kind: 'insert', text: '-local'},
            ]},
        });
        await store.save('doc-a', {
            schemaVersion: 1,
            revision: 1,
            serverContent: 'A',
            localContent: 'A',
        });

        const states = await store.listExisting();
        assert.deepStrictEqual(states.map(state => state.documentId), ['doc-a', 'doc-b']);
        assert.deepStrictEqual(states[1].state.outstanding.ids, ['op-b']);
    });

    it('fails closed when a legacy state filename does not bind its document id', async () => {
        const DocumentStateStore = loadStore();
        const store = new DocumentStateStore(
            {globalStorageUri: TestUri.file(root)},
            'server',
            'project',
        );
        await store.save('doc-a', {
            schemaVersion: 1,
            revision: 0,
            serverContent: 'A',
            localContent: 'A',
        });
        const directory = path.dirname((await findSingleJson(root)));
        await writeFile(path.join(directory, 'forged.json'), JSON.stringify({
            schemaVersion: 1,
            documentId: 'doc-forged',
            state: {
                schemaVersion: 1,
                revision: 0,
                serverContent: 'X',
                localContent: 'X-local',
            },
        }));

        await assert.rejects(store.listExisting(), /Invalid legacy durable document state/);
    });
});

async function findSingleJson(root) {
    let found;
    async function visit(directory) {
        const entries = await require('fs/promises').readdir(directory, {withFileTypes: true});
        for (const entry of entries) {
            const item = path.join(directory, entry.name);
            if (entry.isDirectory()) {
                await visit(item);
            } else if (entry.name.endsWith('.json')) {
                found = item;
            }
        }
    }
    await visit(root);
    return found;
}
