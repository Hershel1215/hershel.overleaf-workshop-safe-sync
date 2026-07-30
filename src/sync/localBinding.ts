import { randomUUID } from 'crypto';
import { link, mkdir, open, readFile, rm } from 'fs/promises';
import * as path from 'path';

export interface LocalReplicaBinding {
    uri: string;
    serverName: string;
    enableCompileNPreview: boolean;
    projectName: string;
}

export function createLocalReplicaBinding(
    uri: string,
    serverName: string,
    projectName: string,
): LocalReplicaBinding {
    return {
        uri,
        serverName,
        enableCompileNPreview: false,
        projectName,
    };
}

function projectIdFromUri(uri: string): { serverName: string; projectId?: string } {
    const parsed = new URL(uri);
    if (parsed.protocol !== 'overleaf-workshop:') {
        throw new Error(`Unsupported local replica URI scheme: ${parsed.protocol}`);
    }
    const query = decodeURIComponent(parsed.search.slice(1));
    return {
        serverName: parsed.host,
        projectId: new URLSearchParams(query).get('project') ?? undefined,
    };
}

export function assertLocalReplicaBinding(
    value: unknown,
    expectedServerName: string,
    expectedProjectId: string,
): asserts value is LocalReplicaBinding {
    if (!value || typeof value !== 'object') {
        throw new Error('Local replica binding is not a JSON object');
    }
    const binding = value as Partial<LocalReplicaBinding>;
    if (typeof binding.uri !== 'string' || typeof binding.serverName !== 'string') {
        throw new Error('Local replica binding is missing its URI or server name');
    }
    const target = projectIdFromUri(binding.uri);
    if (
        binding.serverName !== expectedServerName
        || target.serverName !== expectedServerName
        || target.projectId !== expectedProjectId
    ) {
        throw new Error('Local replica directory is already bound to a different Overleaf project');
    }
}

/** Create a binding without overwriting metadata written by another owner. */
export async function ensureLocalReplicaBinding(
    basePath: string,
    binding: LocalReplicaBinding,
    expectedProjectId: string,
): Promise<boolean> {
    const metadataDirectory = path.join(basePath, '.overleaf');
    const settingsPath = path.join(metadataDirectory, 'settings.json');
    try {
        const existing = JSON.parse(await readFile(settingsPath, 'utf8')) as unknown;
        assertLocalReplicaBinding(existing, binding.serverName, expectedProjectId);
        return false;
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
            throw error;
        }
    }

    await mkdir(metadataDirectory, { recursive: true });
    const temporaryPath = `${settingsPath}.${randomUUID()}.tmp`;
    const handle = await open(temporaryPath, 'wx', 0o600);
    try {
        await handle.writeFile(`${JSON.stringify(binding, null, 4)}\n`);
        await handle.sync();
    } finally {
        await handle.close();
    }

    try {
        await link(temporaryPath, settingsPath);
        return true;
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
            throw error;
        }
        const existing = JSON.parse(await readFile(settingsPath, 'utf8')) as unknown;
        assertLocalReplicaBinding(existing, binding.serverName, expectedProjectId);
        return false;
    } finally {
        await rm(temporaryPath, { force: true });
    }
}
