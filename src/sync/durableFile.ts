import { randomUUID } from 'crypto';
import { mkdir, open, rename, rm, stat } from 'fs/promises';
import * as path from 'path';

export interface DurableFileOptions {
    /** Deterministic crash-boundary injection for tests. */
    beforeRename?: () => Promise<void>;
}

/** Atomically publish bytes only after both data and parent metadata are durable. */
export async function writeFileDurably(
    filePath: string,
    content: Uint8Array,
    options: DurableFileOptions = {},
): Promise<void> {
    const directory = path.dirname(filePath);
    await createDirectoryDurably(directory, 0o700);
    const temporary = path.join(directory, `.${path.basename(filePath)}.${randomUUID()}.tmp`);
    const handle = await open(temporary, 'wx', 0o600);
    try {
        await handle.writeFile(content);
        await handle.sync();
    } finally {
        await handle.close();
    }
    try {
        await options.beforeRename?.();
        await rename(temporary, filePath);
        await syncDirectoryDurably(directory);
    } catch (error) {
        await rm(temporary, {force: true}).catch(() => {});
        throw error;
    }
}

/**
 * Create every missing path component and durably publish each new directory
 * entry before proceeding to its child.
 */
export async function createDirectoryDurably(
    directory: string,
    mode = 0o777,
): Promise<void> {
    const missing: string[] = [];
    let current = path.resolve(directory);
    while (true) {
        try {
            const currentStat = await stat(current);
            if (!currentStat.isDirectory()) {
                throw new Error(`Durable directory path is not a directory: ${current}`);
            }
            break;
        } catch (error) {
            if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') {
                throw error;
            }
            missing.push(current);
            const parent = path.dirname(current);
            if (parent === current) {
                throw error;
            }
            current = parent;
        }
    }

    for (const child of missing.reverse()) {
        try {
            await mkdir(child, {recursive: false, mode});
        } catch (error) {
            if ((error as NodeJS.ErrnoException)?.code !== 'EEXIST') {
                throw error;
            }
            const childStat = await stat(child);
            if (!childStat.isDirectory()) {
                throw error;
            }
        }
        await syncDirectoryDurably(child);
        await syncDirectoryDurably(path.dirname(child));
    }
}

export async function syncDirectoryDurably(directory: string): Promise<void> {
    const handle = await open(directory, 'r');
    try {
        await handle.sync();
    } finally {
        await handle.close();
    }
}
