import { createHash } from 'crypto';
import { minimatch } from 'minimatch';

export const CONFLICT_DIRECTORY = '.overleaf/conflicts';
export const MAX_EDITABLE_TEXT_BYTES = 2 * 1024 * 1024;

export const DEFAULT_IGNORE_PATTERNS = [
    '**/*.aux',
    '**/__latexindent*',
    '**/*.bbl',
    '**/*.bcf',
    '**/*.blg',
    '**/*.fdb_latexmk',
    '**/*.fls',
    '**/.git',
    '**/.git/**',
    '**/*.lof',
    '**/*.log',
    '**/*.lot',
    '**/*.out',
    '**/*.run.xml',
    '**/*.synctex(busy)',
    '**/*.synctex.gz',
    '**/*.toc',
    '**/*.xdv',
    '**/.output',
    '**/.output/**',
    '**/main.pdf',
    '**/output.pdf',
];

const TEXT_EXTENSIONS = new Set([
    '.bbx', '.bib', '.cbx', '.cls', '.csv', '.json', '.latex', '.md',
    '.sty', '.tex', '.txt', '.yaml', '.yml',
]);

export function normalizeRelativePath(path: string): string {
    return path.replace(/\\/g, '/').replace(/^\/+/, '').replace(/\/$/, '');
}

export function isHardIgnored(path: string): boolean {
    const normalized = normalizeRelativePath(path);
    const topLevel = normalized.split('/')[0];
    return normalized === '.overleaf'
        || normalized.startsWith('.overleaf/')
        || topLevel === '.hershel-safe-sync'
        || /^\.hershel-safe-sync\.staging-[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
            .test(topLevel);
}

export function shouldIgnore(path: string, patterns: readonly string[] = DEFAULT_IGNORE_PATTERNS): boolean {
    if (isHardIgnored(path)) {
        return true;
    }
    const normalized = normalizeRelativePath(path);
    return patterns.some(pattern => minimatch(normalized, pattern, { dot: true }));
}

export const TRUNCATION_CONFIRM_MS = 40;
export const TRUNCATION_CONFIRMATIONS = 4;

/**
 * Read a document past a rewrite-in-place truncation window.
 *
 * A tool that rewrites a file truncates it before writing the new bytes, so a
 * watcher can read zero bytes from a document nobody emptied. Those bytes are
 * not a state anybody authored: journaling them deletes the document remotely
 * and republishes it a moment later, taking with it every concurrent writer's
 * text that crossed the window. Only emptiness that holds still is a real
 * truncation, so confirm it before believing it.
 */
export async function readPastTruncationWindow(options: {
    read: () => Promise<string | undefined>;
    previousContent: string | undefined;
    wait: (milliseconds: number) => Promise<void>;
    isCurrent?: () => boolean;
    confirmations?: number;
    confirmMs?: number;
}): Promise<string | undefined> {
    let content = await options.read();
    if ((options.previousContent ?? '') === '') {
        // A document that was already empty cannot be caught mid-rewrite by
        // reading it empty again, and delaying it would only slow its first
        // real content down.
        return content;
    }
    const confirmations = options.confirmations ?? TRUNCATION_CONFIRMATIONS;
    const confirmMs = options.confirmMs ?? TRUNCATION_CONFIRM_MS;
    for (let attempt = 0; content === '' && attempt < confirmations; attempt++) {
        if (options.isCurrent?.() === false) {
            return content;
        }
        await options.wait(confirmMs);
        content = await options.read();
    }
    return content;
}

export function contentHash(content: Uint8Array): string {
    return createHash('sha256').update(content).digest('hex');
}

export function isEditableTextCandidate(path: string, content: Uint8Array): boolean {
    if (content.byteLength > MAX_EDITABLE_TEXT_BYTES || content.includes(0)) {
        return false;
    }
    const normalized = normalizeRelativePath(path).toLowerCase();
    const dot = normalized.lastIndexOf('.');
    const extension = dot >= 0 ? normalized.slice(dot) : '';
    if (!TEXT_EXTENSIONS.has(extension)) {
        return false;
    }
    try {
        new TextDecoder('utf-8', { fatal: true }).decode(content);
        return true;
    } catch {
        return false;
    }
}
