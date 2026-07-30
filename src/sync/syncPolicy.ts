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
    return normalized === '.overleaf'
        || normalized.startsWith('.overleaf/');
}

export function shouldIgnore(path: string, patterns: readonly string[] = DEFAULT_IGNORE_PATTERNS): boolean {
    if (isHardIgnored(path)) {
        return true;
    }
    const normalized = normalizeRelativePath(path);
    return patterns.some(pattern => minimatch(normalized, pattern, { dot: true }));
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
