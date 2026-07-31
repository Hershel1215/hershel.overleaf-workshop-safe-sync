#!/usr/bin/env node
import { readFile } from 'fs/promises';
import { sendAiGatewayRequest } from './aiGateway';

export async function runAiGatewayCli(args: readonly string[]): Promise<unknown> {
    const [metadataPath, method, filePath, ...rest] = args;
    if (!metadataPath || !method || !filePath) {
        throw new Error(
            'Usage: aiGatewayCli <metadata.json> <begin|heartbeat|commit|abort> <path> [arguments]',
        );
    }
    switch (method) {
        case 'begin':
            return sendAiGatewayRequest(metadataPath, {
                method,
                path: filePath,
                holder: rest[0] ?? 'ai-agent',
            });
        case 'heartbeat':
        case 'abort': {
            const [leaseId, fencingToken] = rest;
            return sendAiGatewayRequest(metadataPath, {
                method,
                path: filePath,
                leaseId,
                fencingToken: parseFencingToken(fencingToken),
            });
        }
        case 'commit': {
            const [leaseId, fencingToken, contentPath] = rest;
            const content = contentPath
                ? await readFile(contentPath, 'utf8')
                : await readStdin();
            return sendAiGatewayRequest(metadataPath, {
                method,
                path: filePath,
                leaseId,
                fencingToken: parseFencingToken(fencingToken),
                content,
            });
        }
        default:
            throw new Error(`Unsupported AI gateway method: ${method}`);
    }
}

function parseFencingToken(value: string | undefined): number {
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed < 1) {
        throw new Error('A positive fencing token is required');
    }
    return parsed;
}

async function readStdin(): Promise<string> {
    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString('utf8');
}

if (require.main === module) {
    runAiGatewayCli(process.argv.slice(2)).then(result => {
        process.stdout.write(`${JSON.stringify(result)}\n`);
    }).catch(error => {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
        process.exitCode = 1;
    });
}
