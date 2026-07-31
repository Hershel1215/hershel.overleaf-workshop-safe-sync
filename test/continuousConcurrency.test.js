const assert = require('assert');
const { runConcurrencyScenario } = require('../out/sync/concurrencySimulation');

describe('continuous writer convergence scenarios', () => {
    const scenarios = [
        ['remote continuous against local and AI observers', ['remote']],
        ['VS Code continuous against remote and AI observers', ['vscode']],
        ['AI continuous against remote and VS Code observers', ['ai']],
        ['remote, VS Code, and one AI writer concurrent', ['remote', 'vscode', 'ai']],
    ];

    for (const [name, writers] of scenarios) {
        it(name, () => {
            for (const seed of [11, 29, 47, 83]) {
                const result = runConcurrencyScenario(seed, writers, 120);
                assert(result.serverRevision > 0);
                assert(result.maxNetworkQueue < 2000);
                assert.strictEqual(result.writerOperationCounts.ai > 0, writers.includes('ai'));
                assert.strictEqual(result.writerOperationCounts.vscode > 0, writers.includes('vscode'));
                assert.strictEqual(result.writerOperationCounts.remote > 0, writers.includes('remote'));
            }
        });
    }
});
