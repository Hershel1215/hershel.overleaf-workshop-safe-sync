const assert = require('assert');
const { evaluateSafeSyncCapability } = require('../out/sync/capabilityPolicy');

describe('safe-sync capability policy', () => {
    it('enables the explicitly validated current deployment', () => {
        assert.strictEqual(
            evaluateSafeSyncCapability('10.10.5.4:8888', true).writable,
            true,
        );
    });

    it('keeps fallback and unadvertised servers read-only', () => {
        assert.strictEqual(
            evaluateSafeSyncCapability('10.10.5.4:8888', false).writable,
            false,
        );
        assert.strictEqual(
            evaluateSafeSyncCapability('other.example', true).writable,
            false,
        );
    });

    it('accepts an explicit compatible protocol advertisement', () => {
        assert.strictEqual(
            evaluateSafeSyncCapability('other.example', true, 'legacy-ot-v1').writable,
            true,
        );
    });
});
