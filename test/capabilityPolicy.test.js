const assert = require('assert');
const { evaluateSafeSyncCapability } = require('../out/sync/capabilityPolicy');

describe('safe-sync capability policy', () => {
    it('enables realtime OT on any server authority and port', () => {
        for (const authority of [
            '0.0.0.0:12345',
            '192.0.2.10:1',
            '198.51.100.25:65535',
            'overleaf.example.test:8443',
            '[2001:db8::10]:9443',
        ]) {
            assert.strictEqual(
                evaluateSafeSyncCapability(authority, true).writable,
                true,
                authority,
            );
        }
    });

    it('keeps a transport without realtime acknowledgements read-only', () => {
        assert.strictEqual(
            evaluateSafeSyncCapability('192.0.2.10:4321', false).writable,
            false,
        );
    });

    it('rejects an explicitly incompatible protocol advertisement', () => {
        assert.strictEqual(
            evaluateSafeSyncCapability('192.0.2.10:4321', true, 'future-ot-v2').writable,
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
