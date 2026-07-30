export interface SafeSyncCapabilityDecision {
    writable: boolean;
    protocol: 'legacy-ot-v1' | 'read-only';
    reason: string;
}

export function evaluateSafeSyncCapability(
    serverAuthority: string,
    realtimeOtAvailable: boolean,
    advertisedProtocol?: unknown,
): SafeSyncCapabilityDecision {
    if (!realtimeOtAvailable) {
        return {
            writable: false,
            protocol: 'read-only',
            reason: 'The fallback connection does not expose authoritative OT acknowledgements',
        };
    }
    if (serverAuthority === '10.10.5.4:8888' || advertisedProtocol === 'legacy-ot-v1') {
        return {
            writable: true,
            protocol: 'legacy-ot-v1',
            reason: serverAuthority === '10.10.5.4:8888'
                ? 'Validated deployment policy for 10.10.5.4:8888'
                : 'Server advertised hershelSafeSyncProtocol=legacy-ot-v1',
        };
    }
    return {
        writable: false,
        protocol: 'read-only',
        reason: 'This server has not advertised a compatible safe-sync OT protocol',
    };
}
