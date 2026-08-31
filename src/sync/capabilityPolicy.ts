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
    if (advertisedProtocol !== undefined && advertisedProtocol !== 'legacy-ot-v1') {
        return {
            writable: false,
            protocol: 'read-only',
            reason: `Server advertised an incompatible safe-sync protocol: ${String(advertisedProtocol)}`,
        };
    }
    return {
        writable: true,
        protocol: 'legacy-ot-v1',
        reason: advertisedProtocol === 'legacy-ot-v1'
            ? 'Server advertised hershelSafeSyncProtocol=legacy-ot-v1'
            : `Realtime legacy OT acknowledgements are available at ${serverAuthority}`,
    };
}
