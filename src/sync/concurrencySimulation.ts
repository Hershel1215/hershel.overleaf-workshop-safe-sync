import { TextOperation } from './textOperation';
import { OtClientState, OutgoingOperation } from './otClientState';

export type SimulationWriter = 'remote' | 'vscode' | 'ai';

export interface ConcurrencySimulationResult {
    seed: number;
    rounds: number;
    serverRevision: number;
    finalContent: string;
    maxNetworkQueue: number;
    writerOperationCounts: Record<SimulationWriter, number>;
}

interface Packet {
    recipient: SimulationWriter;
    revision: number;
    operation: TextOperation;
    own: boolean;
}

class SimulationServer {
    content: string;
    version = 0;
    readonly history: TextOperation[] = [];
    readonly packets: Packet[] = [];

    constructor(initialContent: string, private readonly clients: Map<SimulationWriter, SimulationClient>) {
        this.content = initialContent;
    }

    submit(sender: SimulationWriter, outgoing: OutgoingOperation): void {
        if (outgoing.revision > this.version) {
            throw new Error(`Simulation received a future operation from ${sender}`);
        }
        let operation = outgoing.operation;
        for (let version = outgoing.revision; version < this.version; version++) {
            const [transformed] = TextOperation.transform(operation, this.history[version]);
            operation = transformed;
        }
        const baseVersion = this.version;
        this.content = operation.apply(this.content);
        this.history.push(operation);
        this.version++;
        for (const [recipient, client] of this.clients) {
            this.packets.push({
                recipient,
                revision: baseVersion,
                operation,
                own: recipient === sender,
            });
            if (client.receiveQueue.length > 2000) {
                throw new Error('Simulation network queue exceeded the safety bound');
            }
        }
    }
}

class SimulationClient {
    readonly state: OtClientState;
    readonly receiveQueue: Packet[] = [];
    operationCount = 0;

    constructor(
        readonly name: SimulationWriter,
        initialContent: string,
        private readonly server: SimulationServer,
        private readonly random: () => number,
    ) {
        this.state = new OtClientState(0, initialContent, initialContent, () => `${name}-${++this.operationCount}`);
    }

    generate(round: number): void {
        const base = this.state.visibleContent;
        const position = base.length === 0 ? 0 : Math.floor(this.random() * (base.length + 1));
        let operation: TextOperation;
        if (base.length > 0 && this.random() < 0.2) {
            const deletePosition = Math.min(position, base.length - 1);
            operation = new TextOperation()
                .retain(deletePosition)
                .remove(1)
                .insert(`${this.name[0].toUpperCase()}${round}`)
                .retain(base.length - deletePosition - 1);
        } else {
            operation = new TextOperation()
                .retain(position)
                .insert(`${this.name[0].toUpperCase()}${round}`)
                .retain(base.length - position);
        }
        const submission = this.state.submitOperation(operation);
        if (submission.send) {
            this.server.submit(this.name, submission.send);
        }
    }

    deliver(packet: Packet): void {
        if (packet.revision < this.state.currentRevision) {
            return; // duplicate delivery after the client caught up
        }
        if (this.receiveQueue.some(queued => queued.revision === packet.revision)) {
            return;
        }
        this.receiveQueue.push(packet);
        this.receiveQueue.sort((left, right) => left.revision - right.revision);
        while (this.receiveQueue[0]?.revision < this.state.currentRevision) {
            this.receiveQueue.shift();
        }
        while (this.receiveQueue.length > 0 && this.receiveQueue[0].revision === this.state.currentRevision) {
            const next = this.receiveQueue.shift()!;
            const result = this.state.receive(next.operation, next.revision, next.own);
            if (result.send) {
                this.server.submit(this.name, result.send);
            }
        }
    }
}

export function runConcurrencyScenario(
    seed: number,
    activeWriters: readonly SimulationWriter[],
    rounds = 120,
): ConcurrencySimulationResult {
    const random = seededRandom(seed);
    const clients = new Map<SimulationWriter, SimulationClient>();
    const server = new SimulationServer('seed', clients);
    for (const name of ['remote', 'vscode', 'ai'] as const) {
        clients.set(name, new SimulationClient(name, server.content, server, random));
    }

    let maxNetworkQueue = 0;
    for (let round = 0; round < rounds; round++) {
        for (const writer of activeWriters) {
            clients.get(writer)!.generate(round);
        }
        const deliveryCount = Math.max(1, Math.floor(random() * 4));
        for (let index = 0; index < deliveryCount && server.packets.length > 0; index++) {
            const packetIndex = Math.floor(random() * server.packets.length);
            const packet = server.packets.splice(packetIndex, 1)[0];
            clients.get(packet.recipient)!.deliver(packet);
            if (random() < 0.08) {
                clients.get(packet.recipient)!.deliver(packet);
            }
        }
        maxNetworkQueue = Math.max(maxNetworkQueue, server.packets.length);
    }

    while (server.packets.length > 0) {
        const packet = server.packets.shift()!;
        clients.get(packet.recipient)!.deliver(packet);
    }
    for (const client of clients.values()) {
        if (client.state.currentRevision !== server.version || client.state.visibleContent !== server.content) {
            throw new Error(`Client ${client.name} did not converge at revision ${server.version}`);
        }
    }
    return {
        seed,
        rounds,
        serverRevision: server.version,
        finalContent: server.content,
        maxNetworkQueue,
        writerOperationCounts: {
            remote: clients.get('remote')!.operationCount,
            vscode: clients.get('vscode')!.operationCount,
            ai: clients.get('ai')!.operationCount,
        },
    };
}

function seededRandom(seed: number): () => number {
    let value = seed >>> 0;
    return () => {
        value = (1664525 * value + 1013904223) >>> 0;
        return value / 0x100000000;
    };
}
