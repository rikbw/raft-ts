import { OutgoingMessage, RaftNode } from './raftNode';
import { createConnection, createServer } from 'net';
import { Logger } from '../config';
import { NodeMessageCodec, NodeMessageDTO } from './messages';
import { either } from 'fp-ts';
import { NodeMessage } from './state';
import { Entry, RequestId } from './log';

const serializeRequestId = ({ clientId, requestSerial }: RequestId) =>
    `${clientId}-${requestSerial}`;

export type StateMachine<LogValueType> = {
    handleValue(value: LogValueType): void;
};

// Class instance will maintain internal state, and respond to things from the world.
// It will manage timers & fire these.
// It will communicate with other raft nodes.
export class Raft<LogValueType> {
    private readonly raftNode: RaftNode<LogValueType>;
    private electionTimeout: NodeJS.Timeout | undefined = undefined;

    private readonly heartbeatTimeouts: Record<number, NodeJS.Timeout> = {};

    private readonly pendingWritersForRequestId: Map<string, () => void> =
        new Map();

    private readonly serialProcessedPerClientId: Map<number, number> =
        new Map();

    private heartBeatsReceivedFromClientsForReadRequest: Set<{
        heartBeatsFromNodes: Set<number>;
        callbackWhenReceivedHeartBeatFromMajority: () => void;
    }> = new Set();

    // For now, assuming we run on ports on the same machine.
    // That can easily be changed later to ip/port combinations.
    public constructor(
        private readonly nodePort: number,
        private readonly otherNodePorts: ReadonlyArray<number>,
        private readonly stateMachine: StateMachine<LogValueType>,
        private readonly logger: Logger,
        private readonly slowdownTimeBy: number = 1,
        private readonly leaderElectionTimeoutMs: number = 3000,
        private readonly heartbeatTimeoutMs: number = 500,
    ) {
        this.raftNode = new RaftNode<LogValueType>(
            this.sendMessageToNode,
            this.resetElectionTimeout,
            this.onEntriesCommitted,
            logger,
            otherNodePorts,
        );

        const server = createServer((client) => {
            client.setEncoding('utf-8');
            client.on('data', (data) => {
                const dataString = data.toString();
                const message = JSON.parse(dataString);
                const decodedMessage = NodeMessageCodec.decode(message);
                if (either.isLeft(decodedMessage)) {
                    throw new Error('received invalid message');
                }
                const sender = decodedMessage.right.responsePort;

                this.handleMessage(decodedMessage.right, sender);
            });
        });

        server.listen(nodePort);

        this.resetElectionTimeout();
    }

    private handleMessage(message: NodeMessage<LogValueType>, sender: number) {
        const incomingMessage = {
            sender,
            ...message,
        };

        if (message.type === 'appendEntriesResponse') {
            this.onReceiveHeartbeatFromNode(sender);
        }

        this.raftNode.receiveMessage(incomingMessage);
    }

    private sendMessageToNode = (message: OutgoingMessage<LogValueType>) => {
        if (message.type === 'appendEntries') {
            this.resetHeartbeatTimeout(message.receiver);
        }

        const { receiver, ...restOfMessage } = message;
        const messageDto: NodeMessageDTO = {
            ...restOfMessage,
            responsePort: this.nodePort,
        };
        this.sendMessageToPort(messageDto, receiver);
    };

    private sendMessageToPort(message: NodeMessageDTO, port: number) {
        const client = createConnection(
            {
                port,
                timeout: 1000,
            },
            () => {
                const data = JSON.stringify(message);
                client.setEncoding('utf-8');
                client.write(data);
                client.end();
            },
        );

        client.on('timeout', () => {
            this.logger.warn('Timed out sending message to node', {
                port,
            });
            client.end();
        });

        client.on('error', (error) => {
            this.logger.error('failed to send message to node', { error });
        });
    }

    private resetElectionTimeout = () => {
        clearTimeout(this.electionTimeout);
        this.electionTimeout = setTimeout(
            () => this.raftNode.leaderElectionTimeout(),
            this.leaderElectionTimeoutMs *
                (Math.random() + 1) *
                this.slowdownTimeBy,
        );
    };

    private resetHeartbeatTimeout = (nodePort: number) => {
        clearTimeout(this.heartbeatTimeouts[nodePort]);
        this.heartbeatTimeouts[nodePort] = setTimeout(
            () => this.raftNode.sendHeartbeatTimeoutForNode(nodePort),
            this.heartbeatTimeoutMs * this.slowdownTimeBy,
        );
    };

    private onEntriesCommitted = (entries: Array<Entry<LogValueType>>) => {
        entries.forEach((entry) => {
            this.resolvePendingWriter(entry);
            this.applyToStateMachine(entry);
        });
    };

    private resolvePendingWriter(entry: Entry<LogValueType>) {
        if (entry.type === 'noop') {
            return;
        }

        const serializedRequestId = serializeRequestId(entry.id);
        const resolvePendingWriter =
            this.pendingWritersForRequestId.get(serializedRequestId);
        if (resolvePendingWriter != null) {
            resolvePendingWriter();
            this.pendingWritersForRequestId.delete(serializedRequestId);
        }
    }

    private applyToStateMachine(entry: Entry<LogValueType>) {
        if (entry.type === 'noop') {
            return;
        }

        const { id } = entry;
        const { clientId, requestSerial } = id;

        const highestProcessedSerialForClient =
            this.serialProcessedPerClientId.get(clientId) ?? -1;
        if (requestSerial <= highestProcessedSerialForClient) {
            // Skip, this entry has already been applied to the state machine.
            return;
        }

        this.serialProcessedPerClientId.set(clientId, requestSerial);
        this.stateMachine.handleValue(entry.value);
    }

    // Resolves
    // - with true when the entry has been committed and is safe to apply.
    // - with false when it timed out. The entry can be committed in the future.
    public async addToLog(
        value: LogValueType,
        requestId: RequestId,
    ): Promise<either.Either<'notLeader' | 'timedOut', undefined>> {
        const { isLeader } = this.raftNode.appendToLog(value, requestId);

        if (!isLeader) {
            return either.left('notLeader');
        }

        const waitForLogToBeCommitted: Promise<boolean> = new Promise(
            (resolve) => {
                const serializedRequestId = serializeRequestId(requestId);
                const pendingWriter = () => resolve(true);
                this.pendingWritersForRequestId.set(
                    serializedRequestId,
                    pendingWriter,
                );
            },
        );

        const timeout: Promise<boolean> = new Promise((resolve) =>
            setTimeout(() => resolve(false), 10000),
        );

        const ok = await Promise.race([waitForLogToBeCommitted, timeout]);

        if (!ok) {
            return either.left('timedOut');
        }

        return either.right(undefined);
    }

    private onReceiveHeartbeatFromNode = (node: number) => {
        const toDelete: Array<{
            heartBeatsFromNodes: Set<number>;
            callbackWhenReceivedHeartBeatFromMajority: () => void;
        }> = [];

        this.heartBeatsReceivedFromClientsForReadRequest.forEach((entry) => {
            const {
                callbackWhenReceivedHeartBeatFromMajority,
                heartBeatsFromNodes,
            } = entry;

            heartBeatsFromNodes.add(node);

            if (heartBeatsFromNodes.size >= this.otherNodePorts.length / 2) {
                callbackWhenReceivedHeartBeatFromMajority();
                toDelete.push(entry);
            }
        });

        toDelete.forEach((entry) =>
            this.heartBeatsReceivedFromClientsForReadRequest.delete(entry),
        );
    };

    private waitUntilReceivedHeartBeatFromMajority(): Promise<void> {
        return new Promise((resolve) => {
            this.heartBeatsReceivedFromClientsForReadRequest.add({
                heartBeatsFromNodes: new Set(),
                callbackWhenReceivedHeartBeatFromMajority: resolve,
            });
        });
    }

    // This should be called before every read. It resolves to true when:
    // 1. This Raft node is the leader.
    // 2. It has committed at least one entry in this term.
    // 3. It has exchanged heartbeats with a majority of the nodes in the cluster.
    public async syncBeforeRead(): Promise<{ isLeader: boolean }> {
        const isLeaderAndHasCommittedAtLeastOneEntryThisTerm =
            await this.raftNode.isLeaderAndCommittedAtLeastOneEntryThisTerm();
        if (!isLeaderAndHasCommittedAtLeastOneEntryThisTerm) {
            return { isLeader: false };
        }

        await this.waitUntilReceivedHeartBeatFromMajority();
        return { isLeader: true };
    }
}
