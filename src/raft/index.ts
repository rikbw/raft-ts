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

// TODO make timeouts ranges

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

    // For now, assuming we run on ports on the same machine.
    // That can easily be changed later to ip/port combinations.
    public constructor(
        private readonly nodePort: number,
        otherNodePorts: ReadonlyArray<number>,
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
            this.leaderElectionTimeoutMs * this.slowdownTimeBy,
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
        const serializedRequestId = serializeRequestId(entry.id);
        const resolvePendingWriter =
            this.pendingWritersForRequestId.get(serializedRequestId);
        if (resolvePendingWriter != null) {
            resolvePendingWriter();
            this.pendingWritersForRequestId.delete(serializedRequestId);
        }
    }

    private applyToStateMachine(entry: Entry<LogValueType>) {
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
    public addToLog(
        value: LogValueType,
        requestId: RequestId,
    ): Promise<boolean> {
        this.raftNode.appendToLog(value, requestId);

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

        return Promise.race([waitForLogToBeCommitted, timeout]);
    }

    // This should be called before every read. It resolves when:
    // 1.
    public syncBeforeRead(): Promise<void> {
        throw new Error('not implemented');
    }
}
