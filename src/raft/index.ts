import { OutgoingMessage, RaftNode } from './raftNode';
import { createConnection, createServer } from 'net';
import { Logger } from '../config';
import { NodeMessageCodec, NodeMessageDTO } from './messages';
import { either } from 'fp-ts';
import { NodeMessage } from './state';

// TODO make timeouts ranges

// Class instance will maintain internal state, and respond to things from the world.
// It will manage timers & fire these.
// It will communicate with other raft nodes.
export class Raft<LogValueType> {
    private readonly raftNode: RaftNode<LogValueType>;
    private electionTimeout: NodeJS.Timeout | undefined = undefined;

    private readonly heartbeatTimeouts: Record<number, NodeJS.Timeout> = {};

    // For now, assuming we run on ports on the same machine.
    // That can easily be changed later to ip/port combinations.
    public constructor(
        private readonly nodePort: number,
        otherNodePorts: ReadonlyArray<number>,
        private readonly logger: Logger,
        private readonly slowdownTimeBy: number = 1,
        private readonly leaderElectionTimeoutMs: number = 3000,
        private readonly heartbeatTimeoutMs: number = 500,
    ) {
        this.raftNode = new RaftNode<LogValueType>(
            this.sendMessageToNode,
            this.resetElectionTimeout,
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

    // Resolves when the entry has been committed and is safe to apply.
    // It returns a promise with the values that should be applied to the state machine.
    public addToLog(_value: LogValueType): Promise<Array<LogValueType>> {
        throw new Error('not implemented');
    }

    // This should be called before every read. It resolves when:
    // 1.
    public syncBeforeRead(): Promise<void> {
        throw new Error('not implemented');
    }
}
