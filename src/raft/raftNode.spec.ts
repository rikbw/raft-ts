/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { OutgoingMessage, RaftNode } from './raftNode';
import { createLogger } from 'bunyan';

class TestEnvironment {
    public readonly nodes: Array<RaftNode<string>>;

    private disconnectedNodes: Set<number> = new Set();

    private readonly logger = createLogger({
        name: 'Test environment',
        level: 'debug',
    });

    public constructor(nbNodes: number) {
        const allNodes = Array(nbNodes)
            .fill(null)
            .map((_, index) => index);

        this.nodes = allNodes.map((index) => {
            const logger = createLogger({
                name: `node ${index}`,
                level: 'debug',
            });
            const otherNodes = allNodes.filter((id) => id != index);
            const resetElectionTimeout = () => {
                // noop
            };
            return new RaftNode<string>(
                (message) => this.sendMessage({ message, sender: index }),
                resetElectionTimeout,
                logger,
                otherNodes,
            );
        });
    }

    public disconnect(node: number) {
        this.disconnectedNodes.add(node);
    }

    public connect(node: number) {
        this.disconnectedNodes.delete(node);
    }

    private readonly sendMessage = ({
        message,
        sender,
    }: {
        message: OutgoingMessage<string>;
        sender: number;
    }) => {
        const { receiver, ...rest } = message;

        if (this.disconnectedNodes.has(receiver)) {
            this.logger.debug(
                'Not sending message to node because it is disconnected',
                { receiver },
            );
            return;
        }

        if (this.disconnectedNodes.has(sender)) {
            this.logger.debug(
                'Not sending message from node because it is disconnected',
                { sender },
            );
            return;
        }

        const sentMessage = {
            ...rest,
            sender,
        };
        this.nodes[receiver]!.receiveMessage(sentMessage);
    };
}

describe('RaftNode', () => {
    it('syncs the log with follower nodes', () => {
        const environment = new TestEnvironment(3);

        environment.nodes[0]!.leaderElectionTimeout();

        expect(environment.nodes[0]!.__stateForTests.type).toEqual('leader');

        environment.nodes[0]!.appendToLog('x <- 1');
        environment.nodes[0]!.appendToLog('y <- 2');

        environment.nodes.forEach((node) => {
            expect(node.__stateForTests.log.getEntries()).toEqual([
                {
                    term: 1,
                    value: 'x <- 1',
                },
                {
                    term: 1,
                    value: 'y <- 2',
                },
            ]);
        });
    });

    it('does not elect nodes that do not have a complete log (5.4.1 in paper)', () => {
        const environment = new TestEnvironment(3);

        environment.nodes[0]!.leaderElectionTimeout();

        environment.disconnect(2);

        environment.nodes[0]!.appendToLog('x <- 1');
        environment.nodes[0]!.appendToLog('y <- 2');

        // Node 0 and 1 have the log, node 2 has nothing.
        expect(
            environment.nodes[0]!.__stateForTests.log.getEntries().length,
        ).toEqual(2);
        expect(
            environment.nodes[1]!.__stateForTests.log.getEntries().length,
        ).toEqual(2);
        expect(
            environment.nodes[2]!.__stateForTests.log.getEntries().length,
        ).toEqual(0);

        environment.connect(2);

        environment.nodes[2]!.leaderElectionTimeout();

        // Node 0 should still be leader and node 2 be a follower.
        expect(environment.nodes[0]!.__stateForTests.type).toEqual('leader');
        expect(environment.nodes[2]!.__stateForTests.type).toEqual('candidate');
    });
});
