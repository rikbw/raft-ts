/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { OutgoingMessage, RaftNode } from './raftNode';
import { createLogger } from 'bunyan';

class TestEnvironment {
    public readonly nodes: Array<RaftNode<string>>;

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

    private readonly sendMessage = ({
        message,
        sender,
    }: {
        message: OutgoingMessage<string>;
        sender: number;
    }) => {
        const { receiver, ...rest } = message;
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
});
