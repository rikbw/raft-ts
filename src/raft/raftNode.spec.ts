/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { OutgoingMessage, RaftNode } from './raftNode';
import { createLogger } from 'bunyan';
import { Entry } from './log';

class TestEnvironment {
    public readonly nodes: Array<RaftNode<string>>;
    public readonly committedEntries: Record<number, Array<Entry<string>>> = {};

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
            const noop = () => {
                // noop
            };
            const onEntriesCommitted = (entries: Array<Entry<string>>) => {
                if (this.committedEntries[index] == null) {
                    this.committedEntries[index] = [];
                }

                this.committedEntries[index]!.push(...entries);
            };
            return new RaftNode<string>(
                (message) => this.sendMessage({ message, sender: index }),
                noop,
                onEntriesCommitted,
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
        expect(environment.nodes[0]!.__stateForTests.log.length).toEqual(2);
        expect(environment.nodes[1]!.__stateForTests.log.length).toEqual(2);
        expect(environment.nodes[2]!.__stateForTests.log.length).toEqual(0);

        environment.connect(2);

        environment.nodes[2]!.leaderElectionTimeout();

        // Node 0 should be follower (because it received a response with a request with a higher term) and node 2 be a candidate.
        expect(environment.nodes[0]!.__stateForTests.type).toEqual('follower');
        expect(environment.nodes[2]!.__stateForTests.type).toEqual('candidate');

        // Node 1 can get elected
        environment.nodes[1]!.leaderElectionTimeout();
        expect(environment.nodes[0]!.__stateForTests.type).toEqual('follower');
        expect(environment.nodes[1]!.__stateForTests.type).toEqual('leader');
        expect(environment.nodes[2]!.__stateForTests.type).toEqual('follower');
    });

    it('sets commitIndex when replicating logs to a majority', () => {
        const environment = new TestEnvironment(3);

        // Make node 0 leader
        environment.nodes[0]!.leaderElectionTimeout();

        environment.disconnect(0);

        environment.nodes[0]!.appendToLog('x <- 1');
        environment.nodes[0]!.appendToLog('y <- 2');

        environment.nodes.forEach((node, index) => {
            expect(node.__stateForTests.commitIndex).toEqual(-1);
            expect(environment.committedEntries[index] ?? []).toEqual([]);
        });

        environment.connect(0);
        environment.nodes[0]!.sendHeartbeatTimeoutForNode(1);

        const entries = [
            {
                term: 1,
                value: 'x <- 1',
            },
            {
                term: 1,
                value: 'y <- 2',
            },
        ];

        expect(environment.nodes[0]!.__stateForTests.commitIndex).toEqual(1);
        expect(environment.committedEntries[0]).toEqual(entries);
        expect(environment.nodes[1]!.__stateForTests.commitIndex).toEqual(-1);
        expect(environment.committedEntries[1] ?? []).toEqual([]);
        expect(environment.nodes[2]!.__stateForTests.commitIndex).toEqual(-1);
        expect(environment.committedEntries[2] ?? []).toEqual([]);

        // Second heartbeat to update the commitIndex
        environment.nodes[0]!.sendHeartbeatTimeoutForNode(1);

        expect(environment.nodes[0]!.__stateForTests.commitIndex).toEqual(1);
        expect(environment.committedEntries[0]).toEqual(entries);
        expect(environment.nodes[1]!.__stateForTests.commitIndex).toEqual(1);
        expect(environment.committedEntries[1]).toEqual(entries);
        expect(environment.nodes[2]!.__stateForTests.commitIndex).toEqual(-1);
        expect(environment.committedEntries[2] ?? []).toEqual([]);
    });
});
