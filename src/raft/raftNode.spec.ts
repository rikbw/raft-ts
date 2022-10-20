/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { OutgoingMessage, RaftNode } from './raftNode';
import { State } from './state';
import { Log } from './log';
import { createLogger } from 'bunyan';

class TestEnvironment {
    public readonly nodes: Array<RaftNode<string>>;

    public constructor(nodeStates: State<string>[]) {
        this.nodes = nodeStates.map((nodeState, index) => {
            const logger = createLogger({
                name: `node ${index}`,
                level: 'debug',
            });
            return new RaftNode<string>(
                (message) => this.sendMessage({ message, sender: index }),
                logger,
                nodeState,
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

    public get leader(): RaftNode<string> {
        return this.nodes[0]!;
    }
}

describe('RaftNode', () => {
    it('syncs the log with follower nodes', () => {
        const logEntries = [
            {
                term: 0,
                value: 'x <- 1',
            },
            {
                term: 0,
                value: 'y <- 2',
            },
        ];
        const leaderLog = new Log(logEntries);
        const leaderState: State<string> = {
            type: 'leader',
            currentTerm: 1,
            log: leaderLog,
            followerInfo: {},
        };
        const followerState: State<string> = {
            type: 'follower',
            currentTerm: 0,
            log: new Log([]),
        };
        const environment = new TestEnvironment([leaderState, followerState]);

        environment.leader.sendHeartbeatTimeoutForNode(1);

        const [leader, follower] = environment.nodes;

        expect(leader!.__stateForTests.log.getEntries()).toEqual(logEntries);
        expect(follower!.__stateForTests.log.getEntries()).toEqual(logEntries);
    });
});
