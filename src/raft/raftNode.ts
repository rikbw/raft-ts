import {
    getInitialState,
    reduce,
    State,
    Event,
    Effect,
    NodeMessage,
} from './state';
import { Entry, Log, RequestId } from './log';
import { unreachable } from '../util/unreachable';
import { createLogger } from 'bunyan';
import { readEntries, writeEntries } from './persistence';
import Immutable from 'seamless-immutable';

type Logger = ReturnType<typeof createLogger>;

export type IncomingMessage<LogValueType> = NodeMessage<LogValueType> & {
    sender: number;
};

export type OutgoingMessage<LogValueType> = NodeMessage<LogValueType> & {
    receiver: number;
};

const noop = () => {
    // noop
};

// Wrapper on top of State & reducer, to give it an easier API to work with in the rest of the application.
// This means it should remain pure (apart from logs).
export class RaftNode<LogValueType> {
    private state: State<LogValueType>;

    private resolveCommittedAtLeastOneEntry: (result: boolean) => void = noop;
    private committedAtLeastOneEntry: Promise<boolean>;

    public constructor(
        private readonly sendMessage: (
            message: OutgoingMessage<LogValueType>,
        ) => void,
        private readonly resetElectionTimeout: () => void,
        private readonly onEntriesCommitted: (
            entries: Array<Entry<LogValueType>>,
        ) => void,
        private readonly persistenceFilePath: string,
        private readonly logger: Logger,
        otherClusterNodes: ReadonlyArray<number>,
    ) {
        const initialEntries = readEntries<LogValueType>(persistenceFilePath);
        this.state = getInitialState(
            new Log<LogValueType>(initialEntries),
            otherClusterNodes,
        );
        this.committedAtLeastOneEntry = new Promise((resolve) => {
            this.resolveCommittedAtLeastOneEntry = resolve;
        });
    }

    public appendToLog(
        value: LogValueType,
        id: RequestId,
    ): { isLeader: boolean } {
        if (this.state.type !== 'leader') {
            return { isLeader: false };
        }

        this.dispatch({
            type: 'appendToLog',
            entry: {
                type: 'value',
                value,
                id,
            },
        });
        return { isLeader: true };
    }

    public leaderElectionTimeout() {
        this.logger.debug('Leader election timeout');
        this.dispatch({
            type: 'electionTimeout',
        });
    }

    public receiveMessage(message: IncomingMessage<LogValueType>) {
        const { sender, ...rest } = message;
        this.logger.debug('received message', {
            message,
        });
        this.dispatch({
            type: 'receivedMessageFromNode',
            message: rest,
            node: sender,
        });
    }

    public sendHeartbeatTimeoutForNode(node: number) {
        this.logger.debug('heartbeat timeout for node', {
            node,
        });
        this.dispatch({
            type: 'sendHeartbeatMessageTimeout',
            node,
        });
    }

    public get __stateForTests(): State<LogValueType> {
        return this.state;
    }

    private dispatch(event: Event<LogValueType>) {
        const { newState, effects } = reduce(event, this.state);

        this.onStateChange({ oldState: this.state, newState });

        this.state = newState;

        this.handleEffects(effects);
    }

    private onStateChange({
        oldState,
        newState,
    }: {
        oldState: State<LogValueType>;
        newState: State<LogValueType>;
    }) {
        if (newState.type !== oldState.type) {
            this.logger.info(`RaftNode became ${newState.type}`);
        }

        if (newState.commitIndex > oldState.commitIndex) {
            const entries = newState.log
                .getEntries()
                .slice(oldState.commitIndex + 1, newState.commitIndex + 1);
            this.onEntriesCommitted([...entries]);
        }

        if (
            oldState.type === 'leader' &&
            newState.type === 'leader' &&
            !oldState.hasCommittedEntryThisTerm &&
            newState.hasCommittedEntryThisTerm
        ) {
            this.resolveCommittedAtLeastOneEntry(true);
        }

        if (oldState.type === 'leader' && newState.type !== 'leader') {
            this.resolveCommittedAtLeastOneEntry(false);
            this.committedAtLeastOneEntry = new Promise((resolve) => {
                this.resolveCommittedAtLeastOneEntry = resolve;
            });
        }
    }

    private handleEffects(effects: Effect<LogValueType>[]) {
        effects.forEach((effect) => {
            switch (effect.type) {
                case 'sendMessageToNode': {
                    const message = {
                        ...effect.message,
                        receiver: effect.node,
                    };
                    this.logger.debug('sending message', {
                        message,
                    });
                    this.sendMessage(message);
                    return;
                }

                case 'resetElectionTimeout':
                    this.resetElectionTimeout();
                    return;

                case 'appendNoopEntryToLog':
                    this.dispatch({
                        type: 'appendToLog',
                        entry: {
                            type: 'noop',
                        },
                    });
                    return;

                case 'persistLog':
                    this.persistLog(this.state.log);
                    return;

                default:
                    unreachable(effect);
            }
        });
    }

    public async isLeaderAndCommittedAtLeastOneEntryThisTerm(): Promise<boolean> {
        if (this.state.type !== 'leader') {
            return false;
        }

        return this.committedAtLeastOneEntry;
    }

    private persistLog(log: Log<LogValueType>) {
        writeEntries(
            this.persistenceFilePath,
            Immutable.asMutable(log.getEntries()),
        );
    }
}
