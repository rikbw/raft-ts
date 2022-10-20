import {
    getInitialState,
    reduce,
    State,
    Event,
    Effect,
    NodeMessage,
} from './state';
import { Log } from './log';
import { unreachable } from '../util/unreachable';
import { createLogger } from 'bunyan';

type Logger = ReturnType<typeof createLogger>;

type IncomingMessage<LogValueType> = NodeMessage<LogValueType> & {
    sender: number;
};

export type OutgoingMessage<LogValueType> = NodeMessage<LogValueType> & {
    receiver: number;
};

// Wrapper on top of State & reducer, to give it an easier API to work with in the rest of the application.
// This means it should remain pure.
export class RaftNode<LogValueType> {
    private state: State<LogValueType>;

    public constructor(
        private readonly sendMessage: (
            message: OutgoingMessage<LogValueType>,
        ) => void,
        private readonly logger: Logger,
        // TODO when all messages have proper handlers, this won't be necessary anymore.
        // For example, when voting works, we can create a leader by sending it (fake) messages in tests.
        // Now we have to give it an initial state that says it's a leader.
        initialStateForTesting?: State<LogValueType>,
    ) {
        this.state =
            initialStateForTesting ??
            // TODO this should read the log from disk. Should probably be passed down by the caller of this constructor.
            getInitialState(new Log<LogValueType>([]));
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

        if (newState.type !== this.state.type) {
            this.logger.info(`RaftNode became ${newState.type}`);
        }

        this.handleEffects(effects);

        this.state = newState;
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
                    return;

                case 'resetSendHeartbeatMessageTimeout':
                    return;

                case 'broadcastRequestVote':
                    return;

                default:
                    unreachable(effect);
            }
        });
    }
}
