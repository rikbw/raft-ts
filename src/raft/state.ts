import { unreachable } from '../util/unreachable';
import { Log } from './log';

type MutableState<LogValueType> =
    | {
          type: 'follower';
          currentTerm: number;
          log: Log<LogValueType>;
      }
    | {
          type: 'leader';
          currentTerm: number;
          log: Log<LogValueType>;
      }
    | {
          type: 'candidate';
          currentTerm: number;
          log: Log<LogValueType>;
      };

export type State<LogValueType> = Readonly<MutableState<LogValueType>>;

export type FollowerState<LogValueType> = State<LogValueType> & {
    type: 'follower';
};
export type LeaderState<LogValueType> = State<LogValueType> & {
    type: 'leader';
};
export type CandidateState<LogValueType> = State<LogValueType> & {
    type: 'candidate';
};

export function getInitialState<LogValueType>(
    log: Log<LogValueType>,
): State<LogValueType> {
    return {
        type: 'follower',
        currentTerm: 0,
        log,
    };
}

type MutableEvent =
    | {
          type: 'electionTimeout';
      }
    | {
          type: 'sendHeartbeatMessageTimeout';
          node: number;
      }
    | {
          type: 'receivedAppendEntries';
          term: number;
      };

export type Event = Readonly<MutableEvent>;

type MutableEffect =
    | {
          type: 'resetElectionTimeout';
      }
    | {
          type: 'resetSendHeartbeatMessageTimeout';
          node: number;
      }
    | {
          type: 'sendAppendEntries';
          term: number;
          node: number;
      }
    | {
          type: 'broadcastRequestVote';
          term: number;
      }
    | {
          type: 'sendAppendEntriesResponse';
          ok: boolean;
          term: number;
      };

export type Effect = Readonly<MutableEffect>;

type ReducerResult<LogValueType> = {
    newState: State<LogValueType>;
    effects: Effect[];
};

export function reduce<LogValueType>(
    event: Event,
    state: State<LogValueType>,
): ReducerResult<LogValueType> {
    switch (event.type) {
        case 'electionTimeout':
            return reduceElectionTimeout(state);

        case 'receivedAppendEntries':
            return reduceReceivedAppendEntries({
                state,
                term: event.term,
            });

        case 'sendHeartbeatMessageTimeout':
            return reduceSendHeartbeatMessageTimeout(state, event.node);

        default:
            return unreachable(event);
    }
}

function reduceElectionTimeout<LogValueType>(
    state: State<LogValueType>,
): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'leader':
            throw new Error(
                'unreachable: election timeout should not fire when you are a leader',
            );
        case 'follower':
        case 'candidate': {
            const newTerm = state.currentTerm + 1;
            return {
                newState: {
                    type: 'candidate',
                    currentTerm: newTerm,
                    log: state.log,
                },
                effects: [
                    {
                        type: 'broadcastRequestVote',
                        term: newTerm,
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ],
            };
        }

        default:
            return unreachable(state);
    }
}

function reduceReceivedAppendEntries<LogValueType>({
    state,
    term,
}: {
    state: State<LogValueType>;
    term: number;
}): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'follower': {
            if (term > state.currentTerm) {
                return {
                    newState: {
                        type: 'follower',
                        currentTerm: term,
                        log: state.log,
                    },
                    effects: [
                        {
                            type: 'sendAppendEntriesResponse',
                            ok: true,
                            term,
                        },
                    ],
                };
            }

            throw new Error('not implemented');
        }

        case 'candidate':
        case 'leader':
            throw new Error('not implemented');

        default:
            return unreachable(state);
    }
}

function reduceSendHeartbeatMessageTimeout<LogValueType>(
    state: State<LogValueType>,
    node: number,
): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'leader':
            return {
                newState: state,
                effects: [
                    {
                        type: 'resetSendHeartbeatMessageTimeout',
                        node,
                    },
                    {
                        type: 'sendAppendEntries',
                        term: state.currentTerm,
                        node,
                    },
                ],
            };

        case 'candidate':
        case 'follower':
            throw new Error(
                'unreachable: did not expect a send heartbeat message timer to timeout in this state',
            );

        default:
            return unreachable(state);
    }
}
