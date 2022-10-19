import { unreachable } from '../util/unreachable';
import { Entry, EntryIdentifier, Log } from './log';

type FollowerInfo = Record<number, { nextIndex: number }>;

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
          followerInfo: FollowerInfo;
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
          node: number;
      }
    | {
          type: 'receivedAppendEntriesResultOk';
          node: number;
      }
    | {
          type: 'receivedAppendEntriesResultNotOk';
          prevLogIndex: number;
          term: number;
          node: number;
      };

export type Event = Readonly<MutableEvent>;

type MutableEffect<LogValueType> =
    | {
          type: 'resetElectionTimeout';
      }
    | {
          type: 'resetSendHeartbeatMessageTimeout';
          node: number;
      }
    | {
          type: 'sendAppendEntries';
          previousEntryIdentifier: EntryIdentifier | undefined;
          term: number;
          node: number;
          entries: Array<Entry<LogValueType>>;
      }
    | {
          type: 'broadcastRequestVote';
          term: number;
      }
    | {
          type: 'sendAppendEntriesResponseOk';
          node: number;
      }
    | {
          type: 'sendAppendEntriesResponseNotOk';
          prevLogIndex: number;
          term: number;
          node: number;
      };

export type Effect<LogValueType> = Readonly<MutableEffect<LogValueType>>;

type ReducerResult<LogValueType> = {
    newState: State<LogValueType>;
    effects: Effect<LogValueType>[];
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
                node: event.node,
            });

        case 'sendHeartbeatMessageTimeout':
            return reduceSendHeartbeatMessageTimeout(state, event.node);

        case 'receivedAppendEntriesResultOk':
            throw new Error('not implemented');

        case 'receivedAppendEntriesResultNotOk':
            return receivedAppendEntriesResultNotOk({
                state,
                prevLogIndex: event.prevLogIndex,
                term: event.term,
                node: event.node,
            });

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
    node,
}: {
    state: State<LogValueType>;
    term: number;
    node: number;
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
                            type: 'sendAppendEntriesResponseOk',
                            node,
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

function nextIndexForNode<LogValueType>(
    state: LeaderState<LogValueType>,
    node: number,
): number {
    const { followerInfo, log } = state;
    const nodeInfo = followerInfo[node];
    const nextIndex = nodeInfo?.nextIndex ?? log.getEntries().length;

    if (nextIndex < 0) {
        throw new Error('unexpected error: nextIndex is smaller than zero');
    }

    return nextIndex;
}

function previousEntryIdentifierFromNextIndex<ValueType>(
    state: LeaderState<ValueType>,
    nextIndex: number,
): EntryIdentifier | undefined {
    const previousLogIndex = nextIndex - 1;

    if (previousLogIndex < 0) {
        return undefined;
    }

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const previousLogTerm = state.log.getEntries()[previousLogIndex]!.term;

    return {
        index: previousLogIndex,
        term: previousLogTerm,
    };
}

function reduceSendHeartbeatMessageTimeout<LogValueType>(
    state: State<LogValueType>,
    node: number,
): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'leader': {
            const nextIndex = nextIndexForNode(state, node);
            const entries = state.log.getEntries().slice(nextIndex);
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
                        // TODO test this value
                        previousEntryIdentifier:
                            previousEntryIdentifierFromNextIndex(
                                state,
                                nextIndex,
                            ),
                        entries,
                    },
                ],
            };
        }

        case 'candidate':
        case 'follower':
            throw new Error(
                'unreachable: did not expect a send heartbeat message timer to timeout in this state',
            );

        default:
            return unreachable(state);
    }
}

function receivedAppendEntriesResultNotOk<LogValueType>({
    state,
    prevLogIndex,
    term,
    node,
}: {
    state: State<LogValueType>;
    prevLogIndex: number;
    term: number;
    node: number;
}): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'leader': {
            if (term > state.currentTerm) {
                return {
                    newState: {
                        type: 'follower',
                        log: state.log,
                        currentTerm: term,
                    },
                    effects: [],
                };
            }

            const newState: LeaderState<LogValueType> = {
                ...state,
                followerInfo: {
                    ...state.followerInfo,
                    [node]: {
                        nextIndex: Math.max(
                            prevLogIndex,
                            (state.followerInfo[node]?.nextIndex ?? 0) - 1,
                        ),
                    },
                },
            };

            const nextIndex = nextIndexForNode(newState, node);
            const entries = state.log.getEntries().slice(nextIndex);
            return {
                newState,
                effects: [
                    {
                        type: 'resetSendHeartbeatMessageTimeout',
                        node,
                    },
                    {
                        type: 'sendAppendEntries',
                        term: state.currentTerm,
                        node,
                        previousEntryIdentifier:
                            previousEntryIdentifierFromNextIndex(
                                newState,
                                nextIndex,
                            ),
                        entries,
                    },
                ],
            };
        }

        case 'follower':
        case 'candidate':
            throw new Error(
                'unreachable: did not expect to receive a response to append entries in this state',
            );

        default:
            return unreachable(state);
    }
}
