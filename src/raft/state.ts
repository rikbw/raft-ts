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

type MutableEvent<LogValueType> =
    | {
          type: 'electionTimeout';
      }
    | {
          type: 'sendHeartbeatMessageTimeout';
          node: number;
      }
    | {
          type: 'receivedMessageFromNode';
          node: number;
          message: NodeMessage<LogValueType>;
      };

export type Event<LogValueType> = Readonly<MutableEvent<LogValueType>>;

export type NodeMessage<LogValueType> =
    | {
          type: 'appendEntries';
          previousEntryIdentifier: EntryIdentifier | undefined;
          term: number;
          entries: Array<Entry<LogValueType>>;
      }
    | {
          type: 'appendEntriesResponseOk';
      }
    | {
          type: 'appendEntriesResponseNotOk';
          prevLogIndex: number;
          term: number;
      };

type MutableEffect<LogValueType> =
    | {
          type: 'resetElectionTimeout';
      }
    | {
          type: 'resetSendHeartbeatMessageTimeout';
          node: number;
      }
    | {
          type: 'broadcastRequestVote';
          term: number;
      }
    | {
          type: 'sendMessageToNode';
          message: NodeMessage<LogValueType>;
          node: number;
      };

export type Effect<LogValueType> = Readonly<MutableEffect<LogValueType>>;

type ReducerResult<LogValueType> = {
    newState: State<LogValueType>;
    effects: Effect<LogValueType>[];
};

export function reduce<LogValueType>(
    event: Event<LogValueType>,
    state: State<LogValueType>,
): ReducerResult<LogValueType> {
    switch (event.type) {
        case 'electionTimeout':
            return reduceElectionTimeout(state);

        case 'receivedMessageFromNode':
            return reduceReceivedMessage({
                state,
                message: event.message,
                node: event.node,
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

function reduceReceivedMessage<LogValueType>({
    state,
    message,
    node,
}: {
    state: State<LogValueType>;
    message: NodeMessage<LogValueType>;
    node: number;
}): ReducerResult<LogValueType> {
    switch (message.type) {
        case 'appendEntriesResponseOk':
            return reduceReceivedAppendEntriesResponseOk(state);

        case 'appendEntries':
            return reduceReceivedAppendEntries({
                state,
                entries: message.entries,
                node,
                previousEntryIdentifier: message.previousEntryIdentifier,
                term: message.term,
            });

        case 'appendEntriesResponseNotOk':
            return reduceReceivedAppendEntriesResponseNotOk({
                node,
                prevLogIndex: message.prevLogIndex,
                state,
                term: message.term,
            });
    }
}

function reduceReceivedAppendEntriesResponseOk<LogValueType>(
    state: State<LogValueType>,
): ReducerResult<LogValueType> {
    return {
        newState: state,
        effects: [],
    };
}

function reduceReceivedAppendEntries<LogValueType>({
    state,
    term,
    node,
    previousEntryIdentifier,
    entries,
}: {
    state: State<LogValueType>;
    term: number;
    node: number;
    previousEntryIdentifier: EntryIdentifier | undefined;
    entries: Entry<LogValueType>[];
}): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'follower': {
            if (term < state.currentTerm) {
                return {
                    newState: state,
                    effects: [
                        {
                            type: 'sendMessageToNode',
                            node,
                            message: {
                                type: 'appendEntriesResponseNotOk',
                                term: state.currentTerm,
                                // Doesn't matter, the receiver will step down as a leader.
                                prevLogIndex: 0,
                            },
                        },
                    ],
                };
            }

            // TODO make this return the new log instead of mutating it, that doesn't fit with the rest of the app
            const ok = state.log.appendEntries({
                previousEntryIdentifier,
                entries,
            });

            const newState: State<LogValueType> = {
                type: 'follower',
                currentTerm: term,
                log: state.log,
            };

            // TODO add a test for this case
            if (!ok) {
                return {
                    newState,
                    effects: [
                        {
                            type: 'sendMessageToNode',
                            node,
                            message: {
                                type: 'appendEntriesResponseNotOk',
                                prevLogIndex:
                                    previousEntryIdentifier?.index ?? -1,
                                term,
                            },
                        },
                    ],
                };
            }

            if (term > state.currentTerm) {
                return {
                    newState,
                    effects: [
                        {
                            type: 'sendMessageToNode',
                            message: {
                                type: 'appendEntriesResponseOk',
                            },
                            node,
                        },
                    ],
                };
            }

            return {
                newState,
                effects: [],
            };
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
                        type: 'sendMessageToNode',
                        message: {
                            type: 'appendEntries',
                            term: state.currentTerm,
                            // TODO test this value
                            previousEntryIdentifier:
                                previousEntryIdentifierFromNextIndex(
                                    state,
                                    nextIndex,
                                ),
                            entries,
                        },
                        node,
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

function reduceReceivedAppendEntriesResponseNotOk<LogValueType>({
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
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntries',
                            term: state.currentTerm,
                            previousEntryIdentifier:
                                previousEntryIdentifierFromNextIndex(
                                    newState,
                                    nextIndex,
                                ),
                            entries,
                        },
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
