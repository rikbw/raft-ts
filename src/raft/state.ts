import { unreachable } from '../util/unreachable';
import { Entry, EntryIdentifier, Log } from './log';

type FollowerInfo = Record<number, { nextIndex: number }>;

type MutableState<LogValueType> =
    | {
          type: 'follower';
          currentTerm: number;
          log: Log<LogValueType>;
          otherClusterNodes: number[];
          votedFor: number | undefined;
      }
    | {
          type: 'leader';
          currentTerm: number;
          log: Log<LogValueType>;
          followerInfo: FollowerInfo;
          otherClusterNodes: number[];
      }
    | {
          type: 'candidate';
          currentTerm: number;
          votes: Set<number>;
          log: Log<LogValueType>;
          otherClusterNodes: number[];
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
    otherClusterNodes: number[],
): State<LogValueType> {
    return {
        type: 'follower',
        currentTerm: 0,
        log,
        otherClusterNodes,
        votedFor: undefined,
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
          prevLogIndexFromRequest: number;
      }
    | {
          type: 'appendEntriesResponseNotOk';
          prevLogIndexFromRequest: number;
          term: number;
      }
    | {
          // TODO implement leader completeness
          type: 'requestVote';
          term: number;
      }
    | {
          type: 'requestVoteResponse';
          voteGranted: boolean;
          term: number;
      };

type MutableEffect<LogValueType> =
    | {
          type: 'resetElectionTimeout';
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
            const voteRequests = state.otherClusterNodes.map(
                (node): Effect<LogValueType> => ({
                    type: 'sendMessageToNode',
                    node,
                    message: {
                        type: 'requestVote',
                        term: newTerm,
                    },
                }),
            );
            return {
                newState: {
                    type: 'candidate',
                    currentTerm: newTerm,
                    log: state.log,
                    votes: new Set(),
                    otherClusterNodes: state.otherClusterNodes,
                },
                effects: [
                    ...voteRequests,
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
            return reduceReceivedAppendEntriesResponseOk({
                state,
                prevLogIndexFromRequest: message.prevLogIndexFromRequest,
                node,
            });

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
                prevLogIndexFromRequest: message.prevLogIndexFromRequest,
                state,
                term: message.term,
            });

        case 'requestVoteResponse':
            return reduceReceivedRequestVoteResponse({
                state,
                voteGranted: message.voteGranted,
                term: message.term,
                node,
            });

        case 'requestVote':
            return reduceReceivedRequestVote({
                state,
                term: message.term,
                node,
            });

        default:
            return unreachable(message);
    }
}

function reduceReceivedAppendEntriesResponseOk<LogValueType>({
    state,
    node,
    prevLogIndexFromRequest,
}: {
    state: State<LogValueType>;
    node: number;
    prevLogIndexFromRequest: number;
}): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'leader': {
            const newState: State<LogValueType> = {
                ...state,
                followerInfo: {
                    ...state.followerInfo,
                    [node]: {
                        nextIndex: prevLogIndexFromRequest + 2,
                    },
                },
            };
            return {
                newState,
                effects: [],
            };
        }

        case 'follower':
        case 'candidate':
            throw new Error(
                'unexpected error: did not expect to receive append entries result in this state',
            );
    }
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
            const resetElectionTimeoutEffect: Effect<LogValueType> = {
                type: 'resetElectionTimeout',
            };

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
                                prevLogIndexFromRequest: 0,
                            },
                        },
                        resetElectionTimeoutEffect,
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
                otherClusterNodes: state.otherClusterNodes,
                votedFor: undefined,
            };

            const prevLogIndexFromRequest =
                previousEntryIdentifier?.index ?? -1;

            if (!ok) {
                return {
                    newState,
                    effects: [
                        {
                            type: 'sendMessageToNode',
                            node,
                            message: {
                                type: 'appendEntriesResponseNotOk',
                                prevLogIndexFromRequest,
                                term,
                            },
                        },
                        resetElectionTimeoutEffect,
                    ],
                };
            }

            return {
                newState,
                effects: [
                    {
                        type: 'sendMessageToNode',
                        message: {
                            type: 'appendEntriesResponseOk',
                            prevLogIndexFromRequest,
                        },
                        node,
                    },
                    resetElectionTimeoutEffect,
                ],
            };
        }

        case 'candidate':
            if (term >= state.currentTerm) {
                const newState: State<LogValueType> = {
                    type: 'follower',
                    log: state.log,
                    currentTerm: term,
                    otherClusterNodes: state.otherClusterNodes,
                    votedFor: undefined,
                };

                const prevLogIndexFromRequest =
                    previousEntryIdentifier?.index ?? -1;

                return {
                    newState,
                    effects: [
                        {
                            type: 'sendMessageToNode',
                            node,
                            message: {
                                type: 'appendEntriesResponseOk',
                                prevLogIndexFromRequest,
                            },
                        },
                        {
                            type: 'resetElectionTimeout',
                        },
                    ],
                };
            }

            return {
                newState: state,
                effects: [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntriesResponseNotOk',
                            term: state.currentTerm,
                            // Does not matter, the sender will step down as a leader
                            prevLogIndexFromRequest: 0,
                        },
                    },
                ],
            };

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
    log: Log<ValueType>,
    nextIndex: number,
): EntryIdentifier | undefined {
    const previousLogIndex = nextIndex - 1;

    if (previousLogIndex < 0) {
        return undefined;
    }

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const previousLogTerm = log.getEntries()[previousLogIndex]!.term;

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
                        type: 'sendMessageToNode',
                        message: {
                            type: 'appendEntries',
                            term: state.currentTerm,
                            // TODO test this value
                            previousEntryIdentifier:
                                previousEntryIdentifierFromNextIndex(
                                    state.log,
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
    prevLogIndexFromRequest,
    term,
    node,
}: {
    state: State<LogValueType>;
    prevLogIndexFromRequest: number;
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
                        otherClusterNodes: state.otherClusterNodes,
                        votedFor: undefined,
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
                            prevLogIndexFromRequest,
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
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntries',
                            term: state.currentTerm,
                            previousEntryIdentifier:
                                previousEntryIdentifierFromNextIndex(
                                    newState.log,
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

function reduceReceivedRequestVoteResponse<LogValueType>({
    state,
    voteGranted,
    node,
    term,
}: {
    state: State<LogValueType>;
    voteGranted: boolean;
    node: number;
    term: number;
}): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'candidate': {
            if (term > state.currentTerm) {
                return {
                    newState: {
                        type: 'follower',
                        currentTerm: term,
                        otherClusterNodes: state.otherClusterNodes,
                        log: state.log,
                        votedFor: undefined,
                    },
                    effects: [],
                };
            }

            const votes = voteGranted ? state.votes.add(node) : state.votes;

            if (votes.size > state.otherClusterNodes.length / 2) {
                const nextIndex = state.log.getEntries().length;

                const appendEntriesMessage: NodeMessage<LogValueType> = {
                    type: 'appendEntries',
                    entries: [],
                    previousEntryIdentifier:
                        previousEntryIdentifierFromNextIndex(
                            state.log,
                            nextIndex,
                        ),
                    term: state.currentTerm,
                };

                const effects = state.otherClusterNodes.map(
                    (node): Effect<LogValueType> => ({
                        type: 'sendMessageToNode',
                        node,
                        message: appendEntriesMessage,
                    }),
                );

                const followerInfo = state.otherClusterNodes.reduce(
                    (prev: FollowerInfo, node) => ({
                        ...prev,
                        [node]: { nextIndex: state.log.getEntries().length },
                    }),
                    {},
                );

                return {
                    newState: {
                        type: 'leader',
                        log: state.log,
                        otherClusterNodes: state.otherClusterNodes,
                        currentTerm: state.currentTerm,
                        followerInfo,
                    },
                    effects,
                };
            }

            return {
                newState: {
                    ...state,
                    votes,
                },
                effects: [],
            };
        }

        case 'follower':
        case 'leader':
            throw new Error('not implemented');

        default:
            return unreachable(state);
    }
}

function reduceReceivedRequestVote<LogValueType>({
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
            if (
                term < state.currentTerm ||
                (term === state.currentTerm && state.votedFor !== node)
            ) {
                return {
                    newState: state,
                    effects: [
                        {
                            type: 'sendMessageToNode',
                            node,
                            message: {
                                type: 'requestVoteResponse',
                                voteGranted: false,
                                term: state.currentTerm,
                            },
                        },
                    ],
                };
            }

            return {
                newState: {
                    ...state,
                    currentTerm: term,
                    votedFor: node,
                },
                effects: [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: true,
                            term,
                        },
                    },
                ],
            };
        }

        case 'leader':
        case 'candidate':
            throw new Error('not implemented');

        default:
            return unreachable(state);
    }
}
