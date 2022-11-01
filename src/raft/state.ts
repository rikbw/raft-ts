import { unreachable } from '../util/unreachable';
import { Entry, EntryIdentifier, Log, RequestId } from './log';
import { commitIndexFromState } from './commitIndex';
import Immutable from 'seamless-immutable';

type FollowerInfo = Record<number, { nextIndex: number; matchIndex: number }>;

type MutableState<LogValueType> =
    | {
          type: 'follower';
          currentTerm: number;
          log: Log<LogValueType>;
          otherClusterNodes: ReadonlyArray<number>;
          votedFor: number | undefined;
          commitIndex: number;
      }
    | {
          type: 'leader';
          currentTerm: number;
          log: Log<LogValueType>;
          followerInfo: FollowerInfo;
          otherClusterNodes: ReadonlyArray<number>;
          commitIndex: number;
      }
    | {
          type: 'candidate';
          currentTerm: number;
          votes: Set<number>;
          log: Log<LogValueType>;
          otherClusterNodes: ReadonlyArray<number>;
          commitIndex: number;
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
    otherClusterNodes: ReadonlyArray<number>,
): State<LogValueType> {
    return {
        type: 'follower',
        currentTerm: 0,
        log,
        otherClusterNodes,
        votedFor: undefined,
        commitIndex: -1,
    };
}

type EntryWithoutTerm<LogValueType> =
    | {
          type: 'value';
          id: RequestId;
          value: LogValueType;
      }
    | {
          type: 'noop';
      };

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
      }
    | {
          type: 'appendToLog';
          entry: EntryWithoutTerm<LogValueType>;
      };

export type Event<LogValueType> = Readonly<MutableEvent<LogValueType>>;

export type NodeMessage<LogValueType> =
    | {
          type: 'appendEntries';
          previousEntryIdentifier: EntryIdentifier | undefined;
          term: number;
          entries: Array<Entry<LogValueType>>;
          leaderCommit: number;
      }
    | {
          type: 'appendEntriesResponse';
          ok: boolean;
          prevLogIndexFromRequest: number;
          numberOfEntriesSentInRequest: number;
          term: number;
      }
    | {
          type: 'requestVote';
          term: number;
          lastLog: EntryIdentifier | undefined;
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
      }
    | {
          type: 'appendNoopEntryToLog';
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

        case 'appendToLog':
            return reduceAppendToLog(state, event.entry);

        default:
            return unreachable(event);
    }
}

function reduceElectionTimeout<LogValueType>(
    state: State<LogValueType>,
): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'leader':
            // Ignore leader election timeouts as a leader.
            return {
                newState: state,
                effects: [],
            };

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
                        lastLog: lastEntryIdentifierFromState(state),
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
                    commitIndex: state.commitIndex,
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
        case 'appendEntriesResponse':
            return reduceReceivedAppendEntriesResponse({
                state,
                prevLogIndexFromRequest: message.prevLogIndexFromRequest,
                node,
                term: message.term,
                ok: message.ok,
                numberOfEntriesSentInRequest:
                    message.numberOfEntriesSentInRequest,
            });

        case 'appendEntries':
            return reduceReceivedAppendEntries({
                state,
                entries: message.entries,
                node,
                previousEntryIdentifier: message.previousEntryIdentifier,
                term: message.term,
                leaderCommit: message.leaderCommit,
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
                lastLog: message.lastLog,
            });

        default:
            return unreachable(message);
    }
}

const commitIndexAfterReceivingAppendEntries = ({
    currentCommitIndex,
    leaderCommit,
    logLength,
    appendEntriesOk,
}: {
    currentCommitIndex: number;
    leaderCommit: number;
    logLength: number;
    appendEntriesOk: boolean;
}) => {
    if (!appendEntriesOk) {
        return currentCommitIndex;
    }
    return Math.max(currentCommitIndex, Math.min(leaderCommit, logLength - 1));
};

function reduceReceivedAppendEntries<LogValueType>({
    state,
    term,
    node,
    previousEntryIdentifier,
    entries,
    leaderCommit,
}: {
    state: State<LogValueType>;
    term: number;
    node: number;
    previousEntryIdentifier: EntryIdentifier | undefined;
    entries: Entry<LogValueType>[];
    leaderCommit: number;
}): ReducerResult<LogValueType> {
    const numberOfEntriesSentInRequest = entries.length;
    const prevLogIndexFromRequest = previousEntryIdentifier?.index ?? -1;

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
                                type: 'appendEntriesResponse',
                                term: state.currentTerm,
                                prevLogIndexFromRequest,
                                numberOfEntriesSentInRequest,
                                ok: false,
                            },
                        },
                        resetElectionTimeoutEffect,
                    ],
                };
            }

            const { ok, newLog } = state.log.appendEntries({
                previousEntryIdentifier,
                entries,
            });

            const votedFor =
                term > state.currentTerm ? undefined : state.votedFor;

            const commitIndex = commitIndexAfterReceivingAppendEntries({
                currentCommitIndex: state.commitIndex,
                leaderCommit,
                logLength: newLog.length,
                appendEntriesOk: ok,
            });

            const newState: State<LogValueType> = {
                type: 'follower',
                currentTerm: term,
                log: newLog,
                otherClusterNodes: state.otherClusterNodes,
                votedFor,
                commitIndex,
            };

            if (!ok) {
                return {
                    newState,
                    effects: [
                        {
                            type: 'sendMessageToNode',
                            node,
                            message: {
                                type: 'appendEntriesResponse',
                                prevLogIndexFromRequest,
                                numberOfEntriesSentInRequest,
                                term,
                                ok: false,
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
                            type: 'appendEntriesResponse',
                            ok: true,
                            term: newState.currentTerm,
                            prevLogIndexFromRequest,
                            numberOfEntriesSentInRequest,
                        },
                        node,
                    },
                    resetElectionTimeoutEffect,
                ],
            };
        }

        case 'candidate':
            if (term >= state.currentTerm) {
                const previousEntryIdentifier =
                    lastEntryIdentifierFromState(state);

                const { ok, newLog } = state.log.appendEntries({
                    previousEntryIdentifier,
                    entries,
                });

                const commitIndex = commitIndexAfterReceivingAppendEntries({
                    currentCommitIndex: state.commitIndex,
                    leaderCommit,
                    logLength: newLog.length,
                    appendEntriesOk: ok,
                });

                const newState: State<LogValueType> = {
                    type: 'follower',
                    log: newLog,
                    currentTerm: term,
                    otherClusterNodes: state.otherClusterNodes,
                    // This can be undefined, because we didn't vote for anyone else than this node in this term.
                    // So there cannot be two leaders at once for this term.
                    votedFor: undefined,
                    commitIndex,
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
                                type: 'appendEntriesResponse',
                                ok,
                                term,
                                prevLogIndexFromRequest,
                                numberOfEntriesSentInRequest,
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
                            type: 'appendEntriesResponse',
                            ok: false,
                            term: state.currentTerm,
                            prevLogIndexFromRequest,
                            numberOfEntriesSentInRequest,
                        },
                    },
                ],
            };

        case 'leader':
            if (term > state.currentTerm) {
                return {
                    newState: {
                        type: 'follower',
                        log: state.log,
                        otherClusterNodes: state.otherClusterNodes,
                        currentTerm: term,
                        votedFor: undefined,
                        commitIndex: state.commitIndex,
                    },
                    effects: [],
                };
            }

            if (term === state.currentTerm) {
                throw new Error(
                    'unreachable: a node thinks it is leader of the same term as this node',
                );
            }

            return {
                newState: state,
                effects: [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: false,
                            prevLogIndexFromRequest,
                            numberOfEntriesSentInRequest,
                            term: state.currentTerm,
                        },
                    },
                ],
            };

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
    const nextIndex = nodeInfo?.nextIndex ?? log.length;

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

function sendAppendEntriesEffect<LogValueType>({
    node,
    state,
}: {
    node: number;
    state: LeaderState<LogValueType>;
}): Effect<LogValueType> {
    const nextIndex = nextIndexForNode(state, node);
    const entries = state.log.getEntries().slice(nextIndex);

    return {
        type: 'sendMessageToNode',
        message: {
            type: 'appendEntries',
            term: state.currentTerm,
            previousEntryIdentifier: previousEntryIdentifierFromNextIndex(
                state.log,
                nextIndex,
            ),
            entries: Immutable.asMutable(entries),
            leaderCommit: state.commitIndex,
        },
        node,
    };
}

function reduceSendHeartbeatMessageTimeout<LogValueType>(
    state: State<LogValueType>,
    node: number,
): ReducerResult<LogValueType> {
    switch (state.type) {
        case 'leader': {
            return {
                newState: state,
                effects: [sendAppendEntriesEffect({ node, state })],
            };
        }

        case 'candidate':
        case 'follower':
            return {
                newState: state,
                effects: [],
            };

        default:
            return unreachable(state);
    }
}

function reduceReceivedAppendEntriesResponse<LogValueType>({
    state,
    prevLogIndexFromRequest,
    term,
    node,
    ok,
    numberOfEntriesSentInRequest,
}: {
    state: State<LogValueType>;
    prevLogIndexFromRequest: number;
    term: number;
    node: number;
    ok: boolean;
    numberOfEntriesSentInRequest: number;
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
                        commitIndex: state.commitIndex,
                    },
                    effects: [],
                };
            }

            if (!ok) {
                const newState: LeaderState<LogValueType> = {
                    ...state,
                    followerInfo: {
                        ...state.followerInfo,
                        [node]: {
                            // nextIndex should become prevLogIndexFromRequest, because the index we used before that was
                            // prevLogIndexFromRequest + 1. We should go one down, so prevLogIndexFromRequest + 1 - 1.
                            // edge case: if prevLogIndexFromRequest === -1, which can happen if the log is empty,
                            // we should just set nextIndex to 0.
                            nextIndex: Math.max(prevLogIndexFromRequest, 0),
                            matchIndex:
                                state.followerInfo[node]?.matchIndex ?? -1,
                        },
                    },
                };

                return {
                    newState,
                    effects: [
                        sendAppendEntriesEffect({ node, state: newState }),
                    ],
                };
            }

            // Logic: response was ok, and we successfully appended numberOfEntriesSentInRequest entries starting from
            // prevLogIndexFromRequest + 1, so next index should be the sum of those.
            const nextIndex = Math.max(
                prevLogIndexFromRequest + 1 + numberOfEntriesSentInRequest,
                0,
            );

            // Logic: we replicated until nextIndex - 1, but response can be outdated and we should not decrease matchIndex.
            const matchIndex = Math.max(
                nextIndex - 1,
                state.followerInfo[node]?.matchIndex ?? -1,
            );

            const updatedState = {
                ...state,
                followerInfo: {
                    ...state.followerInfo,
                    [node]: {
                        nextIndex,
                        matchIndex,
                    },
                },
            };

            const commitIndex = commitIndexFromState(updatedState);

            const newState = {
                ...updatedState,
                commitIndex,
            };

            return {
                newState,
                effects: [],
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
                        commitIndex: state.commitIndex,
                    },
                    effects: [],
                };
            }

            const votes = voteGranted ? state.votes.add(node) : state.votes;

            if (votes.size + 1 > state.otherClusterNodes.length / 2) {
                const followerInfo = state.otherClusterNodes.reduce(
                    (prev: FollowerInfo, node) => ({
                        ...prev,
                        [node]: {
                            nextIndex: state.log.length,
                            matchIndex: -1,
                        },
                    }),
                    {},
                );

                const newState: LeaderState<LogValueType> = {
                    type: 'leader',
                    log: state.log,
                    otherClusterNodes: state.otherClusterNodes,
                    currentTerm: state.currentTerm,
                    followerInfo,
                    commitIndex: state.commitIndex,
                };

                return {
                    newState,
                    effects: [
                        {
                            type: 'appendNoopEntryToLog',
                        },
                    ],
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

        // Ignore, either we're already leader or we're no longer expecting to get elected.
        case 'follower':
        case 'leader':
            return {
                newState: state,
                effects: [],
            };

        default:
            return unreachable(state);
    }
}

const votedFor = <T>(state: State<T>, node: number): boolean => {
    switch (state.type) {
        case 'follower':
            return state.votedFor === node;

        case 'leader':
        case 'candidate':
            return false;

        default:
            return unreachable(state);
    }
};

function requestLastLogIsNotUpToDate({
    requestLastLog = { term: -1, index: -1 },
    stateLastLog = { term: -1, index: -1 },
}: {
    requestLastLog: EntryIdentifier | undefined;
    stateLastLog: EntryIdentifier | undefined;
}) {
    return (
        requestLastLog.term < stateLastLog.term ||
        (requestLastLog.term === stateLastLog.term &&
            requestLastLog.index < stateLastLog.index)
    );
}

function reduceReceivedRequestVote<LogValueType>({
    state,
    term,
    node,
    lastLog,
}: {
    state: State<LogValueType>;
    term: number;
    node: number;
    lastLog: EntryIdentifier | undefined;
}): ReducerResult<LogValueType> {
    const stateLastLog = lastEntryIdentifierFromState(state);

    if (
        requestLastLogIsNotUpToDate({
            requestLastLog: lastLog,
            stateLastLog,
        }) ||
        term < state.currentTerm ||
        (term === state.currentTerm && !votedFor(state, node))
    ) {
        const newState: State<LogValueType> =
            term > state.currentTerm
                ? {
                      type: 'follower',
                      log: state.log,
                      votedFor: undefined,
                      otherClusterNodes: state.otherClusterNodes,
                      currentTerm: term,
                      commitIndex: state.commitIndex,
                  }
                : state;
        return {
            newState,
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
            type: 'follower',
            log: state.log,
            otherClusterNodes: state.otherClusterNodes,
            currentTerm: term,
            votedFor: node,
            commitIndex: state.commitIndex,
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

function reduceAppendToLog<LogValueType>(
    state: State<LogValueType>,
    entry: EntryWithoutTerm<LogValueType>,
): ReducerResult<LogValueType> {
    if (state.type !== 'leader') {
        throw new Error('can only append to log of leader node');
    }

    const previousEntryIdentifier = lastEntryIdentifierFromState(state);

    const { ok, newLog } = state.log.appendEntries({
        previousEntryIdentifier,
        entries: [
            {
                ...entry,
                term: state.currentTerm,
            },
        ],
    });

    if (!ok) {
        throw new Error('unexpected error: failed to append to leader log');
    }

    const newState = {
        ...state,
        log: newLog,
    };

    const effects = newState.otherClusterNodes.map(
        (node): Effect<LogValueType> =>
            sendAppendEntriesEffect({ state: newState, node }),
    );

    return {
        newState,
        effects,
    };
}

function lastEntryIdentifierFromState<LogValueType>(
    state: State<LogValueType>,
) {
    return previousEntryIdentifierFromNextIndex(state.log, state.log.length);
}
