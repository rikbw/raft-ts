import {
    Event,
    reduce,
    Effect,
    FollowerState,
    CandidateState,
    LeaderState,
} from './state';
import { Entry, Log } from './log';

const followerState = ({
    currentTerm = 0,
    log = new Log([]),
    otherClusterNodes = [],
    votedFor = undefined,
    commitIndex = -1,
}: Partial<FollowerState<string>> = {}): FollowerState<string> => ({
    type: 'follower',
    currentTerm,
    log,
    otherClusterNodes,
    votedFor,
    commitIndex,
});

const candidateState = ({
    currentTerm = 0,
    log = new Log([]),
    otherClusterNodes = [],
    votes = new Set(),
    commitIndex = -1,
}: Partial<CandidateState<string>> = {}): CandidateState<string> => ({
    type: 'candidate',
    currentTerm,
    log,
    otherClusterNodes,
    votes,
    commitIndex,
});

const leaderState = ({
    currentTerm = 0,
    log = new Log([]),
    followerInfo = {},
    otherClusterNodes = [],
    commitIndex = -1,
    hasCommittedEntryThisTerm = false,
}: Partial<LeaderState<string>> = {}): LeaderState<string> => ({
    type: 'leader',
    currentTerm,
    log,
    followerInfo,
    otherClusterNodes,
    commitIndex,
    hasCommittedEntryThisTerm,
});

const createLogEntries = ({
    nbEntries = 2,
    term,
}: {
    nbEntries?: number;
    term: number;
}): Array<Entry<string>> => {
    return Array(nbEntries)
        .fill(undefined)
        .map(
            (_, index): Entry<string> => ({
                term,
                type: 'value',
                value: `x <- ${index}`,
                id: {
                    clientId: 1,
                    requestSerial: index,
                },
            }),
        );
};

const createLog = (...params: Parameters<typeof createLogEntries>) => {
    const entries = createLogEntries(...params);
    return new Log(entries);
};

describe('state', () => {
    describe('follower', () => {
        it('transitions to candidate and requests votes when election timeout fires', () => {
            const state = followerState({
                currentTerm: 0,
                otherClusterNodes: [0, 2],
                log: createLog({
                    nbEntries: 1,
                    term: 0,
                }),
            });
            const event: Event<string> = {
                type: 'electionTimeout',
            };

            const newState = candidateState({
                currentTerm: 1,
                log: state.log,
                otherClusterNodes: state.otherClusterNodes,
            });
            const effects: Effect<string>[] = [
                {
                    type: 'sendMessageToNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 1,
                        lastLog: {
                            term: 0,
                            index: 0,
                        },
                    },
                },
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'requestVote',
                        term: 1,
                        lastLog: {
                            term: 0,
                            index: 0,
                        },
                    },
                },
                {
                    type: 'resetElectionTimeout',
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState,
                effects,
            });
        });

        describe('when it receives appendEntries with an equal or higher term number', () => {
            it('updates its term and acknowledges the receival', () => {
                const state = followerState({
                    currentTerm: 2,
                    votedFor: 3,
                });
                const node = 1;
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntries',
                        term: 3,
                        entries: [],
                        previousEntryIdentifier: undefined,
                        leaderCommit: -1,
                    },
                };

                const newState = followerState({
                    currentTerm: 3,
                    votedFor: undefined,
                });
                const effects: Effect<string>[] = [
                    {
                        type: 'sendMessageToNode',
                        message: {
                            type: 'appendEntriesResponse',
                            ok: true,
                            term: 3,
                            prevLogIndexFromRequest: -1,
                            numberOfEntriesSentInRequest: 0,
                        },
                        node,
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('appends to its log if the previousEntryIdentifier matches', () => {
                const state = followerState({
                    currentTerm: 2,
                    log: new Log([]),
                });
                const node = 1;
                const entries = createLogEntries({
                    nbEntries: 2,
                    term: 1,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        previousEntryIdentifier: undefined,
                        entries,
                        leaderCommit: -1,
                    },
                };

                const newState = followerState({
                    ...state,
                    log: new Log(entries),
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'persistLog',
                    },
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: true,
                            term: 2,
                            prevLogIndexFromRequest: -1,
                            numberOfEntriesSentInRequest: 2,
                        },
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not append to the log if the previousEntryIdentifier does not match', () => {
                const state = followerState({
                    currentTerm: 2,
                    log: createLog({ term: 1, nbEntries: 1 }),
                });
                const node = 1;
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntries',
                        term: 3,
                        previousEntryIdentifier: {
                            term: 2,
                            index: 0,
                        },
                        entries: [],
                        leaderCommit: -1,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: false,
                            prevLogIndexFromRequest: 0,
                            term: 3,
                            numberOfEntriesSentInRequest: 0,
                        },
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ];
                const newState = {
                    ...state,
                    currentTerm: 3,
                };
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('keeps votedFor if the term is the same', () => {
                const state = followerState({
                    currentTerm: 2,
                    votedFor: 3,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 1,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        entries: [],
                        previousEntryIdentifier: undefined,
                        leaderCommit: -1,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 1,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: true,
                            term: 2,
                            prevLogIndexFromRequest: -1,
                            numberOfEntriesSentInRequest: 0,
                        },
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects,
                });
            });

            it('updates commitIndex', () => {
                const state = followerState({
                    commitIndex: 1,
                    log: createLog({
                        nbEntries: 2,
                        term: 0,
                    }),
                });
                const entries = createLogEntries({
                    nbEntries: 1,
                    term: 0,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'appendEntries',
                        term: 0,
                        entries,
                        leaderCommit: 2,
                        previousEntryIdentifier: {
                            term: 0,
                            index: 1,
                        },
                    },
                };

                const newState = followerState({
                    commitIndex: 2,
                    log: new Log([...state.log.getEntries(), ...entries]),
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'persistLog',
                    },
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: true,
                            numberOfEntriesSentInRequest: 1,
                            prevLogIndexFromRequest: 1,
                            term: 0,
                        },
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('sets commitIndex no higher than the length of the log', () => {
                const state = followerState({
                    commitIndex: -1,
                    log: new Log([]),
                });
                const entries = createLogEntries({ term: 0, nbEntries: 1 });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'appendEntries',
                        term: 0,
                        entries,
                        leaderCommit: 2,
                        previousEntryIdentifier: undefined,
                    },
                };

                const newState = followerState({
                    commitIndex: 0,
                    log: new Log(entries),
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'persistLog',
                    },
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: true,
                            numberOfEntriesSentInRequest: 1,
                            prevLogIndexFromRequest: -1,
                            term: 0,
                        },
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not decrease commitIndex', () => {
                const entries = createLogEntries({ nbEntries: 1, term: 0 });
                const state = followerState({
                    commitIndex: 0,
                    log: new Log(entries),
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'appendEntries',
                        term: 0,
                        entries,
                        leaderCommit: -1,
                        previousEntryIdentifier: undefined,
                    },
                };

                const newState = followerState({
                    commitIndex: 0,
                    log: new Log(entries),
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'persistLog',
                    },
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: true,
                            numberOfEntriesSentInRequest: 1,
                            prevLogIndexFromRequest: -1,
                            term: 0,
                        },
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });
        });

        it('ignores heartbeat message timeout timers', () => {
            const state = followerState();
            const event: Event<string> = {
                type: 'sendHeartbeatMessageTimeout',
                node: 2,
            };

            expect(reduce(event, state)).toEqual({
                newState: state,
                effects: [],
            });
        });

        it('lets the calling server know that it has an outdated term when it receives an appendEntries with lower term number', () => {
            const state = followerState({
                currentTerm: 3,
            });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 2,
                message: {
                    type: 'appendEntries',
                    previousEntryIdentifier: undefined,
                    term: 2,
                    entries: [],
                    leaderCommit: -1,
                },
            };

            const effects: Array<Effect<string>> = [
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: false,
                        prevLogIndexFromRequest: expect.any(Number),
                        term: 3,
                        numberOfEntriesSentInRequest: 0,
                    },
                },
                {
                    type: 'resetElectionTimeout',
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState: state,
                effects,
            });
        });

        describe('when it receives requestVote', () => {
            it('votes for the server if it has not voted before', () => {
                const state = followerState({
                    currentTerm: 0,
                    votedFor: undefined,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 1,
                        lastLog: undefined,
                    },
                };

                const newState = followerState({
                    currentTerm: 1,
                    votedFor: 0,
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: true,
                            term: 1,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('votes for the same server again', () => {
                const state = followerState({
                    currentTerm: 0,
                    votedFor: 0,
                    log: createLog({ term: 0, nbEntries: 1 }),
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 1,
                        lastLog: {
                            index: 0,
                            term: 0,
                        },
                    },
                };

                const newState = followerState({
                    currentTerm: 1,
                    votedFor: 0,
                    log: state.log,
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: true,
                            term: 1,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not vote for a different server this term', () => {
                const state = followerState({
                    currentTerm: 1,
                    votedFor: 0,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 1,
                    message: {
                        type: 'requestVote',
                        term: 1,
                        lastLog: undefined,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 1,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: false,
                            term: 1,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects,
                });
            });

            it('does not vote for an outdated term (e.g. very late delivery of message)', () => {
                const state = followerState({
                    currentTerm: 2,
                    votedFor: 2,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 2,
                    message: {
                        type: 'requestVote',
                        term: 0,
                        lastLog: undefined,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 2,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: false,
                            term: 2,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects,
                });
            });

            it('votes for a server with a newer term', () => {
                const state = followerState({
                    currentTerm: 1,
                    votedFor: 1,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 2,
                    message: {
                        type: 'requestVote',
                        term: 2,
                        lastLog: undefined,
                    },
                };

                const newState = followerState({
                    currentTerm: 2,
                    votedFor: 2,
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 2,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: true,
                            term: 2,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not vote if the requesters latest log term is out of date (but does update its term)', () => {
                const state = followerState({
                    log: createLog({ nbEntries: 1, term: 2 }),
                    currentTerm: 2,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        lastLog: {
                            index: 1,
                            term: 1,
                        },
                        term: 3,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: false,
                            term: state.currentTerm,
                        },
                    },
                ];
                const newState = {
                    ...state,
                    currentTerm: 3,
                };
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not vote if the requesters latest log index is out of date', () => {
                const state = followerState({
                    log: createLog({ nbEntries: 2, term: 2 }),
                    currentTerm: 2,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        lastLog: {
                            index: 0,
                            term: 2,
                        },
                        term: 3,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: false,
                            term: 2,
                        },
                    },
                ];
                const newState = {
                    ...state,
                    currentTerm: 3,
                };
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });
        });

        // Can happen when it stepped down because someone else won the vote, and other cases.
        describe('when it receives requestVoteResponse', () => {
            it('ignores it', () => {
                const state = followerState();
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 2,
                    message: {
                        type: 'requestVoteResponse',
                        voteGranted: true,
                        term: 2,
                    },
                };

                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects: [],
                });
            });
        });
    });

    describe('candidate', () => {
        it('starts a new voting term when election timeout fires', () => {
            const state = candidateState({
                currentTerm: 2,
                otherClusterNodes: [1, 2],
            });
            const event: Event<string> = {
                type: 'electionTimeout',
            };

            const newState = candidateState({
                currentTerm: 3,
                otherClusterNodes: state.otherClusterNodes,
            });
            const effects: Effect<string>[] = [
                {
                    type: 'sendMessageToNode',
                    node: 1,
                    message: {
                        type: 'requestVote',
                        term: 3,
                        lastLog: undefined,
                    },
                },
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'requestVote',
                        term: 3,
                        lastLog: undefined,
                    },
                },
                {
                    type: 'resetElectionTimeout',
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState,
                effects,
            });
        });

        it('ignores heartbeat message timeouts', () => {
            const state = candidateState();
            const event: Event<string> = {
                type: 'sendHeartbeatMessageTimeout',
                node: 2,
            };

            expect(reduce(event, state)).toEqual({
                newState: state,
                effects: [],
            });
        });

        it('transitions to follower and appends entries if it receives an appendEntries of equal or higher term', () => {
            const state = candidateState({
                currentTerm: 2,
            });
            const entries = createLogEntries({ nbEntries: 1, term: 2 });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 2,
                message: {
                    type: 'appendEntries',
                    previousEntryIdentifier: undefined,
                    term: 2,
                    entries,
                    leaderCommit: -1,
                },
            };

            const newState = followerState({
                currentTerm: 2,
                log: new Log(entries),
            });
            const effects: Array<Effect<string>> = [
                {
                    type: 'persistLog',
                },
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: true,
                        term: 2,
                        prevLogIndexFromRequest: -1,
                        numberOfEntriesSentInRequest: 1,
                    },
                },
                {
                    type: 'resetElectionTimeout',
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState,
                effects,
            });
        });

        it('sends a not ok response if it receives an appendEntries of lower term', () => {
            const state = candidateState({
                currentTerm: 1,
            });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 1,
                message: {
                    type: 'appendEntries',
                    term: 0,
                    previousEntryIdentifier: undefined,
                    entries: [],
                    leaderCommit: 1,
                },
            };

            const effects: Array<Effect<string>> = [
                {
                    type: 'sendMessageToNode',
                    node: 1,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: false,
                        term: 1,
                        prevLogIndexFromRequest: expect.any(Number),
                        numberOfEntriesSentInRequest: 0,
                    },
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState: state,
                effects,
            });
        });

        it('updates commitIndex if it receives an appendEntries', () => {
            const state = candidateState({
                currentTerm: 1,
                commitIndex: -1,
            });
            const entries = createLogEntries({ nbEntries: 1, term: 1 });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 0,
                message: {
                    type: 'appendEntries',
                    leaderCommit: 0,
                    entries,
                    term: 1,
                    previousEntryIdentifier: undefined,
                },
            };

            const newState = followerState({
                currentTerm: 1,
                log: new Log(entries),
                commitIndex: 0,
            });
            expect(reduce(event, state).newState).toEqual(newState);
        });

        describe('when it receives request vote response', () => {
            it('becomes leader when it receives a majority of the votes and appends a noop entry', () => {
                const state = candidateState({
                    currentTerm: 1,
                    otherClusterNodes: [1, 2],
                    votes: new Set(),
                    log: createLog({ nbEntries: 1, term: 0 }),
                    commitIndex: 0,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 2,
                    message: {
                        type: 'requestVoteResponse',
                        voteGranted: true,
                        term: 1,
                    },
                };

                const newState = leaderState({
                    currentTerm: 1,
                    otherClusterNodes: state.otherClusterNodes,
                    followerInfo: {
                        1: { nextIndex: 1, matchIndex: -1 },
                        2: { nextIndex: 1, matchIndex: -1 },
                    },
                    log: state.log,
                    commitIndex: state.commitIndex,
                });
                // This will trigger sending heartbeat messages
                const effects: Array<Effect<string>> = [
                    {
                        type: 'appendNoopEntryToLog',
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not count the same vote twice', () => {
                const state = candidateState({
                    currentTerm: 1,
                    otherClusterNodes: [1, 2, 3, 4],
                    votes: new Set([1]),
                });

                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 1,
                    message: {
                        type: 'requestVoteResponse',
                        voteGranted: true,
                        term: 1,
                    },
                };

                const newState = candidateState({
                    currentTerm: 1,
                    otherClusterNodes: state.otherClusterNodes,
                    votes: state.votes,
                });
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects: [],
                });
            });

            it('does not count the vote if the response is not granted', () => {
                const state = candidateState({
                    currentTerm: 3,
                    otherClusterNodes: [0, 1],
                    votes: new Set(),
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVoteResponse',
                        voteGranted: false,
                        term: 3,
                    },
                };

                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects: [],
                });
            });
        });

        describe('when it receives requestVote', () => {
            it('votes for servers of higher term and becomes follower of that term', () => {
                const state = candidateState({
                    currentTerm: 3,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 4,
                        lastLog: {
                            term: 2,
                            index: 2,
                        },
                    },
                };

                const newState = followerState({
                    currentTerm: 4,
                    votedFor: 0,
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: true,
                            term: 4,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not vote for servers of equal term', () => {
                const state = candidateState({
                    currentTerm: 3,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 3,
                        lastLog: undefined,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: false,
                            term: 3,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects,
                });
            });
        });
    });

    describe('leader', () => {
        it('ignores leader election timeouts', () => {
            const state = leaderState({
                currentTerm: 5,
            });
            const event: Event<string> = {
                type: 'electionTimeout',
            };

            expect(reduce(event, state)).toEqual({
                newState: state,
                effects: [],
            });
        });

        it('sends heartbeat messages when the timer to do so expires', () => {
            const state = leaderState({
                currentTerm: 2,
                commitIndex: 3,
            });
            const node = 2;
            const event: Event<string> = {
                type: 'sendHeartbeatMessageTimeout',
                node,
            };

            const effects: Effect<string>[] = [
                {
                    type: 'sendMessageToNode',
                    node,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        previousEntryIdentifier: undefined,
                        entries: [],
                        leaderCommit: 3,
                    },
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState: state,
                effects,
            });
        });

        describe('if a node replies that appendEntries is not ok', () => {
            it('decrements lastIndex and sends the relevant parts of the log', () => {
                const state = leaderState({
                    currentTerm: 2,
                    log: new Log<string>([
                        {
                            type: 'value',
                            value: 'x <- 1',
                            term: 1,
                            id: {
                                clientId: 1,
                                requestSerial: 1,
                            },
                        },
                        {
                            type: 'value',
                            value: 'y <- 2',
                            term: 2,
                            id: {
                                clientId: 1,
                                requestSerial: 2,
                            },
                        },
                    ]),
                    commitIndex: 0,
                });
                const node = 4;
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: false,
                        prevLogIndexFromRequest: 1,
                        term: 2,
                        numberOfEntriesSentInRequest: 0,
                    },
                };

                const newState = leaderState({
                    ...state,
                    followerInfo: {
                        [node]: {
                            nextIndex: 1,
                            matchIndex: -1,
                        },
                    },
                });
                const effects: Effect<string>[] = [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntries',
                            term: 2,
                            previousEntryIdentifier: {
                                index: 0,
                                term: 1,
                            },
                            entries: [
                                {
                                    type: 'value',
                                    value: 'y <- 2',
                                    term: 2,
                                    id: {
                                        clientId: 1,
                                        requestSerial: 2,
                                    },
                                },
                            ],
                            leaderCommit: 0,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('uses null as an indicator of the beginning of the log', () => {
                const entries = createLogEntries({ nbEntries: 2, term: 2 });
                const state = leaderState({
                    currentTerm: 2,
                    log: new Log<string>(entries),
                });
                const node = 4;
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: false,
                        prevLogIndexFromRequest: 0,
                        term: 2,
                        numberOfEntriesSentInRequest: 0,
                    },
                };

                const newState = leaderState({
                    ...state,
                    followerInfo: {
                        [node]: {
                            nextIndex: 0,
                            matchIndex: -1,
                        },
                    },
                });
                const effects: Effect<string>[] = [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntries',
                            term: 2,
                            previousEntryIdentifier: undefined,
                            entries,
                            leaderCommit: -1,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('steps down if the term of the response is higher', () => {
                const state = leaderState({
                    currentTerm: 1,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 1,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: false,
                        term: 2,
                        numberOfEntriesSentInRequest: 0,
                        prevLogIndexFromRequest: -1,
                    },
                };

                const newState = followerState({
                    currentTerm: 2,
                });
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects: [],
                });
            });
        });

        describe('when it receives that appendEntries is ok', () => {
            type TestCase = {
                logLength: number;
                numberOfEntriesSent: number;
                previousEntryIndex: number;
                expectedNextIndex: number;
                expectedMatchIndex: number;
            };

            const testCases: TestCase[] = [
                {
                    logLength: 0,
                    numberOfEntriesSent: 0,
                    previousEntryIndex: -1,
                    expectedNextIndex: 0,
                    expectedMatchIndex: -1,
                },
                {
                    logLength: 1,
                    numberOfEntriesSent: 1,
                    previousEntryIndex: -1,
                    expectedNextIndex: 1,
                    expectedMatchIndex: 0,
                },
                {
                    logLength: 1,
                    numberOfEntriesSent: 0,
                    previousEntryIndex: 0,
                    expectedNextIndex: 1,
                    expectedMatchIndex: 0,
                },
                {
                    logLength: 2,
                    numberOfEntriesSent: 1,
                    previousEntryIndex: -1,
                    expectedNextIndex: 1,
                    expectedMatchIndex: 0,
                },
                {
                    logLength: 2,
                    numberOfEntriesSent: 1,
                    previousEntryIndex: 0,
                    expectedNextIndex: 2,
                    expectedMatchIndex: 1,
                },
                {
                    logLength: 2,
                    numberOfEntriesSent: 2,
                    previousEntryIndex: -1,
                    expectedNextIndex: 2,
                    expectedMatchIndex: 1,
                },
            ];

            it.each(testCases)(
                'sets the nextIndex properly in all cases',
                ({
                    logLength,
                    numberOfEntriesSent,
                    previousEntryIndex,
                    expectedNextIndex,
                    expectedMatchIndex,
                }) => {
                    const state = leaderState({
                        log: new Log(
                            Array(logLength).fill({ term: 1, value: 'x <- 1' }),
                        ),
                    });
                    const event: Event<string> = {
                        type: 'receivedMessageFromNode',
                        node: 2,
                        message: {
                            type: 'appendEntriesResponse',
                            ok: true,
                            term: state.currentTerm,
                            prevLogIndexFromRequest: previousEntryIndex,
                            numberOfEntriesSentInRequest: numberOfEntriesSent,
                        },
                    };

                    const { newState } = reduce(event, state);

                    expect(
                        newState.type === 'leader' &&
                            newState.followerInfo[2]?.nextIndex,
                    ).toEqual(expectedNextIndex);
                    expect(
                        newState.type === 'leader' &&
                            newState.followerInfo[2]?.matchIndex,
                    ).toEqual(expectedMatchIndex);
                },
            );

            it('never decreases matchIndex', () => {
                const state = leaderState({
                    log: new Log(Array(3).fill({ term: 1, value: 'x <- 1' })),
                    followerInfo: {
                        2: {
                            nextIndex: -1,
                            matchIndex: 2,
                        },
                        1: {
                            nextIndex: 3,
                            matchIndex: 2,
                        },
                    },
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 2,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: true,
                        prevLogIndexFromRequest: -1,
                        numberOfEntriesSentInRequest: 1,
                        term: 0,
                    },
                };

                const newState = {
                    ...state,
                    followerInfo: {
                        ...state.followerInfo,
                        2: {
                            nextIndex: 1,
                            matchIndex: 2,
                        },
                    },
                };
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects: [],
                });
            });

            it('steps down if the term of the response is higher', () => {
                const state = leaderState({
                    currentTerm: 1,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 1,
                    message: {
                        type: 'appendEntriesResponse',
                        prevLogIndexFromRequest: -1,
                        ok: true,
                        term: 2,
                        numberOfEntriesSentInRequest: 0,
                    },
                };

                const newState = followerState({
                    currentTerm: 2,
                });
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects: [],
                });
            });

            it('updates commitIndex if necessary and sets hasCommittedEntriesThisTerm', () => {
                const state = leaderState({
                    currentTerm: 1,
                    log: createLog({ nbEntries: 2, term: 1 }),
                    commitIndex: -1,
                    followerInfo: {
                        1: {
                            nextIndex: -1,
                            matchIndex: -1,
                        },
                        2: {
                            nextIndex: -1,
                            matchIndex: -1,
                        },
                    },
                    hasCommittedEntryThisTerm: false,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 1,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: true,
                        numberOfEntriesSentInRequest: 2,
                        prevLogIndexFromRequest: -1,
                        term: 1,
                    },
                };

                const newState = leaderState({
                    ...state,
                    followerInfo: {
                        ...state.followerInfo,
                        1: {
                            nextIndex: 2,
                            matchIndex: 1,
                        },
                    },
                    commitIndex: 1,
                    hasCommittedEntryThisTerm: true,
                });
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects: [],
                });
            });
        });

        it('transitions to follower if it receives an appendEntries of higher term', () => {
            const state = leaderState({
                currentTerm: 1,
            });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 1,
                message: {
                    type: 'appendEntries',
                    term: 2,
                    entries: [],
                    previousEntryIdentifier: undefined,
                    leaderCommit: 3,
                },
            };

            const newState = followerState({
                currentTerm: 2,
            });
            expect(reduce(event, state)).toEqual({
                newState,
                effects: [],
            });
        });

        it('crashes (?) if if receives an appendEntries of equal term (should be unreachable)', () => {
            const state = leaderState({
                currentTerm: 1,
            });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 1,
                message: {
                    type: 'appendEntries',
                    term: 1,
                    entries: [],
                    previousEntryIdentifier: undefined,
                    leaderCommit: -1,
                },
            };

            expect(() => {
                reduce(event, state);
            }).toThrowErrorMatchingInlineSnapshot(
                '"unreachable: a node thinks it is leader of the same term as this node"',
            );
        });

        it('replies false if receives an appendEntries with a lower term', () => {
            // It will send a heartbeat soon anyway. An optimization would be to immediately send appendEntries.
            const state = leaderState({
                currentTerm: 1,
            });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 1,
                message: {
                    type: 'appendEntries',
                    term: 0,
                    entries: [],
                    previousEntryIdentifier: undefined,
                    leaderCommit: -1,
                },
            };

            const effects: Array<Effect<string>> = [
                {
                    type: 'sendMessageToNode',
                    node: 1,
                    message: {
                        type: 'appendEntriesResponse',
                        ok: false,
                        term: 1,
                        numberOfEntriesSentInRequest: 0,
                        prevLogIndexFromRequest: -1,
                    },
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState: state,
                effects,
            });
        });

        describe('when it receives requestVote', () => {
            it('votes for servers of higher term and becomes follower of that term', () => {
                const state = leaderState({
                    currentTerm: 3,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 4,
                        lastLog: undefined,
                    },
                };

                const newState = followerState({
                    currentTerm: 4,
                    votedFor: 0,
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: true,
                            term: 4,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('does not vote for servers of equal term', () => {
                const state = leaderState({
                    currentTerm: 3,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 3,
                        lastLog: undefined,
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 0,
                        message: {
                            type: 'requestVoteResponse',
                            voteGranted: false,
                            term: 3,
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects,
                });
            });
        });

        it('appends to the log when receiving clientAppendToLog', () => {
            const initialEntries = createLogEntries({ nbEntries: 1, term: 1 });
            const state = leaderState({
                currentTerm: 2,
                log: new Log(initialEntries),
                otherClusterNodes: [0, 2],
                followerInfo: {
                    0: { nextIndex: 1, matchIndex: 0 },
                    2: { nextIndex: 0, matchIndex: 0 },
                },
                commitIndex: 0,
            });
            const event: Event<string> = {
                type: 'appendToLog',
                entry: {
                    type: 'value',
                    value: 'y <- 3',
                    id: {
                        clientId: 12,
                        requestSerial: 34,
                    },
                },
            };

            const entry: Entry<string> = {
                term: 2,
                type: 'value',
                value: 'y <- 3',
                id: {
                    clientId: 12,
                    requestSerial: 34,
                },
            };
            const newState = leaderState({
                ...state,
                log: new Log([...initialEntries, entry]),
            });
            const effects: Array<Effect<string>> = [
                {
                    type: 'persistLog',
                },
                {
                    type: 'sendMessageToNode',
                    node: 0,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        entries: [entry],
                        previousEntryIdentifier: {
                            term: 1,
                            index: 0,
                        },
                        leaderCommit: 0,
                    },
                },
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        entries: [...initialEntries, entry],
                        previousEntryIdentifier: undefined,
                        leaderCommit: 0,
                    },
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState,
                effects,
            });
        });

        describe('when it receives requestVoteResponse', () => {
            it('ignores it, because it is already leader', () => {
                const state = leaderState({
                    currentTerm: 2,
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 2,
                    message: {
                        type: 'requestVoteResponse',
                        voteGranted: true,
                        term: 2,
                    },
                };

                expect(reduce(event, state)).toEqual({
                    newState: state,
                    effects: [],
                });
            });
        });
    });
});
