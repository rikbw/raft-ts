import {
    Event,
    reduce,
    Effect,
    FollowerState,
    CandidateState,
    LeaderState,
    NodeMessage,
} from './state';
import { Log } from './log';

const followerState = ({
    currentTerm = 0,
    log = new Log([]),
    otherClusterNodes = [],
    votedFor = undefined,
}: Partial<FollowerState<string>> = {}): FollowerState<string> => ({
    type: 'follower',
    currentTerm,
    log,
    otherClusterNodes,
    votedFor,
});

const candidateState = ({
    currentTerm = 0,
    log = new Log([]),
    otherClusterNodes = [],
    votes = new Set(),
}: Partial<CandidateState<string>> = {}): CandidateState<string> => ({
    type: 'candidate',
    currentTerm,
    log,
    otherClusterNodes,
    votes,
});

const leaderState = ({
    currentTerm = 0,
    log = new Log([]),
    followerInfo = {},
    otherClusterNodes = [],
}: Partial<LeaderState<string>> = {}): LeaderState<string> => ({
    type: 'leader',
    currentTerm,
    log,
    followerInfo,
    otherClusterNodes,
});

describe('state', () => {
    describe('follower', () => {
        it('transitions to candidate and requests votes when election timeout fires', () => {
            const state = followerState({
                currentTerm: 0,
                otherClusterNodes: [0, 2],
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
                    },
                },
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'requestVote',
                        term: 1,
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
                    },
                };

                const newState = followerState({
                    currentTerm: 3,
                });
                const effects: Effect<string>[] = [
                    {
                        type: 'sendMessageToNode',
                        message: {
                            type: 'appendEntriesResponseOk',
                            prevLogIndexFromRequest: -1,
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
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        previousEntryIdentifier: undefined,
                        entries: [
                            {
                                term: 1,
                                value: 'w <- 2',
                            },
                            {
                                term: 1,
                                value: 'x <- 4',
                            },
                        ],
                    },
                };

                const newState = followerState({
                    ...state,
                    log: new Log([
                        {
                            term: 1,
                            value: 'w <- 2',
                        },
                        {
                            term: 1,
                            value: 'x <- 4',
                        },
                    ]),
                });
                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntriesResponseOk',
                            prevLogIndexFromRequest: -1,
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
                    log: new Log([
                        {
                            term: 1,
                            value: 'x <- 2',
                        },
                    ]),
                });
                const node = 1;
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        previousEntryIdentifier: {
                            term: 2,
                            index: 0,
                        },
                        entries: [],
                    },
                };

                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node,
                        message: {
                            type: 'appendEntriesResponseNotOk',
                            prevLogIndexFromRequest: 0,
                            term: 2,
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
        });

        it('does not expect a timer to expire to send heartbeat messages', () => {
            const state = followerState();
            const event: Event<string> = {
                type: 'sendHeartbeatMessageTimeout',
                node: 2,
            };

            expect(() =>
                reduce(event, state),
            ).toThrowErrorMatchingInlineSnapshot(
                '"unreachable: did not expect a send heartbeat message timer to timeout in this state"',
            );
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
                },
            };

            const effects: Array<Effect<string>> = [
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'appendEntriesResponseNotOk',
                        prevLogIndexFromRequest: expect.any(Number),
                        term: 3,
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
                });
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node: 0,
                    message: {
                        type: 'requestVote',
                        term: 1,
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
                    },
                },
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'requestVote',
                        term: 3,
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

        it('does not expect a timer to expire to send heartbeat messages', () => {
            const state = candidateState();
            const event: Event<string> = {
                type: 'sendHeartbeatMessageTimeout',
                node: 2,
            };

            expect(() =>
                reduce(event, state),
            ).toThrowErrorMatchingInlineSnapshot(
                '"unreachable: did not expect a send heartbeat message timer to timeout in this state"',
            );
        });

        it('transitions to follower if it receives an appendEntries of equal or higher term', () => {
            const state = candidateState({
                currentTerm: 2,
            });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 2,
                message: {
                    type: 'appendEntries',
                    previousEntryIdentifier: undefined,
                    term: 2,
                    entries: [],
                },
            };

            const newState = followerState({
                currentTerm: 2,
            });
            const effects: Array<Effect<string>> = [
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'appendEntriesResponseOk',
                        prevLogIndexFromRequest: expect.any(Number),
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
                },
            };

            const effects: Array<Effect<string>> = [
                {
                    type: 'sendMessageToNode',
                    node: 1,
                    message: {
                        type: 'appendEntriesResponseNotOk',
                        term: 1,
                        prevLogIndexFromRequest: expect.any(Number),
                    },
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState: state,
                effects,
            });
        });

        describe('when it receives request vote response', () => {
            it('becomes leader when it receives a majority of the votes', () => {
                const state = candidateState({
                    currentTerm: 1,
                    otherClusterNodes: [1, 2],
                    votes: new Set([1]),
                    log: new Log([{ term: 0, value: 'x <- 2' }]),
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
                        1: { nextIndex: 1 },
                        2: { nextIndex: 1 },
                    },
                    log: state.log,
                });
                const message: NodeMessage<string> = {
                    type: 'appendEntries',
                    entries: [],
                    term: 1,
                    previousEntryIdentifier: {
                        term: 0,
                        index: 0,
                    },
                };
                const effects: Array<Effect<string>> = [
                    {
                        type: 'sendMessageToNode',
                        node: 1,
                        message,
                    },
                    {
                        type: 'sendMessageToNode',
                        node: 2,
                        message,
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
                    otherClusterNodes: [1, 2],
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
        it('does not expect an election timeout', () => {
            const state = leaderState({
                currentTerm: 5,
            });
            const event: Event<string> = {
                type: 'electionTimeout',
            };

            expect(() => {
                reduce(event, state);
            }).toThrowErrorMatchingInlineSnapshot(
                '"unreachable: election timeout should not fire when you are a leader"',
            );
        });

        it('sends heartbeat messages when the timer to do so expires', () => {
            const state = leaderState({
                currentTerm: 2,
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
                            value: 'x <- 1',
                            term: 1,
                        },
                        {
                            value: 'y <- 2',
                            term: 2,
                        },
                    ]),
                });
                const node = 4;
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntriesResponseNotOk',
                        prevLogIndexFromRequest: 1,
                        term: 2,
                    },
                };

                const newState = leaderState({
                    ...state,
                    followerInfo: {
                        [node]: {
                            nextIndex: 1,
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
                                    value: 'y <- 2',
                                    term: 2,
                                },
                            ],
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it('uses null as an indicator of the beginning of the log', () => {
                const state = leaderState({
                    currentTerm: 2,
                    log: new Log<string>([
                        {
                            value: 'x <- 1',
                            term: 1,
                        },
                        {
                            value: 'y <- 2',
                            term: 2,
                        },
                    ]),
                });
                const node = 4;
                const event: Event<string> = {
                    type: 'receivedMessageFromNode',
                    node,
                    message: {
                        type: 'appendEntriesResponseNotOk',
                        prevLogIndexFromRequest: 0,
                        term: 2,
                    },
                };

                const newState = leaderState({
                    ...state,
                    followerInfo: {
                        [node]: {
                            nextIndex: 0,
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
                            entries: [
                                {
                                    value: 'x <- 1',
                                    term: 1,
                                },
                                {
                                    value: 'y <- 2',
                                    term: 2,
                                },
                            ],
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });
        });

        // TODO this isn't entirely correct: this is only correct when appending zero entries.
        // So we need to know how many entries were appended as well.
        it('updates follower state when it receives that appendEntries is ok', () => {
            const state = leaderState({
                followerInfo: {
                    2: {
                        nextIndex: 3,
                    },
                },
            });
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 2,
                message: {
                    type: 'appendEntriesResponseOk',
                    prevLogIndexFromRequest: 2,
                },
            };

            const newState = leaderState({
                followerInfo: {
                    2: {
                        nextIndex: 4,
                    },
                },
            });
            expect(reduce(event, state)).toEqual({
                newState: newState,
                effects: [],
            });
        });

        it.todo(
            'transitions to follower if it receives an appendEntries of higher term',
        );

        it.todo(
            'crashes (?) if if receives an appendEntries of equal term (should be unreachable)',
        );

        it.todo(
            'sends an empty appendEntries if if receives an appendEntries with a lower term',
        );

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
            const state = leaderState({
                currentTerm: 2,
                log: new Log([
                    {
                        term: 1,
                        value: 'x <- 2',
                    },
                ]),
                otherClusterNodes: [0, 2],
                followerInfo: {
                    0: { nextIndex: 1 },
                    2: { nextIndex: 0 },
                },
            });
            const event: Event<string> = {
                type: 'clientAppendToLog',
                value: 'y <- 3',
            };

            const newState = leaderState({
                ...state,
                log: new Log([
                    {
                        term: 1,
                        value: 'x <- 2',
                    },
                    {
                        term: 2,
                        value: 'y <- 3',
                    },
                ]),
            });
            const effects: Array<Effect<string>> = [
                {
                    type: 'sendMessageToNode',
                    node: 0,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        entries: [
                            {
                                value: 'y <- 3',
                                term: 2,
                            },
                        ],
                        previousEntryIdentifier: {
                            term: 1,
                            index: 0,
                        },
                    },
                },
                {
                    type: 'sendMessageToNode',
                    node: 2,
                    message: {
                        type: 'appendEntries',
                        term: 2,
                        entries: [
                            {
                                value: 'x <- 2',
                                term: 1,
                            },
                            {
                                value: 'y <- 3',
                                term: 2,
                            },
                        ],
                        previousEntryIdentifier: undefined,
                    },
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState,
                effects,
            });
        });
    });
});
