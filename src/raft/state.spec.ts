import {
    Event,
    reduce,
    Effect,
    FollowerState,
    CandidateState,
    LeaderState,
} from './state';
import { Log } from './log';

const followerState = ({
    currentTerm = 0,
    log = new Log([]),
}: Partial<FollowerState<string>> = {}): FollowerState<string> => ({
    type: 'follower',
    currentTerm,
    log,
});

const candidateState = ({
    currentTerm = 0,
    log = new Log([]),
}: Partial<CandidateState<string>> = {}): CandidateState<string> => ({
    type: 'candidate',
    currentTerm,
    log,
});

const leaderState = ({
    currentTerm = 0,
    log = new Log([]),
    followerInfo = {},
}: Partial<LeaderState<string>> = {}): LeaderState<string> => ({
    type: 'leader',
    currentTerm,
    log,
    followerInfo,
});

describe('state', () => {
    describe('follower', () => {
        it('transitions to candidate and requests votes when election timeout fires', () => {
            const state = followerState({
                currentTerm: 0,
            });
            const event: Event<string> = {
                type: 'electionTimeout',
            };

            const newState = candidateState({
                currentTerm: 1,
                log: state.log,
            });
            const effects: Effect<string>[] = [
                {
                    type: 'broadcastRequestVote',
                    term: 1,
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
                        message: { type: 'appendEntriesResponseOk' },
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
                            prevLogIndex: 0,
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

        // A mechanism of sending a response should somehow be in the event.
        it.todo(
            'lets the calling server know that it has an outdated term when it receives an appendEntries with lower term number',
        );
    });

    describe('candidate', () => {
        it('starts a new voting term when election timeout fires', () => {
            const state = candidateState({
                currentTerm: 2,
            });
            const event: Event<string> = {
                type: 'electionTimeout',
            };

            const newState = candidateState({
                currentTerm: 3,
            });
            const effects: Effect<string>[] = [
                {
                    type: 'broadcastRequestVote',
                    term: 3,
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

        it.todo(
            'transitions to follower if it receives an appendEntries of equal or higher term',
        );

        it.todo(
            '(? todo verify this) sends a requestVote message if it receives an appendEntries of lower term',
        );

        it.todo('resets its election timeout if it receives appendEntries');
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
                    type: 'resetSendHeartbeatMessageTimeout',
                    node,
                },
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
                        prevLogIndex: 1,
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
                        type: 'resetSendHeartbeatMessageTimeout',
                        node,
                    },
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
                        prevLogIndex: 0,
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
                        type: 'resetSendHeartbeatMessageTimeout',
                        node,
                    },
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

        it('does nothing when it receives that appendEntries is ok', () => {
            const state = leaderState();
            const event: Event<string> = {
                type: 'receivedMessageFromNode',
                node: 2,
                message: {
                    type: 'appendEntriesResponseOk',
                },
            };

            expect(reduce(event, state)).toEqual({
                newState: state,
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
    });
});
