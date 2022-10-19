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
}: Partial<LeaderState<string>> = {}): LeaderState<string> => ({
    type: 'leader',
    currentTerm,
    log,
});

describe('state', () => {
    describe('follower', () => {
        it('transitions to candidate and requests votes when election timeout fires', () => {
            const state = followerState({
                currentTerm: 0,
            });
            const event: Event = {
                type: 'electionTimeout',
            };

            const newState = candidateState({
                currentTerm: 1,
                log: state.log,
            });
            const effects: Effect[] = [
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
                const event: Event = {
                    type: 'receivedAppendEntries',
                    term: 3,
                };

                const newState = followerState({
                    currentTerm: 3,
                });
                const effects: Effect[] = [
                    {
                        type: 'sendAppendEntriesResponse',
                        ok: true,
                        term: 3,
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it.todo('resets its election timeout');
        });

        it('does not expect a timer to expire to send heartbeat messages', () => {
            const state = followerState();
            const event: Event = {
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
            const event: Event = {
                type: 'electionTimeout',
            };

            const newState = candidateState({
                currentTerm: 3,
            });
            const effects: Effect[] = [
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
            const event: Event = {
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
            const event: Event = {
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
            const event: Event = {
                type: 'sendHeartbeatMessageTimeout',
                node,
            };

            const effects: Effect[] = [
                {
                    type: 'resetSendHeartbeatMessageTimeout',
                    node,
                },
                {
                    type: 'sendAppendEntries',
                    term: 2,
                    node,
                },
            ];
            expect(reduce(event, state)).toEqual({
                newState: state,
                effects,
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
