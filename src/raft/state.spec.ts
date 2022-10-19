import { State, Event, reduce, Effect } from './state';

describe('state', () => {
    describe('follower', () => {
        it('transitions to candidate and requests votes when election timeout fires', () => {
            const state: State = {
                type: 'follower',
                currentTerm: 0,
            };
            const event: Event = {
                type: 'electionTimeout',
            };

            const newState: State = {
                type: 'candidate',
                currentTerm: 1,
            };
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
                const state: State = {
                    type: 'follower',
                    currentTerm: 2,
                };
                const event: Event = {
                    type: 'receivedAppendEntries',
                    term: 3,
                    requestId: 23,
                };

                const newState: State = {
                    type: 'follower',
                    currentTerm: 3,
                };
                const effects: Effect[] = [
                    {
                        type: 'response',
                        requestId: 23,
                        result: {
                            type: 'appendEntriesResult',
                            ok: true
                        },
                    },
                ];
                expect(reduce(event, state)).toEqual({
                    newState,
                    effects,
                });
            });

            it.todo('resets its election timeout');
        });

        // A mechanism of sending a response should somehow be in the event.
        it.todo(
            'lets the calling server know that it has an outdated term when it receives an appendEntries with lower term number',
        );
    });

    describe('candidate', () => {
        it('starts a new voting term when election timeout fires', () => {
            const state: State = {
                type: 'candidate',
                currentTerm: 2,
            };
            const event: Event = {
                type: 'electionTimeout',
            };

            const newState: State = {
                type: 'candidate',
                currentTerm: 3,
            };
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
            const state: State = {
                type: 'leader',
                currentTerm: 5,
            };
            const event: Event = {
                type: 'electionTimeout',
            };

            expect(() => {
                reduce(event, state);
            }).toThrowErrorMatchingInlineSnapshot(
                '"unreachable: election timeout should not fire when you are a leader"',
            );
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
