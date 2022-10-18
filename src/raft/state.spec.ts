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
    });
});
