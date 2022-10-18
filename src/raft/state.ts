import { unreachable } from '../util/unreachable';

type MutableState =
    | {
          type: 'follower';
          currentTerm: number;
      }
    | {
          type: 'leader';
          currentTerm: number;
      }
    | {
          type: 'candidate';
          currentTerm: number;
      };

export type State = Readonly<MutableState>;

export const initialState: State = {
    type: 'follower',
    currentTerm: 0,
};

type MutableEvent =
    | {
          type: 'electionTimeout';
      }
    | {
          type: 'receivedAppendEntries';
          term: number;
      };

export type Event = Readonly<MutableEvent>;

type MutableEffect =
    | {
          type: 'resetElectionTimeout';
      }
    | {
          type: 'broadcastRequestVote';
          term: number;
      };

export type Effect = Readonly<MutableEffect>;

type ReducerResult = {
    newState: State;
    effects: Effect[];
};

export function reduce(event: Event, state: State): ReducerResult {
    switch (event.type) {
        case 'electionTimeout':
            return reduceElectionTimeout(state);

        case 'receivedAppendEntries':
            return reduceReceivedAppendEntries(state, event.term);

        default:
            return unreachable(event);
    }
}

function reduceElectionTimeout(state: State): ReducerResult {
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

function reduceReceivedAppendEntries(
    state: State,
    term: number,
): ReducerResult {
    switch (state.type) {
        case 'follower': {
            if (term > state.currentTerm) {
                return {
                    newState: {
                        type: 'follower',
                        currentTerm: term,
                    },
                    effects: [],
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
