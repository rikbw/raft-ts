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

type MutableEvent = {
    type: 'electionTimeout';
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

type ReducerResult = [State, Effect[]];

export function reduce(event: Event, state: State): ReducerResult {
    switch (event.type) {
        case 'electionTimeout':
            return reduceElectionTimeout(state);

        default:
            return unreachable(event.type);
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
            return [
                {
                    type: 'candidate',
                    currentTerm: newTerm,
                },
                [
                    {
                        type: 'broadcastRequestVote',
                        term: newTerm,
                    },
                    {
                        type: 'resetElectionTimeout',
                    },
                ],
            ];
        }

        default:
            return unreachable(state);
    }
}
