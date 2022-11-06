import { Entry, Log } from './log';
import { LeaderState } from './state';
import { array, function as func } from 'fp-ts';
import { commitIndexFromState } from './commitIndex';

const id = {
    clientId: 1,
    requestSerial: 2,
};

const logEntry = (value: string) => ({ type: 'value' as const, value, id });

describe('commitIndex', () => {
    type TestCase = {
        followerMatchIndex: Record<number, number>;
        logEntries: Array<Entry<string>>;
        currentTerm: number;
        currentCommitIndex: number;
        expectedCommitIndex: number;
    };

    const cases: TestCase[] = [
        {
            currentTerm: 3,
            logEntries: [
                {
                    term: 1,
                    ...logEntry('x <- 2'),
                },
                {
                    term: 2,
                    ...logEntry('x <- 2'),
                },
            ],
            currentCommitIndex: 0,
            followerMatchIndex: {
                1: 1,
                2: 1,
            },
            // Entry at index 1 is not of the current term.
            expectedCommitIndex: 0,
        },
        {
            currentTerm: 3,
            logEntries: [
                {
                    term: 1,
                    ...logEntry('x <- 2'),
                },
                {
                    term: 3,
                    ...logEntry('x <- 3'),
                },
            ],
            currentCommitIndex: 0,
            followerMatchIndex: {
                1: -1,
                2: 1,
            },
            // Entry is replicated at a majority of nodes and of the current term.
            expectedCommitIndex: 1,
        },
        {
            currentTerm: 2,
            logEntries: [],
            currentCommitIndex: -1,
            followerMatchIndex: {
                1: -1,
                2: -1,
            },
            // Edge case
            expectedCommitIndex: -1,
        },
        {
            currentTerm: 3,
            logEntries: [
                {
                    term: 1,
                    ...logEntry('x <- 2'),
                },
                {
                    term: 2,
                    ...logEntry('x <- 3'),
                },
                {
                    term: 3,
                    ...logEntry('x <- 4'),
                },
            ],
            currentCommitIndex: -1,
            followerMatchIndex: {
                1: -1,
                2: 2,
                3: 1,
                4: 0,
            },
            // Can't consider any logs committed since the term of the logs is too low.
            expectedCommitIndex: -1,
        },
        {
            currentTerm: 1,
            logEntries: [
                {
                    term: 1,
                    ...logEntry('x <- 2'),
                },
                {
                    term: 1,
                    ...logEntry('x <- 3'),
                },
                {
                    term: 1,
                    ...logEntry('x <- 4'),
                },
            ],
            currentCommitIndex: -1,
            followerMatchIndex: {
                1: -1,
                2: 2,
                3: 1,
                4: 0,
            },
            // Highest index that has been replicated on a majority, of this term.
            expectedCommitIndex: 1,
        },
    ];

    it.each(cases)(
        'correctly calculates the next commit index for the updated state',
        ({
            currentTerm,
            logEntries,
            currentCommitIndex,
            followerMatchIndex,
            expectedCommitIndex,
        }) => {
            const followerInfo = func.pipe(
                followerMatchIndex,
                Object.entries,
                array.map(([key, matchIndex]) => [
                    key,
                    { matchIndex, nextIndex: 0 },
                ]),
                Object.fromEntries,
            );
            const state: LeaderState<string> = {
                currentTerm,
                log: new Log(logEntries),
                followerInfo,
                commitIndex: currentCommitIndex,
                type: 'leader',
                // Not used
                otherClusterNodes: [],
                hasCommittedEntryThisTerm: false,
            };

            expect(commitIndexFromState(state)).toEqual(expectedCommitIndex);
        },
    );
});
