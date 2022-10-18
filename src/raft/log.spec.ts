import { Log } from './log';

describe('Log', () => {
    describe('appendEntries', () => {
        it('accepts the entries if the previousIndex is the same', () => {
            const log = new Log([
                {
                    term: 1,
                    value: 'x <- 1',
                },
            ]);

            const result = log.appendEntries({
                previousEntryIdentifier: {
                    term: 1,
                    index: 0,
                },
                entries: [
                    {
                        term: 2,
                        value: 'y <- 2',
                    },
                ],
            });

            expect(result).toEqual(true);
            expect(log.getEntries()).toEqual([
                {
                    term: 1,
                    value: 'x <- 1',
                },
                {
                    term: 2,
                    value: 'y <- 2',
                },
            ]);
        });

        it('accepts the entries if the log is empty and previousIndex is undefined', () => {
            const log = new Log([]);

            const result = log.appendEntries({
                previousEntryIdentifier: undefined,
                entries: [
                    {
                        term: 3,
                        value: 'x <- 8',
                    },
                    {
                        term: 3,
                        value: 'y <- 12',
                    },
                ],
            });

            expect(result).toEqual(true);
            expect(log.getEntries()).toEqual([
                {
                    term: 3,
                    value: 'x <- 8',
                },
                {
                    term: 3,
                    value: 'y <- 12',
                },
            ]);
        });

        it('is idempotent', () => {
            const initialEntries = [
                {
                    term: 1,
                    value: 'x <- 1',
                },
            ];
            const log = new Log(initialEntries);

            const results = Array(5)
                .fill(null)
                .map(() =>
                    log.appendEntries({
                        previousEntryIdentifier: {
                            term: 1,
                            index: 0,
                        },
                        entries: [
                            {
                                value: 'x <- 2',
                                term: 1,
                            },
                            {
                                term: 1,
                                value: 'y <- 3',
                            },
                        ],
                    }),
                );

            expect(results).toEqual(Array(5).fill(true));
            expect(log.getEntries()).toEqual([
                ...initialEntries,
                {
                    value: 'x <- 2',
                    term: 1,
                },
                {
                    term: 1,
                    value: 'y <- 3',
                },
            ]);
        });

        it('overwrites the log if the previousEntryIdentifier is somewhere in the list and the log has entries after it', () => {
            const log = new Log([
                {
                    term: 1,
                    value: 'x <- 1',
                },
                {
                    term: 1,
                    value: 'y <- 1',
                },
                {
                    term: 1,
                    value: 'z <- 1',
                },
            ]);

            const result = log.appendEntries({
                previousEntryIdentifier: {
                    index: 0,
                    term: 1,
                },
                entries: [
                    {
                        term: 2,
                        value: 'a <- 2',
                    },
                    {
                        term: 2,
                        value: 'b <- 3',
                    },
                ],
            });

            expect(result).toEqual(true);
            expect(log.getEntries()).toEqual([
                {
                    term: 1,
                    value: 'x <- 1',
                },
                {
                    term: 2,
                    value: 'a <- 2',
                },
                {
                    term: 2,
                    value: 'b <- 3',
                },
            ]);
        });

        it('overwrites the whole log if the previousEntryIdentifier is undefined', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            const result = log.appendEntries({
                previousEntryIdentifier: undefined,
                entries: [
                    {
                        term: 3,
                        value: 'x <- 1',
                    },
                ],
            });

            expect(result).toEqual(true);
            expect(log.getEntries()).toEqual([{ term: 3, value: 'x <- 1' }]);
        });

        it('rejects the entries if the term of the previous entry identifier is not correct', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            const result = log.appendEntries({
                previousEntryIdentifier: {
                    term: 2,
                    index: 0,
                },
                entries: [
                    {
                        term: 4,
                        value: 'w <- 34',
                    },
                ],
            });

            expect(result).toEqual(false);
            expect(log.getEntries()).toEqual(initialEntries);
        });

        it('rejects the entries when the index of the previous identifier is not in the log', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            const result = log.appendEntries({
                previousEntryIdentifier: {
                    term: 3,
                    index: 10,
                },
                entries: [
                    {
                        term: 4,
                        value: 'w <- 34',
                    },
                ],
            });

            expect(result).toEqual(false);
            expect(log.getEntries()).toEqual(initialEntries);
        });

        it('returns if the previous entry identifier is correct when entries is empty', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            expect(
                log.appendEntries({
                    previousEntryIdentifier: {
                        term: 0,
                        index: 3,
                    },
                    entries: [],
                }),
            ).toEqual(false);
            expect(
                log.appendEntries({
                    previousEntryIdentifier: {
                        term: 3,
                        index: 10,
                    },
                    entries: [],
                }),
            ).toEqual(false);

            expect(log.getEntries()).toEqual(initialEntries);
        });
    });
});
