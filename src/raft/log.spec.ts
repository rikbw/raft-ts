import { Log } from './log';

describe('Log', () => {
    describe('appendEntries', () => {
        it('accepts the entries if the previousIndex is the same', () => {
            const log = new Log<string>([
                {
                    term: 1,
                    value: 'x <- 1',
                },
            ]);

            const { ok, newLog } = log.appendEntries({
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

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
                {
                    term: 1,
                    value: 'x <- 1',
                },
                {
                    term: 2,
                    value: 'y <- 2',
                },
            ]);
            expect(log.getEntries()).toEqual([
                {
                    term: 1,
                    value: 'x <- 1',
                },
            ]);
        });

        it('accepts the entries if the log is empty and previousIndex is undefined', () => {
            const log = new Log([]);

            const { ok, newLog } = log.appendEntries({
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

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
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

            const { ok, log: newLog } = Array(5)
                .fill(null)
                .reduce(
                    ({ log, ok }: { log: Log<string>; ok: boolean }) => {
                        const { ok: newOk, newLog } = log.appendEntries({
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
                        });

                        return {
                            ok: ok && newOk,
                            log: newLog,
                        };
                    },
                    {
                        log,
                        ok: true,
                    },
                );

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
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

        it('overwrites the log if the previousEntryIdentifier is somewhere in the list and the log has entries after it and there is a conflict between the terms', () => {
            const log = new Log<string>([
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

            const { ok, newLog } = log.appendEntries({
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

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
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

        it('overwrites the whole log if the previousEntryIdentifier is undefined and the term of the new logs is higher', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            const { ok, newLog } = log.appendEntries({
                previousEntryIdentifier: undefined,
                entries: [
                    {
                        term: 4,
                        value: 'x <- 1',
                    },
                ],
            });

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
                {
                    term: 4,
                    value: 'x <- 1',
                },
            ]);
        });

        it('rejects the entries if the term of the previous entry identifier is not correct', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            const { ok, newLog } = log.appendEntries({
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

            expect(ok).toEqual(false);
            expect(newLog.getEntries()).toEqual(initialEntries);
        });

        it('rejects the entries when the index of the previous identifier is not in the log', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            const { ok, newLog } = log.appendEntries({
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

            expect(ok).toEqual(false);
            expect(newLog.getEntries()).toEqual(initialEntries);
        });

        it('returns if the previous entry identifier is correct when entries is empty', () => {
            const initialEntries = [
                {
                    term: 3,
                    value: 'z <- 123',
                },
            ];
            const log = new Log(initialEntries);

            const { ok: ok1, newLog: newLog1 } = log.appendEntries({
                previousEntryIdentifier: {
                    term: 0,
                    index: 3,
                },
                entries: [],
            });
            expect(ok1).toEqual(false);
            expect(newLog1.getEntries()).toEqual(initialEntries);

            const { ok: ok2, newLog: newLog2 } = log.appendEntries({
                previousEntryIdentifier: {
                    term: 3,
                    index: 10,
                },
                entries: [],
            });
            expect(ok2).toEqual(false);

            expect(newLog2.getEntries()).toEqual(initialEntries);
        });
    });

    it('does not overwrite entries if they are already consistent (e.g. late delivery of a message)', () => {
        const initialEntries = [
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
        ];
        const log = new Log(initialEntries);

        const { ok, newLog } = log.appendEntries({
            previousEntryIdentifier: {
                term: 1,
                index: 1,
            },
            entries: [
                {
                    value: 'y <- 1',
                    term: 1,
                },
            ],
        });

        expect(ok).toEqual(true);
        expect(newLog.getEntries()).toEqual(initialEntries);
    });
});
