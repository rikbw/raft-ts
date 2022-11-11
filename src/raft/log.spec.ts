import { Entry, Log } from './log';

const id = {
    clientId: 123,
    requestSerial: 456,
};

describe('Log', () => {
    describe('appendEntries', () => {
        it('accepts the entries if the previousIndex is the same', () => {
            const log = new Log<string>([
                {
                    term: 1,
                    type: 'value',
                    value: 'x <- 1',
                    id,
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
                        type: 'value',
                        value: 'y <- 2',
                        id,
                    },
                ],
            });

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
                {
                    term: 1,
                    type: 'value',
                    value: 'x <- 1',
                    id,
                },
                {
                    term: 2,
                    type: 'value',
                    value: 'y <- 2',
                    id,
                },
            ]);
            expect(log.getEntries()).toEqual([
                {
                    term: 1,
                    type: 'value',
                    value: 'x <- 1',
                    id,
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
                        type: 'value',
                        value: 'x <- 8',
                        id,
                    },
                    {
                        term: 3,
                        type: 'value',
                        value: 'y <- 12',
                        id,
                    },
                ],
            });

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
                {
                    term: 3,
                    type: 'value',
                    value: 'x <- 8',
                    id,
                },
                {
                    term: 3,
                    type: 'value',
                    value: 'y <- 12',
                    id,
                },
            ]);
        });

        it('is idempotent', () => {
            const initialEntries: Entry<string>[] = [
                {
                    term: 1,
                    type: 'value',
                    value: 'x <- 1',
                    id,
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
                                    type: 'value',
                                    value: 'x <- 2',
                                    term: 1,
                                    id,
                                },
                                {
                                    term: 1,
                                    type: 'value',
                                    value: 'y <- 3',
                                    id,
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
                    type: 'value',
                    value: 'x <- 2',
                    term: 1,
                    id,
                },
                {
                    term: 1,
                    type: 'value',
                    value: 'y <- 3',
                    id,
                },
            ]);
        });

        it('overwrites the log if the previousEntryIdentifier is somewhere in the list and the log has entries after it and there is a conflict between the terms', () => {
            const log = new Log<string>([
                {
                    term: 1,
                    type: 'value',
                    value: 'x <- 1',
                    id,
                },
                {
                    term: 1,
                    type: 'value',
                    value: 'y <- 1',
                    id,
                },
                {
                    term: 1,
                    type: 'value',
                    value: 'z <- 1',
                    id,
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
                        type: 'value',
                        value: 'a <- 2',
                        id,
                    },
                    {
                        term: 2,
                        type: 'value',
                        value: 'b <- 3',
                        id,
                    },
                ],
            });

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
                {
                    term: 1,
                    type: 'value',
                    value: 'x <- 1',
                    id,
                },
                {
                    term: 2,
                    type: 'value',
                    value: 'a <- 2',
                    id,
                },
                {
                    term: 2,
                    type: 'value',
                    value: 'b <- 3',
                    id,
                },
            ]);
        });

        it('overwrites the whole log if the previousEntryIdentifier is undefined and the term of the new logs is higher', () => {
            const initialEntries: Entry<string>[] = [
                {
                    term: 3,
                    type: 'value',
                    value: 'z <- 123',
                    id,
                },
            ];
            const log = new Log(initialEntries);

            const { ok, newLog } = log.appendEntries({
                previousEntryIdentifier: undefined,
                entries: [
                    {
                        term: 4,
                        type: 'value',
                        value: 'x <- 1',
                        id,
                    },
                ],
            });

            expect(ok).toEqual(true);
            expect(newLog.getEntries()).toEqual([
                {
                    term: 4,
                    type: 'value',
                    value: 'x <- 1',
                    id,
                },
            ]);
        });

        it('rejects the entries if the term of the previous entry identifier is not correct', () => {
            const initialEntries: Entry<string>[] = [
                {
                    term: 3,
                    type: 'value',
                    value: 'z <- 123',
                    id,
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
                        type: 'value',
                        value: 'w <- 34',
                        id,
                    },
                ],
            });

            expect(ok).toEqual(false);
            expect(newLog.getEntries()).toEqual(initialEntries);
        });

        it('rejects the entries when the index of the previous identifier is not in the log', () => {
            const initialEntries: Entry<string>[] = [
                {
                    term: 3,
                    type: 'value',
                    value: 'z <- 123',
                    id,
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
                        type: 'value',
                        value: 'w <- 34',
                        id,
                    },
                ],
            });

            expect(ok).toEqual(false);
            expect(newLog.getEntries()).toEqual(initialEntries);
        });

        it('returns if the previous entry identifier is correct when entries is empty', () => {
            const initialEntries: Entry<string>[] = [
                {
                    term: 3,
                    type: 'value',
                    value: 'z <- 123',
                    id,
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
        const initialEntries: Entry<string>[] = [
            {
                term: 1,
                type: 'value',
                value: 'x <- 1',
                id,
            },
            {
                term: 1,
                type: 'value',
                value: 'y <- 1',
                id,
            },
            {
                term: 1,
                type: 'value',
                value: 'z <- 1',
                id,
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
                    type: 'value',
                    value: 'y <- 1',
                    term: 1,
                    id,
                },
            ],
        });

        expect(ok).toEqual(true);
        expect(newLog.getEntries()).toEqual(initialEntries);
    });

    it('appends noop entries', () => {
        const log = new Log<string>([]);
        const entries: Entry<string>[] = [
            {
                term: 0,
                type: 'noop',
            },
        ];

        const { ok, newLog } = log.appendEntries({
            previousEntryIdentifier: undefined,
            entries,
        });

        expect(ok).toEqual(true);
        expect(newLog.getEntries()).toEqual(entries);
    });

    it('does not mutate the old log', () => {
        const log = new Log<string>([]);
        const entries: Entry<string>[] = [
            {
                term: 0,
                type: 'noop',
            },
            {
                term: 0,
                type: 'value',
                value: 'x <- 2',
                id: {
                    requestSerial: 0,
                    clientId: 2,
                },
            },
        ];

        const { ok } = log.appendEntries({
            previousEntryIdentifier: undefined,
            entries,
        });

        expect(ok).toEqual(true);
        expect(log.getEntries()).toEqual([]);
    });
});
