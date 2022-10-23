import Immutable, { ImmutableArray } from 'seamless-immutable';

export type EntryIdentifier = {
    index: number;
    term: number;
};

export type Entry<ValueType> = {
    term: number;
    value: ValueType;
};

export class Log<ValueType> {
    private readonly entries: ImmutableArray<Entry<ValueType>>;

    public constructor(
        initialEntries:
            | Array<Entry<ValueType>>
            | ImmutableArray<Entry<ValueType>>,
    ) {
        this.entries = Immutable.isImmutable(initialEntries)
            ? initialEntries
            : Immutable(initialEntries);
    }

    // Returns the index of entries the previousEntryIdentifier refers to.
    // Returns undefined if the there's a different term at the given index in the log.
    private entriesIndexFromPreviousEntryIdentifier(
        previousEntryIdentifier: EntryIdentifier | undefined,
    ): number | undefined {
        if (previousEntryIdentifier == null) {
            return 0;
        }

        const { index, term } = previousEntryIdentifier;
        if (
            this.entries.length - 1 >= index &&
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            this.entries[index]!.term === term
        ) {
            return index + 1;
        }

        return undefined;
    }

    private entriesHaveNoConflictsWithRequest({
        entries,
        index,
    }: {
        entries: Entry<ValueType>[];
        index: number;
    }): boolean {
        const entriesToCompare = this.entries.slice(index);
        if (entriesToCompare.length != entries.length) {
            return false;
        }
        return entries.every(
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            (entry, index) => entriesToCompare[index]!.term === entry.term,
        );
    }

    // The called of this method should check that the term of the appendEntries request is higher or equal than the
    // expected term.
    public appendEntries({
        previousEntryIdentifier,
        entries,
    }: {
        previousEntryIdentifier: EntryIdentifier | undefined;
        entries: Entry<ValueType>[];
    }) {
        const index = this.entriesIndexFromPreviousEntryIdentifier(
            previousEntryIdentifier,
        );

        // The entry identifier is not valid.
        if (index == null) {
            return {
                ok: false,
                newLog: this,
            };
        }

        if (
            this.entriesHaveNoConflictsWithRequest({
                entries,
                index,
            })
        ) {
            return {
                ok: true,
                newLog: this,
            };
        }

        // There's a conflict, so we have to truncate.
        const newEntries = this.entries.slice(0, index).concat(entries);

        return {
            ok: true,
            newLog: new Log(newEntries),
        };
    }

    public getEntries(): ImmutableArray<Entry<ValueType>> {
        return this.entries;
    }

    public get length() {
        return this.entries.length;
    }
}
