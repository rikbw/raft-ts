export type EntryIdentifier = {
    index: number;
    term: number;
};

type Entry<ValueType> = {
    term: number;
    value: ValueType;
};

export class Log<ValueType> {
    private entries: Entry<ValueType>[];

    public constructor(initialEntries: Entry<ValueType>[]) {
        this.entries = [...initialEntries];
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
    }): boolean {
        const index = this.entriesIndexFromPreviousEntryIdentifier(
            previousEntryIdentifier,
        );

        // The entry identifier is not valid.
        if (index == null) {
            return false;
        }

        if (
            this.entriesHaveNoConflictsWithRequest({
                entries,
                index,
            })
        ) {
            return true;
        }

        // There's a conflict, so we have to truncate.
        this.entries = [...this.entries.slice(0, index), ...entries];

        return true;
    }

    public getEntries(): ReadonlyArray<Entry<ValueType>> {
        return this.entries;
    }
}
