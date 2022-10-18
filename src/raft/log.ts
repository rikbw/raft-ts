type EntryIdentifier = {
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

    private getIndexFromWhereToTruncateEntries(
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

    public appendEntries({
        previousEntryIdentifier,
        entries,
    }: {
        previousEntryIdentifier: EntryIdentifier | undefined;
        entries: Entry<ValueType>[];
    }): boolean {
        const index = this.getIndexFromWhereToTruncateEntries(
            previousEntryIdentifier,
        );

        if (index == null) {
            return false;
        }

        this.entries = [...this.entries.slice(0, index), ...entries];

        return true;
    }

    public getEntries(): ReadonlyArray<Entry<ValueType>> {
        return this.entries;
    }
}
