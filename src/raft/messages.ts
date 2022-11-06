import * as io from 'io-ts';

const EntryIdentifier = io.union([
    io.undefined,
    io.type({
        index: io.number,
        term: io.number,
    }),
]);

const Entry = io.union([
    io.type({
        type: io.literal('value'),
        term: io.number,
        value: io.any,
        id: io.type({
            requestSerial: io.number,
            clientId: io.number,
        }),
    }),
    io.type({
        type: io.literal('noop'),
        term: io.number,
    }),
]);

const AppendEntries = io.type({
    type: io.literal('appendEntries'),
    previousEntryIdentifier: EntryIdentifier,
    term: io.number,
    entries: io.array(Entry),
    leaderCommit: io.number,
});

const AppendEntriesResponse = io.type({
    type: io.literal('appendEntriesResponse'),
    ok: io.boolean,
    prevLogIndexFromRequest: io.number,
    term: io.number,
    numberOfEntriesSentInRequest: io.number,
});

const RequestVote = io.type({
    type: io.literal('requestVote'),
    term: io.number,
    lastLog: EntryIdentifier,
});

const RequestVoteResponse = io.type({
    type: io.literal('requestVoteResponse'),
    voteGranted: io.boolean,
    term: io.number,
});

export const NodeMessageCodec = io.intersection([
    io.union([
        AppendEntries,
        AppendEntriesResponse,
        RequestVote,
        RequestVoteResponse,
    ]),
    io.type({
        responsePort: io.number,
    }),
]);
export type NodeMessageDTO = io.TypeOf<typeof NodeMessageCodec>;
