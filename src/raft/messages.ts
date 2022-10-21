import * as io from 'io-ts';

const AppendEntries = io.type({
    type: io.literal('appendEntries'),
    previousEntryIdentifier: io.union([
        io.undefined,
        io.type({
            index: io.number,
            term: io.number,
        }),
    ]),
    term: io.number,
    entries: io.array(
        io.type({
            term: io.number,
            value: io.any,
        }),
    ),
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
