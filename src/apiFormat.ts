import * as io from 'io-ts';

export const DeleteBody = io.type({
    clientId: io.number,
    requestSerial: io.number,
});
export type DeleteBody = io.TypeOf<typeof DeleteBody>;

export const SetBody = io.type({
    clientId: io.number,
    value: io.string,
    requestSerial: io.number,
});
export type SetBody = io.TypeOf<typeof SetBody>;
