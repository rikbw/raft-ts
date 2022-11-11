import split2 from 'split2';
import fetch from 'node-fetch';
import { either, function as func } from 'fp-ts';
import * as io from 'io-ts';
import { unreachable } from '../util/unreachable';
import { DeleteBody, SetBody } from '../apiFormat';
import { IntFromString } from 'io-ts-types';

process.stdin.pipe(split2()).on('data', handleInput);

class Client {
    public constructor(private readonly id: number) {}

    private requestSerial = 0;

    private nextRequestSerial() {
        this.requestSerial += 1;
        return this.requestSerial;
    }

    private static url({
        path,
        serverPort,
    }: {
        path: string;
        serverPort: number;
    }) {
        return `http://localhost:${serverPort}/${path}`;
    }

    async request({
        path,
        method,
        body,
        serverPort,
    }: {
        path: string;
        method: string;
        body?: string;
        serverPort: number;
    }): Promise<string> {
        const url = Client.url({ path, serverPort });
        try {
            const result = await fetch(url, {
                method,
                body,
                headers: { 'Content-Type': 'text/plain' },
            });
            if (!result.ok) {
                console.log(`status code ${result.status}`);
            }
            return result.text();
        } catch (error) {
            console.error(error);
            return 'Failed to fetch';
        }
    }

    public get(key: string, serverPort: number): Promise<string> {
        return this.request({
            path: `get/${key}`,
            method: 'get',
            serverPort,
        });
    }

    public async set(
        key: string,
        value: string,
        serverPort: number,
        requestSerial: number = this.nextRequestSerial(),
    ): Promise<string> {
        const body = SetBody.encode({
            value,
            requestSerial,
            clientId: this.id,
        });
        return this.request({
            path: `set/${key}`,
            method: 'post',
            body: JSON.stringify(body),
            serverPort,
        });
    }

    public async delete(
        key: string,
        serverPort: number,
        requestSerial: number = this.nextRequestSerial(),
    ): Promise<string> {
        const body = DeleteBody.encode({
            requestSerial,
            clientId: this.id,
        });
        return this.request({
            path: `delete/${key}`,
            method: 'post',
            body: JSON.stringify(body),
            serverPort,
        });
    }
}

const id = IntFromString.decode(process.argv[2]);

if (either.isLeft(id)) {
    throw new Error('id should be a number');
}

const client = new Client(id.right);

console.log('Client ready');

async function handleInput(input: string) {
    const commandResult = parseInput(input);

    if (either.isLeft(commandResult)) {
        console.log('error: invalid command');
        return;
    }

    const command = commandResult.right;

    switch (command.type) {
        case 'get': {
            const result = await client.get(command.key, command.serverPort);
            console.log(result);
            return;
        }

        case 'delete': {
            const result = await client.delete(
                command.key,
                command.serverPort,
                command.requestSerial,
            );
            console.log(result);
            return;
        }

        case 'set': {
            const { key, value } = command;
            const result = await client.set(
                key,
                value,
                command.serverPort,
                command.requestSerial,
            );
            console.log(result);
            return;
        }

        default:
            unreachable(command);
    }
}

type Command =
    | {
          type: 'get';
          key: string;
          serverPort: number;
      }
    | {
          type: 'delete';
          key: string;
          requestSerial: number | undefined;
          serverPort: number;
      }
    | {
          type: 'set';
          key: string;
          value: string;
          requestSerial: number | undefined;
          serverPort: number;
      };

const DeleteInput = io.union([
    io.tuple([io.string, IntFromString]),
    io.tuple([io.string, IntFromString, IntFromString]),
]);
const SetInput = io.union([
    io.tuple([io.string, io.string, IntFromString]),
    io.tuple([io.string, io.string, IntFromString, IntFromString]),
]);
const GetInput = io.tuple([io.string, IntFromString]);

function parseInput(input: string): either.Either<'failed', Command> {
    const parts = input.split(' ');
    const [command, ...rest] = parts;

    switch (command) {
        case 'get':
            return func.pipe(
                rest,
                GetInput.decode,
                either.map(([key, serverPort]) => {
                    return {
                        type: 'get' as const,
                        key,
                        serverPort,
                    };
                }),
                either.mapLeft(() => 'failed' as const),
            );

        case 'set':
            return func.pipe(
                rest,
                SetInput.decode,
                either.map(([key, value, serverPort, requestSerial]) => {
                    return {
                        type: 'set' as const,
                        key,
                        value,
                        requestSerial,
                        serverPort,
                    };
                }),
                either.mapLeft(() => 'failed' as const),
            );

        case 'delete':
            return func.pipe(
                rest,
                DeleteInput.decode,
                either.map(([key, serverPort, requestSerial]) => {
                    return {
                        type: 'delete' as const,
                        key,
                        requestSerial,
                        serverPort,
                    };
                }),
                either.mapLeft(() => 'failed' as const),
            );

        default:
            return either.left('failed' as const);
    }
}
