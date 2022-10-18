import split2 from 'split2';
import fetch from 'node-fetch';
import { either, function as func } from 'fp-ts';
import * as io from 'io-ts';
import { unreachable } from '../util/unreachable';

process.stdin.pipe(split2()).on('data', handleInput);

class Client {
    private static url({ path }: { path: string }) {
        return `http://localhost:3000/${path}`;
    }

    async request({
        path,
        method,
        body,
    }: {
        path: string;
        method: string;
        body?: string;
    }) {
        const url = Client.url({ path });
        const result = await fetch(url, {
            method,
            body,
            headers: { 'Content-Type': 'text/plain' },
        });
        return result.text();
    }

    public get(key: string): Promise<string> {
        return this.request({
            path: `get/${key}`,
            method: 'get',
        });
    }

    public async set(key: string, value: string): Promise<void> {
        await this.request({
            path: `set/${key}`,
            method: 'post',
            body: JSON.stringify(value),
        });
    }

    public async delete(key: string): Promise<void> {
        await this.request({
            path: `delete/${key}`,
            method: 'get',
        });
    }
}

const client = new Client();

async function handleInput(input: string) {
    const commandResult = parseInput(input);

    if (either.isLeft(commandResult)) {
        console.log('error: invalid command');
        return;
    }

    const command = commandResult.right;

    switch (command.type) {
        case 'get': {
            const result = await client.get(command.key);
            console.log(result);
            return;
        }

        case 'delete': {
            await client.delete(command.key);
            console.log('ok');
            return;
        }

        case 'set': {
            const { key, value } = command;
            await client.set(key, value);
            console.log('ok');
            return;
        }

        default:
            unreachable(command);
    }
}

type Command =
    | {
          type: 'get' | 'delete';
          key: string;
      }
    | {
          type: 'set';
          key: string;
          value: string;
      };

const GetInput = io.tuple([io.string]);
const SetInput = io.tuple([io.string, io.string]);
const DeleteInput = io.tuple([io.string]);

function parseInput(input: string): either.Either<'failed', Command> {
    const parts = input.split(' ');
    const [command, ...rest] = parts;

    switch (command) {
        case 'get':
            return func.pipe(
                rest,
                GetInput.decode,
                either.map(([key]) => {
                    return {
                        type: 'get' as const,
                        key,
                    };
                }),
                either.mapLeft(() => 'failed' as const),
            );

        case 'set':
            return func.pipe(
                rest,
                SetInput.decode,
                either.map(([key, value]) => {
                    return {
                        type: 'set' as const,
                        key,
                        value,
                    };
                }),
                either.mapLeft(() => 'failed' as const),
            );

        case 'delete':
            return func.pipe(
                rest,
                DeleteInput.decode,
                either.map(([key]) => {
                    return {
                        type: 'delete' as const,
                        key,
                    };
                }),
                either.mapLeft(() => 'failed' as const),
            );

        default:
            return either.left('failed' as const);
    }
}
