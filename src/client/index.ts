import split2 from 'split2';
import fetch from 'node-fetch';

process.stdin.pipe(split2()).on('data', handleInput);

const unreachable = (_: never) => {
    throw new Error('unreachable');
};

// TODO this should be in env vars
const getUrl = ({ path }: { path: string }) => `http://localhost:3000/${path}`;

// TODO use shared io-ts types for client-server interactions

async function request({
    path,
    method,
    body,
}: {
    path: string;
    method: string;
    body?: any;
}) {
    const url = getUrl({ path });
    const result = await fetch(url, {
        method,
        body,
        headers: { 'Content-Type': 'text/plain' },
    });
    return result.text();
}

async function handleInput(input: string) {
    const command = parseInput(input);

    switch (command.type) {
        case 'get': {
            const result = await request({
                path: `get/${command.key}`,
                method: 'get',
            });
            console.log(result);
            return;
        }

        case 'delete': {
            const result = await request({
                path: `delete/${command.key}`,
                method: 'get',
            });
            console.log(result);
            return;
        }

        case 'set': {
            const { key, value } = command;
            const result = await request({
                path: `set/${key}`,
                method: 'post',
                body: JSON.stringify(value),
            });
            console.log(result);
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

function parseInput(input: string): Command {
    // TODO use io-ts
    const parts = input.split(' ');
    const command = parts[0];

    switch (command) {
        case 'get':
            return {
                type: 'get',
                key: parts[1]!,
            };

        case 'set':
            return {
                type: 'set',
                key: parts[1]!,
                value: parts[2]!,
            };

        case 'delete':
            return {
                type: 'delete',
                key: parts[1]!,
            };

        default:
            throw new Error(`failed to parse: ${input}`);
    }
}
