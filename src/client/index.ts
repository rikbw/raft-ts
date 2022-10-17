import split2 from 'split2';
import fetch from 'node-fetch';

process.stdin.pipe(split2()).on('data', handleInput);

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const unreachable = (_: never) => {
    throw new Error('unreachable');
};

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
    const command = parseInput(input);

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
