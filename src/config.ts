import * as io from 'io-ts';
import { createLogger } from 'bunyan';
import { either } from 'fp-ts';

export type Logger = ReturnType<typeof createLogger>;

type Configuration = {
    port: number;
    otherPorts: number[];
    logger: Logger;
};

let config: Configuration | undefined = undefined;

export function getConfig(): Configuration {
    if (config == null) {
        config = setupConfig();
    }

    return config;
}

function setupConfig(): Configuration {
    const port = io.number.decode(process.env['PORT']);

    if (either.isLeft(port)) {
        throw new Error('PORT is not a number');
    }

    const otherPorts = io.array(io.number).decode(process.env['OTHER_PORTS']);

    if (either.isLeft(otherPorts)) {
        throw new Error('OTHER_PORTS is not a valid json array of numbers');
    }

    const logger = createLogger({
        name: 'Raft node',
        level: 'debug',
    });

    return {
        port: port.right,
        otherPorts: otherPorts.right,
        logger,
    };
}
