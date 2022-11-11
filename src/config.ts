import * as io from 'io-ts';
import { createLogger } from 'bunyan';
import { either } from 'fp-ts';
import { IntFromString } from 'io-ts-types';

export type Logger = ReturnType<typeof createLogger>;

type Configuration = {
    port: number;
    otherPorts: ReadonlyArray<number>;
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
    const port = IntFromString.decode(process.env['PORT']);

    if (either.isLeft(port)) {
        throw new Error('PORT is not a number');
    }

    const otherPortsStrings = process.env['OTHER_PORTS']?.split(',');

    const otherPorts = io.array(IntFromString).decode(otherPortsStrings);

    if (either.isLeft(otherPorts)) {
        throw new Error('OTHER_PORTS is not a comma-separated list of numbers');
    }

    const logLevel = io
        .union([io.literal('debug'), io.literal('info'), io.undefined])
        .decode(process.env['LOG_LEVEL']);

    if (either.isLeft(logLevel)) {
        throw new Error(
            'LOG_LEVEL is not a valid log level (allowed values: debug, info)',
        );
    }

    const logger = createLogger({
        name: 'Raft node',
        level: logLevel.right ?? 'info',
    });

    return {
        port: port.right,
        otherPorts: otherPorts.right,
        logger,
    };
}
