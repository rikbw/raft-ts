import { Raft, StateMachine } from '../src/raft';
import { createLogger } from 'bunyan';

const logger = createLogger({
    name: 'raft',
    level: 'debug',
});

const args = process.argv.slice(2).map((arg) => Number.parseInt(arg));

const [basePort, offset, clusterSize] = args;

// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
const nodePort = basePort! + offset!;

const allNodePorts = Array(clusterSize)
    .fill(basePort)
    .map((port, index) => port + index);

const otherNodePorts = allNodePorts.filter((port) => port != nodePort);

const stateMachine: StateMachine<string> = {
    handleValue(value: string) {
        logger.info(value);
    },
};

new Raft(nodePort, otherNodePorts, stateMachine, logger, 2);
