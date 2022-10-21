import { Raft } from '../src/raft';
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

new Raft(nodePort, otherNodePorts, logger, 2);
