# raft-ts

This project was developed during (and after) [David Beazley's Raft course](https://www.dabeaz.com/raft.html).
It's an implementation of the [Raft consensus algorithm](https://raft.github.io/) in TypeScript.

### Limitations

Right now it likely contains some bugs and will not be performant.
Verifying that the program works correctly will require extra work.

This implementation does not support configuration changes.

The library only supports running Raft nodes on a single machine.
To support multiple machines, the `Raft` class should take urls and not ports as inputs.

## Library API

### Class: `Raft`

A Raft instance contains all core raft logic around leader election, log replication, but also persistence, timers and communicating with other raft instances.
You pass it a state machine (e.g. a key-value store) and some configuration.
The instance will then automatically connect to other instances, and replicate the log.

```typescript
const raft = new Raft<LogValueType>(
    nodePort,
    otherNodePorts,
    stateMachine,
    logger,
    persistenceFilePath,
    slowdownTimeBy,
    leaderElectionTimeoutMs,
    heartbeatTimeoutMs,
);
```

-   `nodePort: number` the port on which to listen for incoming connections from other Raft instances.
-   `otherNodePorts: Array<number>` the ports of other raft nodes in the cluster.
-   `stateMachine: StateMachine` the state machine (see below).
-   `logger: bunyan.Logger` a logger, which in the future shouldn't be a dependency logger but an interface.
-   `persistenceFilePath: string` path to the file where the persistent data from this raft node should be read and written.
-   `slowdownTimeBy: number | undefined` slow down time for testing. Default: 1.
-   `leaderElectionTimeoutMs: number | undefined` raft will wait for `[t, t * 2]` to call a new leader election. Default: 3000.
-   `heartbeatTimeoutMs: number | undefined` the time after which leaders send heartbeats to followers. Default: 500.

### Method: `raft.addToLog(logValue, requestId)`

Append a value to the log.
The raft instance that this is called on should be the leader.
The raft instance will apply the value to the state machine when it is committed.

-   `logValue: LogValueType` the value that is applied to the state machine (see below) when committed (replicated across a majority).
-   `requestId: { clientId: number, requestSerial: number }` unique identifier of this request. When retrying requests, use the same identifier. `requestId` should be monotonically increasing for different requests.
-   Returns: `Promise<either.Either<'notLeader' | 'timedOut', undefined>>`
    -   Resolves with `either.right(undefined)` when the entry is committed.
    -   Resolves with `either.left('notLeader')` when the raft instance is not the leader.
    -   Resolves with `either.left('timedOut')` if the request timed out. This can happen on network partitions. Retry with the same request ID.

### Method: `raft.syncBeforeRead()`

Should be called before every read on the state machine.
See section 8 of the Raft paper on details what this function does.

-   Returns: `Promise<{ isLeader: boolean }>`
    -   Resolves with `{ isLeader: true }` when it is safe to read from the state machine.
    -   Resolves with `{ isLeader: false }` when the raft instance is not the leader. After this, it is not safe to read from the state machine.

### Type: `StateMachine`

```typescript
type StateMachine<LogValueType> = {
    handleValue(value: LogValueType): void;
};
```

Raft requires the state machine to implement `handleValue`, which the raft instance calls with the values of committed log entries.

## Default implementation

The default implementation is a simple key value server.
Its state machine handles values of type:

```typescript
type KeyValueStoreAction =
    | {
          type: 'set';
          key: string;
          value: string;
      }
    | {
          type: 'delete';
          key: string;
      };
```

These values are appended to the log by the http handlers in `src/index.ts`.

### Installation

Run `yarn` in the root directory.

### Running the server

Running the server can be done with the command

```shell
$ yarn server
```

It accepts the following environment variables:

-   `PORT` the port on which to run the http handler. E.g. `3000`
-   `OTHER_PORTS` other ports in the cluster. E.g. `3001,3002`
-   `PERSISTENCE_FILE_PATH` path to file in which raft stores persistent data.
-   `LOG_LEVEL` info or debug log level. Default: info.

### Running the example client

Running a client can be done with the command

```shell
$ yarn client $CLIENT_ID
```

It starts an interactive shell on which to type commands. Supported are:

-   `get KEY PORT REQUEST_ID`
-   `set KEY VALUE PORT REQUEST_ID`
-   `delete KEY PORT REQUEST_ID`

The parameters are as follows:

-   `KEY`: the key of the entry in the key value store.
-   `VALUE` the value of the entry in the key value store.
-   `PORT` the port of the server on which to send the request.
-   `REQUEST_ID` the id of the request. Default: monotonically increasing request id.
