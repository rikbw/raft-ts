import Koa from 'koa';
import Router from '@koa/router';
import koaBodyParser from 'koa-body';
import { Raft, StateMachine } from './raft';
import { getConfig } from './config';
import { unreachable } from './util/unreachable';
import * as io from 'io-ts';
import { parseBodyMiddleware } from './middleware/parseBodyMiddleware';
import { parseParamsMiddleware } from './middleware/parseParamsMiddleware';
import { RequestId } from './raft/log';
import { either } from 'fp-ts';

// An example usage of the Raft library: state machine is a simple K/V store

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

class KeyValueStore implements StateMachine<KeyValueStoreAction> {
    private store = new Map<string, string>();

    handleValue(value: KeyValueStoreAction) {
        switch (value.type) {
            case 'delete':
                this.store.delete(value.key);
                return;

            case 'set':
                this.store.set(value.key, value.value);
                return;

            default:
                return unreachable(value);
        }
    }

    public get(key: string) {
        return this.store.get(key);
    }
}

const keyValueStore = new KeyValueStore();

const { port, otherPorts, logger } = getConfig();

const raft = new Raft(port, otherPorts, keyValueStore, logger);

const app = new Koa();

app.use(koaBodyParser());

const mainRouter = new Router();

const KeyParams = io.type({
    key: io.string,
});
type KeyParams = io.TypeOf<typeof KeyParams>;

mainRouter.get(
    '/get/:key',
    parseParamsMiddleware(KeyParams),
    async (context) => {
        const { key } = context.params;

        logger.info('got get request', { key });

        const { isLeader } = await raft.syncBeforeRead();

        if (!isLeader) {
            context.throw('This Raft node is not the leader', 400);
        }

        context.body = keyValueStore.get(key as string);
    },
);

const DeleteBody = io.type({
    clientId: io.number,
    requestSerial: io.number,
});
type DeleteBody = io.TypeOf<typeof DeleteBody>;

mainRouter.post(
    '/delete/:key',
    parseBodyMiddleware(DeleteBody),
    parseParamsMiddleware(KeyParams),
    async (context) => {
        const { key } = context.params as KeyParams;
        const { clientId, requestSerial } = context.request.body as DeleteBody;

        logger.info('got delete request', { key, clientId });

        const action: KeyValueStoreAction = {
            type: 'delete',
            key,
        };

        const id: RequestId = {
            clientId,
            requestSerial,
        };

        const result = await raft.addToLog(action, id);

        if (either.isLeft(result)) {
            switch (result.left) {
                case 'notLeader':
                    context.throw('This raft node is not the leader', 400);
                    break;

                case 'timedOut':
                    context.throw(
                        'Request timed out. Try again with the same request serial',
                        503,
                    );
                    break;

                default:
                    unreachable(result.left);
            }
        }

        context.body = 'ok';
    },
);

const SetBody = io.type({
    clientId: io.number,
    value: io.string,
    requestSerial: io.number,
});
type SetBody = io.TypeOf<typeof SetBody>;

mainRouter.post(
    '/set/:key',
    parseParamsMiddleware(KeyParams),
    parseBodyMiddleware(SetBody),
    async (context) => {
        const { key } = context.params as KeyParams;
        const { value, clientId, requestSerial } = context.request
            .body as SetBody;

        logger.info('got set request', { key, value, clientId });

        const action: KeyValueStoreAction = {
            type: 'set',
            key,
            value,
        };

        const id: RequestId = {
            clientId,
            requestSerial,
        };

        const result = await raft.addToLog(action, id);

        if (either.isLeft(result)) {
            switch (result.left) {
                case 'notLeader':
                    context.throw('This raft node is not the leader', 400);
                    break;

                case 'timedOut':
                    context.throw(
                        'Request timed out. Try again with the same request id',
                        503,
                    );
                    break;

                default:
                    unreachable(result.left);
            }
        }

        context.body = 'ok';
    },
);

app.use(mainRouter.routes());

app.listen(3000);

logger.info('listening on port 3000');
