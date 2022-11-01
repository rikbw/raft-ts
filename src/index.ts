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

const clientRequestSerialMap = new Map<number, number>();
const clientRequestSerial = (clientId: number) => {
    const serial = clientRequestSerialMap.get(clientId) ?? 0;
    clientRequestSerialMap.set(clientId, serial + 1);
    return serial;
};

const app = new Koa();

app.use(koaBodyParser());

const mainRouter = new Router();

mainRouter.get('/get/:key', (context) => {
    const { key } = context.params;
    console.log('got get request', { key });
    // TODO
    //  - sync before read - paper section 8
    //  - check that you are leader & redirect if not
    context.body = keyValueStore.get(key as string);
});

const DeleteBody = io.type({
    clientId: io.number,
});
type DeleteBody = io.TypeOf<typeof DeleteBody>;

const KeyParams = io.type({
    key: io.string,
});
type KeyParams = io.TypeOf<typeof KeyParams>;

mainRouter.post(
    '/delete/:key',
    parseBodyMiddleware(DeleteBody),
    parseParamsMiddleware(KeyParams),
    async (context) => {
        const { key } = context.params as KeyParams;
        const { clientId } = context.request.body as DeleteBody;

        console.log('got delete request', { key, clientId });

        const requestSerial = clientRequestSerial(clientId);

        const action: KeyValueStoreAction = {
            type: 'delete',
            key,
        };

        const id: RequestId = {
            clientId,
            requestSerial,
        };

        await raft.addToLog(action, id);

        context.body = 'ok';
    },
);

const SetBody = io.type({
    clientId: io.number,
    value: io.string,
});
type SetBody = io.TypeOf<typeof SetBody>;

mainRouter.post(
    '/set/:key',
    parseParamsMiddleware(KeyParams),
    parseBodyMiddleware(SetBody),
    async (context) => {
        const { key } = context.params as KeyParams;
        const { value, clientId } = context.request.body as SetBody;

        logger.info('got set request', { key, value, clientId });

        const requestSerial = clientRequestSerial(clientId);

        const action: KeyValueStoreAction = {
            type: 'set',
            key,
            value,
        };

        const id: RequestId = {
            clientId,
            requestSerial,
        };

        await raft.addToLog(action, id);

        context.body = 'ok';
    },
);

app.use(mainRouter.routes());

app.listen(3000);

console.log('listening on port 3000');
