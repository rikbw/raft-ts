import Koa from 'koa';
import Router from '@koa/router';
import koaBodyParser from 'koa-body';

const app = new Koa();

app.use(koaBodyParser());

const mainRouter = new Router();

const store = new Map<string, string>();

// TODO use shared io-ts types for request bodies and responses

mainRouter.get('/get/:key', (context) => {
    const { key } = context.params;
    console.log('got get request', { key });
    context.body = store.get(key as string);
});

mainRouter.get('/delete/:key', (context) => {
    const { key } = context.params;
    console.log('got delete request', { key });
    store.delete(key as string);
    context.body = 'ok';
});

mainRouter.post('/set/:key', (context) => {
    const { key } = context.params;
    const value = JSON.parse(context.request.body);
    console.log('got set request', { key, value });
    store.set(key as string, value);
    context.body = 'ok';
});

app.use(mainRouter.routes());

app.listen(3000);

console.log('listening on port 3000');
