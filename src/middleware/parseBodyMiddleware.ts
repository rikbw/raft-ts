import * as io from 'io-ts';
import { Middleware } from 'koa';
import { either } from 'fp-ts';

export const parseBodyMiddleware =
    (body: io.Any): Middleware =>
    (context, next) => {
        const result = body.decode(context.request.body);
        if (either.isLeft(result)) {
            context.throw(400, 'Invalid request body');
            return;
        }

        return next();
    };
