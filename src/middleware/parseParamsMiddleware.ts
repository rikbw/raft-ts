import * as io from 'io-ts';
import { Middleware } from 'koa';
import { either } from 'fp-ts';

export const parseParamsMiddleware =
    (params: io.Any): Middleware =>
    (context, next) => {
        const result = params.decode(context.params);
        if (either.isLeft(result)) {
            context.throw(400, 'Invalid request params');
            return;
        }

        return next();
    };
