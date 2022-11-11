import * as io from 'io-ts';
import { Middleware } from 'koa';
import { either } from 'fp-ts';

export const parseBodyMiddleware =
    (bodyCodec: io.Any): Middleware =>
    (context, next) => {
        try {
            const body = JSON.parse(context.request.body);
            const result = bodyCodec.decode(body);
            if (either.isLeft(result)) {
                context.throw(400, 'Invalid request body');
                return;
            }

            context.request.body = body;

            return next();
        } catch {
            context.throw(400, 'Request body is not valid JSON');
            return;
        }
    };
