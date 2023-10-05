import { HttpHandler, InvocationContext } from '@azure/functions';
import { InternalServerError } from '../../errors';
import ApiError from '../../errors/ApiError';

export default function withErrorHandling(handler: HttpHandler): HttpHandler {
  return async (request, context) => {
    try {
      return await handler(request, context);
    } catch (err) {
      const apiError = handleApiError(err, context);

      return {
        status: apiError.status,
        jsonBody: apiError,
      };
    }
  };
}

function handleApiError(err: unknown, context: InvocationContext): ApiError {
  if (err instanceof ApiError) {
    return err;
  }

  context.error(err);

  return new InternalServerError();
}
