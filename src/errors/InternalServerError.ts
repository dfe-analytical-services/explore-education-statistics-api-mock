import { ErrorDictionary } from '../schema';
import ApiError from './ApiError';

export default class InternalServerError extends ApiError {
  constructor(err?: { title?: string; errors?: ErrorDictionary }) {
    super({
      title: err?.title ?? 'There was a problem processing the request.',
      type: 'Internal Server Error',
      status: 500,
      errors: err?.errors,
    });
  }
}
