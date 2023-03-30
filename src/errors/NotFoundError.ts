import { ErrorDictionary } from '../schema';
import ApiError from './ApiError';

export default class NotFoundError extends ApiError {
  constructor(err?: { title?: string; errors?: ErrorDictionary }) {
    super({
      title: err?.title ?? 'Could not find the requested resource.',
      type: 'Not Found',
      status: 404,
    });
  }
}
