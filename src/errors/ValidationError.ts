import { BadRequest } from 'express-openapi-validator/dist/framework/types';
import { isEqual } from 'lodash';
import { ErrorDictionary, ErrorViewModel } from '../schema';
import ApiError from './ApiError';
import isOpenApiValidationError from './isOpenApiValidationError';

export default class ValidationError extends ApiError {
  constructor({
    title = 'There are validation errors with the request.',
    errors,
  }: {
    title?: string;
    errors: ErrorDictionary;
  }) {
    super({
      title: title,
      type: 'Bad Request',
      status: 400,
      errors,
    });
  }

  public static fromBadRequest(error: BadRequest): ValidationError {
    return new ValidationError({
      errors: normalizeOpenApiValidationErrors(error.errors),
    });
  }
}

function normalizeOpenApiValidationErrors(errors: unknown[]): ErrorDictionary {
  const unsorted = errors
    .filter(isOpenApiValidationError)
    .reduce<ErrorDictionary>((acc, error) => {
      const path = normalizePath(error.path);

      if (!acc[path]) {
        acc[path] = [];
      }

      const newItem: ErrorViewModel = {
        message: error.message,
        code: error.errorCode,
      };

      // If this error is a duplicate, don't add it.
      if (!acc[path].some((item) => isEqual(item, newItem))) {
        acc[path].push(newItem);
      }

      return acc;
    }, {});

  return Object.keys(unsorted)
    .sort()
    .reduce<ErrorDictionary>((acc, key) => {
      acc[key] = unsorted[key];

      return acc;
    }, {});
}

function normalizePath(path: string): string {
  if (path.includes('/')) {
    return '';
  }

  return path.replace(/\.(body|query|params)\./g, '');
}
