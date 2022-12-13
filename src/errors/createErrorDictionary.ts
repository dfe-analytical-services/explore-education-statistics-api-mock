import { isEqual } from 'lodash';
import { ErrorDictionary, ErrorViewModel } from '../schema';
import isErrorLike from './isErrorLike';
import isOpenApiValidationError from './isOpenApiValidationError';

export default function createErrorDictionary(
  errors: unknown[]
): ErrorDictionary {
  const unsorted = errors.reduce<ErrorDictionary>((acc, error) => {
    if (isOpenApiValidationError(error)) {
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
    } else if (isErrorLike(error)) {
      acc[''].push({
        message: error.message,
      });
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
