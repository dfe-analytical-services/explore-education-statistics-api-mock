import { Request } from 'express';
import {
  BadRequest,
  OpenApiRequestMetadata,
} from 'express-openapi-validator/dist/framework/types';
import { has, isEqual, uniqWith, upperFirst } from 'lodash';
import { ErrorDictionary, ErrorViewModel } from '../schema';
import ApiError from './ApiError';
import isOpenApiValidationError, {
  OpenApiValidationError,
} from './isOpenApiValidationError';

interface OpenApiRequest extends Request {
  openapi: OpenApiRequestMetadata;
}

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

  public static fromBadRequest(
    error: BadRequest,
    req: Request
  ): ValidationError {
    return new ValidationError({
      errors: normalizeOpenApiValidationErrors(
        error.errors,
        req as OpenApiRequest
      ),
    });
  }

  public static atPath(path: string, error: ErrorViewModel): ValidationError {
    return new ValidationError({
      errors: {
        [path]: [error],
      },
    });
  }
}

function normalizeOpenApiValidationErrors(
  errors: unknown[],
  req: OpenApiRequest
): ErrorDictionary {
  const uniqueErrors = uniqWith(
    errors.filter(isOpenApiValidationError),
    isEqual
  );

  const unsortedErrors = uniqueErrors.reduce<ErrorDictionary>((acc, error) => {
    const pathParts = error.path
      .replace(/\/(body|query|params)\//, '')
      .split('/');

    // Don't include any errors that don't actually have a corresponding
    // path in the body. These errors can occur when using `oneOf` validation
    // e.g. with data set queries.
    if (error.path.startsWith('/body/') && !has(req.body, pathParts)) {
      return acc;
    }

    const viewModel = createErrorViewModel(error, uniqueErrors);

    if (!viewModel) {
      return acc;
    }

    const jsonPath = toJsonPath(pathParts);

    if (!acc[jsonPath]) {
      acc[jsonPath] = [];
    }

    acc[jsonPath].push(viewModel);

    return acc;
  }, {});

  return Object.keys(unsortedErrors)
    .sort()
    .reduce<ErrorDictionary>((acc, path) => {
      acc[path] = unsortedErrors[path];

      return acc;
    }, {});
}

function createErrorViewModel(
  error: OpenApiValidationError,
  allErrors: OpenApiValidationError[]
): ErrorViewModel | undefined {
  const code =
    error.errorCode?.replace('.openapi.validation', '') ??
    parseFallbackCode(error);

  switch (code) {
    case 'additionalProperties': {
      // If there are errors on a child path of the current
      // error's path, then this is error is likely to create
      // unnecessary noise, so we should just strip it out.
      if (hasChildPathError(error, allErrors)) {
        return undefined;
      }

      return {
        message: 'Unknown properties are not allowed',
        code: 'unknown',
      };
    }
    case 'oneOf': {
      // If there are errors on a child path of the current
      // error's path, then this is error is likely to create
      // unnecessary noise, so we should just strip it out.
      if (hasChildPathError(error, allErrors)) {
        return undefined;
      }

      return {
        message: 'Must match an allowed schema',
        code: 'oneOf',
      };
    }
  }

  return {
    message: upperFirst(error.message),
    code,
  };
}

function toJsonPath(path: string[]): string {
  // Convert path to JSONPath syntax e.g. a.b[0].c
  return path
    .map((part) => (Number.isInteger(Number(part)) ? `[${part}]` : `.${part}`))
    .join('')
    .replace('.', '');
}

function hasChildPathError(
  error: OpenApiValidationError,
  allErrors: OpenApiValidationError[]
): boolean {
  return allErrors.some(
    (err) => err.path !== error.path && err.path.startsWith(error.path)
  );
}

function parseFallbackCode(error: OpenApiValidationError): string {
  if (error.message.startsWith('Unknown query parameter')) {
    return 'unknown';
  }

  return '';
}
