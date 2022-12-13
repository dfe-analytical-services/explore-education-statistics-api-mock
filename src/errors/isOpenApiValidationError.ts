import { isObject, isString } from 'lodash';

export interface OpenApiValidationError {
  path: string;
  message: string;
  errorCode: string;
}

export default function isOpenApiValidationError(
  value: unknown
): value is OpenApiValidationError {
  return (
    isObject(value) &&
    'path' in value &&
    'message' in value &&
    'errorCode' in value &&
    isString(value.path) &&
    isString(value.message) &&
    isString(value.errorCode)
  );
}
