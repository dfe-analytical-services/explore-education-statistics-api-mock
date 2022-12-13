import { isObject } from 'lodash';

export interface ErrorLike {
  message: string;
  name: string;
}

export default function isErrorLike(value: unknown): value is ErrorLike {
  return isObject(value) && 'message' in value && 'name' in value;
}
