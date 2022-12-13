import { ErrorViewModel } from '../schema';

export type ErrorViewModelFactory = (...params: any[]) => ErrorViewModel;

export const arrayErrors = {
  notEmpty: {
    message: 'Cannot be empty.',
    code: 'array.notEmpty',
  },
  noBlankStrings: {
    message: 'Must all be non-blank strings.',
    code: 'array.noBlankStrings',
  },
} as const satisfies Dictionary<ErrorViewModel | ErrorViewModelFactory>;
