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

export const genericErrors = {
  notFound({ items }: { items: unknown[] }) {
    return {
      message: `Could not find ${items.length === 1 ? 'item' : 'items'}.`,
      code: 'notFound',
      details: {
        items,
      },
    };
  },
} as const satisfies Dictionary<ErrorViewModel | ErrorViewModelFactory>;
