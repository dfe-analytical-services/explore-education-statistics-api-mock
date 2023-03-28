import { WarningViewModel } from '../schema';

export type WarningViewModelFactory = (...params: any[]) => WarningViewModel;

export const criteriaWarnings = {
  empty: {
    message: 'Empty criteria are not permitted.',
    code: 'empty',
  },
} as const satisfies Dictionary<WarningViewModel | WarningViewModelFactory>;
