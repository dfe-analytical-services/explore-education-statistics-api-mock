import { WarningViewModel } from '../schema';

export type WarningViewModelFactory = (...params: any[]) => WarningViewModel;

export const criteriaWarnings = {
  empty: {
    message: 'This criteria is empty and will be ignored.',
    code: 'criteria.empty',
  }
} as const satisfies Dictionary<WarningViewModel | WarningViewModelFactory>;
