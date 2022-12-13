import Hashids from 'hashids';
import { isEqual } from 'lodash';
import { DataSetQuery, ErrorViewModel, WarningViewModel } from '../schema';
import Database from './Database';
import { tableFile, TableName } from './dataSetPaths';
import {
  createFilterIdHasher,
  createIndicatorIdHasher,
  createLocationIdHasher,
} from './idHashers';

export default class DataSetQueryState {
  public readonly db: Database = new Database();
  public readonly filterIdHasher: Hashids;
  public readonly indicatorIdHasher: Hashids;
  public readonly locationIdHasher: Hashids;

  private readonly errors: Dictionary<ErrorViewModel[]> = {};
  private readonly warnings: Dictionary<WarningViewModel[]> = {};

  constructor(public readonly dataSetDir: string) {
    this.filterIdHasher = createFilterIdHasher(dataSetDir);
    this.indicatorIdHasher = createIndicatorIdHasher(dataSetDir);
    this.locationIdHasher = createLocationIdHasher(dataSetDir);
  }

  tableFile = (name: TableName) => {
    return tableFile(this.dataSetDir, name);
  };

  appendError = (path: keyof DataSetQuery | string, error: ErrorViewModel) => {
    append(this.errors, path, error);
  };

  prependError = (path: keyof DataSetQuery | string, error: ErrorViewModel) => {
    prepend(this.errors, path, error);
  };

  appendWarning = (
    path: keyof DataSetQuery | string,
    warning: WarningViewModel
  ) => {
    append(this.warnings, path, warning);
  };

  prependWarning = (
    path: keyof DataSetQuery | string,
    warning: WarningViewModel
  ) => {
    prepend(this.warnings, path, warning);
  };

  getWarnings = () => ({ ...this.warnings });

  getErrors = () => ({ ...this.errors });

  hasErrors = () => Object.keys(this.errors).length > 0;

  hasWarnings = () => Object.keys(this.warnings).length > 0;
}

function prepend(dict: Dictionary<unknown[]>, path: string, value: unknown) {
  if (!dict[path]) {
    dict[path] = [];
  }

  // Avoid duplicates being added.
  if (!dict[path].some((existing) => isEqual(existing, value))) {
    dict[path].unshift(value);
  }
}

function append(dict: Dictionary<unknown[]>, path: string, value: unknown) {
  if (!dict[path]) {
    dict[path] = [];
  }

  // Avoid duplicates being added.
  if (!dict[path].some((existing) => isEqual(existing, value))) {
    dict[path].push(value);
  }
}
