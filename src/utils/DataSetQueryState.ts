import Hashids from 'hashids';
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

  constructor(public readonly dataSetDir: string) {
    this.filterIdHasher = createFilterIdHasher(dataSetDir);
    this.indicatorIdHasher = createIndicatorIdHasher(dataSetDir);
    this.locationIdHasher = createLocationIdHasher(dataSetDir);
  }

  tableFile = (name: TableName) => {
    return tableFile(this.dataSetDir, name);
  };
}
