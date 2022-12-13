export type TableName =
  | 'data'
  | 'indicators'
  | 'filters'
  | 'time_periods'
  | 'locations';

export function tableFile(dataSetDir: string, table: TableName) {
  return `${dataSetDir}/${table}.parquet`;
}
