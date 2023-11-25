export type TableName =
  | 'data'
  | 'indicators'
  | 'filters'
  | 'time_periods'
  | 'locations';

export function tableFile(dataSetDir: string, table: TableName) {
  if (table === 'data') {
    return `${dataSetDir}/data_normalised.parquet`;
  }

  return `${dataSetDir}/${table}.parquet`;
}
