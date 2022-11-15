export function tableFile(
  dataSetDir: string,
  table: 'data' | 'indicators' | 'filters' | 'time_periods' | 'locations'
) {
  return `${dataSetDir}/${table}.parquet`;
}
