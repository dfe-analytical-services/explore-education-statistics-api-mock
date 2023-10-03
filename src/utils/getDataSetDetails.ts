import { DataSetViewModel } from '../schema';
import Database from './Database';
import { tableFile } from './dataSetPaths';
import getDataSetDir from './getDataSetDir';

export default async function getDataSetDetails(
  dataSets: Omit<DataSetViewModel, 'indicators' | 'filters'>[],
): Promise<DataSetViewModel[]> {
  const db = new Database();

  const mappedDataSets: DataSetViewModel[] = [];

  for (const dataSet of dataSets) {
    const dataSetDir = getDataSetDir(dataSet.id);

    const [filters, indicators] = await Promise.all([
      db.all<{ group_label: string }>(
        `SELECT DISTINCT group_label FROM '${tableFile(
          dataSetDir,
          'filters',
        )}'`,
      ),
      db.all<{ label: string }>(
        `SELECT DISTINCT label FROM '${tableFile(dataSetDir, 'indicators')}'`,
      ),
    ]);

    mappedDataSets.push({
      ...dataSet,
      filters: filters.map((filter) => filter.group_label),
      indicators: indicators.map((indicator) => indicator.label),
    });
  }

  return mappedDataSets;
}
