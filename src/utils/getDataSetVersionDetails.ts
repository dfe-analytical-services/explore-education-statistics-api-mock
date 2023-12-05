import { DataSetVersionViewModelMock } from '../mocks/dataSetVersions';
import { DataSetVersionViewModel } from '../schema';
import Database from './Database';
import { tableFile } from './dataSetPaths';
import getDataSetDir from './getDataSetDir';

export default async function getDataSetVersionDetails(
  dataSetId: string,
  dataSetVersions: DataSetVersionViewModelMock[],
): Promise<DataSetVersionViewModel[]> {
  const db = new Database();

  const mappedVersions: DataSetVersionViewModel[] = [];

  for (const dataSet of dataSetVersions) {
    const dataSetDir = getDataSetDir(dataSetId);

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

    mappedVersions.push({
      ...dataSet,
      filters: filters.map((filter) => filter.group_label),
      indicators: indicators.map((indicator) => indicator.label),
    });
  }

  return mappedVersions;
}
