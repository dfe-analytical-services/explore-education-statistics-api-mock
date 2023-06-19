import {
  DataSetMetaViewModel,
  FilterMetaViewModel,
  GeographicLevel,
  IndicatorMetaViewModel,
  LocationLevelMetaViewModel,
  LocationMetaViewModel,
  TimePeriodMetaViewModel,
  Unit,
} from '../schema';
import {
  FilterRow,
  IndicatorRow,
  LocationRow,
  TimePeriodRow,
} from '../types/dbSchemas';
import Database from './Database';
import { tableFile } from './dataSetPaths';
import formatTimePeriodLabel from './formatTimePeriodLabel';
import getDataSetDir from './getDataSetDir';
import {
  createFilterIdHasher,
  createIndicatorIdHasher,
  createLocationIdHasher,
} from './idHashers';
import { geographicLevelLabels } from './locationConstants';
import parseTimePeriodCode from './parseTimePeriodCode';

export default async function getDataSetMeta(
  dataSetId: string
): Promise<Omit<DataSetMetaViewModel, '_links'>> {
  const dataSetDir = getDataSetDir(dataSetId);
  const db = new Database();

  const { total } = await db.first<{ total: number }>(
    `SELECT count(*) as total FROM '${tableFile(dataSetDir, 'data')}';`
  );

  try {
    return {
      totalResults: total,
      timePeriods: await getTimePeriodsMeta(db, dataSetDir),
      filters: await getFiltersMeta(db, dataSetDir),
      indicators: await getIndicatorsMeta(db, dataSetDir),
      locations: await getLocationsMeta(db, dataSetDir),
    };
  } finally {
    db.close();
  }
}

async function getTimePeriodsMeta(
  db: Database,
  dataSetDir: string
): Promise<TimePeriodMetaViewModel[]> {
  const timePeriods = await db.all<TimePeriodRow>(
    `SELECT *
       FROM '${tableFile(dataSetDir, 'time_periods')}';`
  );

  return timePeriods.map((timePeriod) => {
    const code = parseTimePeriodCode(timePeriod.identifier);

    return {
      code,
      label: formatTimePeriodLabel(code, timePeriod.year),
      year: timePeriod.year,
    };
  });
}

async function getLocationsMeta(
  db: Database,
  dataSetDir: string
): Promise<LocationLevelMetaViewModel[]> {
  const locations = await db.all<LocationRow>(
    `SELECT * FROM '${tableFile(dataSetDir, 'locations')}'`
  );

  if (locations.length === 0) {
    return [];
  }

  const hasher = createLocationIdHasher(dataSetDir);

  const grouped = locations.reduce<Dictionary<LocationMetaViewModel[]>>(
    (acc, location) => {
      if (!acc[location.level]) {
        acc[location.level] = [];
      }

      acc[location.level].push({
        id: hasher.encode(location.id),
        code: location.code,
        name: location.name,
      });

      return acc;
    },
    {}
  );

  return Object.entries(grouped).map(([level, locations]) => {
    const geographicLevel = level as GeographicLevel;

    return {
      label: geographicLevelLabels[geographicLevel],
      level: geographicLevel,
      options: locations,
    };
  });
}

async function getFiltersMeta(
  db: Database,
  dataSetDir: string
): Promise<FilterMetaViewModel[]> {
  const filePath = tableFile(dataSetDir, 'filters');

  const groups = await db.all<{ label: string; name: string; hint: string }>(
    `SELECT DISTINCT 
        group_label AS label,
        group_name AS name,
        group_hint AS hint
      FROM '${filePath}';`
  );

  const hasher = createFilterIdHasher(dataSetDir);

  const filtersMeta: FilterMetaViewModel[] = [];

  for (const group of groups) {
    const items = await db.all<
      Pick<FilterRow, 'id' | 'label' | 'is_aggregate'>
    >(
      `SELECT id, label, is_aggregate FROM '${filePath}' WHERE group_label = ? ORDER BY label ASC`,
      [group.label]
    );

    filtersMeta.push({
      label: group.label,
      name: group.name,
      hint: group.hint,
      options: items.map((item) => {
        return {
          id: hasher.encode(item.id),
          label: item.label,
          isAggregate: item.is_aggregate || undefined,
        };
      }),
    });
  }

  return filtersMeta;
}

async function getIndicatorsMeta(
  db: Database,
  dataSetDir: string
): Promise<IndicatorMetaViewModel[]> {
  const hasher = createIndicatorIdHasher(dataSetDir);

  const indicators = await db.all<IndicatorRow>(
    `SELECT * FROM '${tableFile(dataSetDir, 'indicators')}' ORDER BY label ASC;`
  );

  return indicators.map((indicator) => {
    return {
      id: hasher.encode(indicator.id),
      label: indicator.label,
      name: indicator.name,
      unit: indicator.unit as Unit,
      decimalPlaces: indicator.decimal_places || undefined,
    };
  });
}
