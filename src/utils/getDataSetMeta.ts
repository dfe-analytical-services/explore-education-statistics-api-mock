import { invert } from 'lodash';
import {
  DataSetMetaViewModel,
  FilterMetaViewModel,
  GeographicLevel,
  IndicatorMetaViewModel,
  LocationMetaViewModel,
  TimePeriodMetaViewModel,
  Unit,
} from '../schema';
import { Filter, Indicator, TimePeriod } from '../types/dbSchemas';
import Database from './Database';
import { tableFile } from './dataSetPaths';
import formatTimePeriodLabel from './formatTimePeriodLabel';
import getDataSetDir from './getDataSetDir';
import {
  createFilterIdHasher,
  createIndicatorIdHasher,
  createLocationIdHasher,
} from './idHashers';
import {
  geographicLevelColumns,
  geographicLevelCsvLabels,
} from './locationConstants';
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
  const timePeriods = await db.all<TimePeriod>(
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
): Promise<Dictionary<LocationMetaViewModel[]>> {
  const filePath = tableFile(dataSetDir, 'locations');

  const levels = (
    await db.all<{ level: string }>(
      `SELECT DISTINCT geographic_level AS level FROM '${filePath}';`
    )
  ).map((row) => row.level);

  const geographicLevelLabels = invert(geographicLevelCsvLabels);

  const hasher = createLocationIdHasher(dataSetDir);

  const locationsMeta: Dictionary<LocationMetaViewModel[]> = {};

  for (const level of levels) {
    const geographicLevel = geographicLevelLabels[level] as
      | GeographicLevel
      | undefined;

    if (!geographicLevel) {
      throw new Error(`Invalid geographic level: ${level}`);
    }

    const cols = [
      'id',
      `${geographicLevelColumns[geographicLevel].code} AS code`,
      `${geographicLevelColumns[geographicLevel].name} AS label`,
    ];

    const levelLocations = await db.all<{
      id: number;
      code: string;
      label: string;
    }>(
      `SELECT ${cols} FROM '${filePath}' WHERE geographic_level = ? ORDER BY label ASC`,
      [level]
    );

    locationsMeta[geographicLevel] = levelLocations.map<LocationMetaViewModel>(
      (location) => {
        return {
          id: hasher.encode(location.id),
          code: location.code,
          label: location.label,
          level: geographicLevel,
        };
      }
    );
  }

  return locationsMeta;
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
    const items = await db.all<Pick<Filter, 'id' | 'label' | 'is_aggregate'>>(
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

  const indicators = await db.all<Indicator>(
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
