import Hashids from 'hashids';
import {
  groupBy,
  isString,
  keyBy,
  mapValues,
  noop,
  pickBy,
  zipObject,
} from 'lodash';
import {
  DataSetQuery,
  DataSetQueryConditions,
  DataSetQueryCriteria,
  DataSetQueryCriteriaFilters,
  DataSetQueryCriteriaGeographicLevels,
  DataSetQueryCriteriaLocations,
  DataSetQueryCriteriaTimePeriods,
  GeographicLevel,
  TimePeriodViewModel,
} from '../schema';
import { Filter, Location } from '../types/dbSchemas';
import Database from './Database';
import { tableFile } from './dataSetPaths';
import {
  columnsToGeographicLevel,
  geographicLevelColumns,
  geographicLevelCsvLabels,
} from './locationConstants';
import parseIdHashes from './parseIdHashes';
import { parseIdLikeStrings } from './parseIdLikeStrings';
import { indexPlaceholders, placeholders } from './queryUtils';
import { timePeriodCodeIdentifiers } from './timePeriodConstants';

interface QueryFragmentParams {
  fragment: string;
  params: any[];
}

type CriteriaParser<TCriteria extends ValuesOf<DataSetQueryCriteria>> = (
  criteria: TCriteria
) => QueryFragmentParams | void;

type CriteriaParsers = Required<{
  [Key in keyof DataSetQueryCriteria]: CriteriaParser<
    Required<DataSetQueryCriteria>[Key]
  >;
}>;

export type FilterItem = Pick<Filter, 'id' | 'label' | 'group_name'>;

export default async function parseDataSetQueryConditions(
  db: Database,
  dataSetDir: string,
  { query }: DataSetQuery,
  locationCols: string[],
  filterIdHasher: Hashids,
  locationIdHasher: Hashids
): Promise<QueryFragmentParams> {
  const rawFilterItemIdSet = new Set<string>();
  const rawLocationIdSet = new Set<string>();

  const extractFilterItemIds: CriteriaParser<DataSetQueryCriteriaFilters> = (
    criteria
  ) => {
    Object.values(criteria).forEach((value) => {
      const values = Array.isArray(value) ? value : [value];
      values.forEach((value) => {
        rawFilterItemIdSet.add(value);
      });
    });
  };

  const extractLocationIds: CriteriaParser<DataSetQueryCriteriaLocations> = (
    criteria
  ) => {
    Object.values(criteria).forEach((value) => {
      const values = Array.isArray(value) ? value : [value];
      values.forEach((value) => {
        rawLocationIdSet.add(value);
      });
    });
  };

  const extractParsers: CriteriaParsers = {
    filters: extractFilterItemIds,
    geographicLevels: noop,
    locationParents: extractLocationIds,
    locations: extractLocationIds,
    timePeriods: noop,
  };

  if ('and' in query) {
    parseConditions(query.and, extractParsers);
  } else if ('or' in query) {
    parseConditions(query.or, extractParsers);
  }

  const rawLocationIds = [...rawLocationIdSet];
  const locationIds = parseIdLikeStrings(rawLocationIds, locationIdHasher);
  const locationIdsByRawId = zipObject(rawLocationIds, locationIds);

  const rawFilterItemIds = [...rawFilterItemIdSet];
  const filterItemIds = parseIdHashes(rawFilterItemIds, filterIdHasher);

  const [locations, filterItems] = await Promise.all([
    getLocations(db, dataSetDir, locationIds, locationCols),
    getFilterItems(db, dataSetDir, filterItemIds),
  ]);

  const locationCodeCols = locationCols.filter((col) => {
    const geographicLevel = columnsToGeographicLevel[col];

    if (!geographicLevel) {
      return false;
    }

    return geographicLevelColumns[geographicLevel].code === col;
  });

  const locationsByRawId = mapValues(locationIdsByRawId, (locationId) => {
    return locations.find(
      (location) =>
        location.id === Number(locationId) ||
        locationCodeCols.some((col) => location[col] === locationId)
    );
  });
  const groupedFilterItems = groupBy(
    filterItems,
    (filter) => filter.group_name
  );

  const conditionParsers: CriteriaParsers = {
    filters: noop,
    geographicLevels: createDefaultParser({
      fragment: parseGeographicLevelFragment,
    }),
    locationParents: noop,
    locations: createDefaultParser({
      fragment: parseLocationsFragment,
      params: (values) => values.map((val) => locationsByRawId[val]?.id ?? 0),
    }),
    timePeriods: createDefaultParser({
      fragment: parseTimePeriodFragment,
      params: (values) =>
        values.flatMap(({ year, code }) => [
          year,
          timePeriodCodeIdentifiers[code],
        ]),
    }),
  };

  if ('and' in query) {
    return parseConditions(query.and, conditionParsers);
  } else if ('or' in query) {
    return parseConditions(query.or, conditionParsers);
  } else {
    return {
      fragment: '',
      params: [],
    };
  }
}

function parseConditions(
  conditions: (DataSetQueryConditions | DataSetQueryCriteria)[],
  parsers: CriteriaParsers
): QueryFragmentParams {
  return conditions.reduce<QueryFragmentParams>(
    (acc, condition) => {
      let parsed: QueryFragmentParams;

      if ('and' in condition || 'or' in condition) {
        parsed = parseConditions(conditions, parsers);

        acc.fragment = acc.fragment
          ? `(${acc.fragment}) ${condition} (${parsed.fragment})`
          : `(${parsed.fragment})`;
      } else {
        parsed = parseCriteria(condition, parsers);

        acc.fragment = acc.fragment
          ? `${acc.fragment} ${parsed.fragment}`
          : parsed.fragment;
      }

      acc.params.push(...parsed.params);

      return acc;
    },
    {
      fragment: '',
      params: [],
    }
  );
}

function parseCriteria(
  criteria: DataSetQueryCriteria,
  parsers: CriteriaParsers
): QueryFragmentParams {
  return Object.entries(criteria).reduce<QueryFragmentParams>(
    (acc, [k, comparators]) => {
      const key = k as keyof DataSetQueryCriteria;
      const parser = parsers[key] as CriteriaParser<any>;

      if (!parser) {
        throw new Error(`No matching parser for '${key}'`);
      }

      const parsed = parser(comparators);

      if (parsed) {
        acc.fragment = acc.fragment
          ? `${acc.fragment} AND ${parsed.fragment}`
          : parsed.fragment;
        acc.params.push(...parsed.params);
      }

      return acc;
    },
    {
      fragment: '',
      params: [],
    }
  );
}

function createDefaultParser<
  TCriteria extends ValuesOf<DataSetQueryCriteria>,
  TComparator extends keyof TCriteria,
  TValue
>(options: {
  fragment: (comparator: TComparator, values: TValue[]) => string;
  params?: (values: TValue[]) => (string | number | boolean)[];
}): CriteriaParser<TCriteria> {
  return (criteria) => {
    return Object.entries(criteria as any).reduce<QueryFragmentParams>(
      (acc, [key, value]) => {
        const comparator = key as TComparator;

        const values = Array.isArray(value) ? value : [value];
        const fragment = options.fragment(comparator, values);
        const params = options.params?.(values) ?? values;

        acc.params.push(...params);
        acc.fragment = acc.fragment
          ? `${acc.fragment} AND ${fragment}`
          : fragment;

        return acc;
      },
      {
        fragment: '',
        params: [],
      }
    );
  };
}

function parseTimePeriodFragment(
  comparator: keyof Required<DataSetQueryCriteriaTimePeriods>,
  values: TimePeriodViewModel[]
): string {
  switch (comparator) {
    case 'eq':
      return '(data.time_period = ? AND data.time_identifier = ?)';
    case 'notEq':
      return '(data.time_period != ? AND data.time_identifier = ?)';
    case 'gte':
      return '(data.time_period >= ? AND data.time_identifier = ?)';
    case 'gt':
      return '(data.time_period > ? AND data.time_identifier = ?)';
    case 'lte':
      return '(data.time_period <= ? AND data.time_identifier = ?)';
    case 'lt':
      return '(data.time_period < ? AND data.time_identifier = ?)';
    case 'in':
      return `(data.time_period, data.time_identifier) IN (${values.map(
        (_) => '(?, ?)'
      )})`;
    case 'notIn':
      return `(data.time_period, data.time_identifier) NOT IN (${values.map(
        (_) => '(?, ?)'
      )})`;
  }
}

function parseGeographicLevelFragment(
  comparator: keyof Required<DataSetQueryCriteriaGeographicLevels>,
  values: GeographicLevel[]
): string {
  switch (comparator) {
    case 'eq':
      return 'data.geographic_level = ?';
    case 'notEq':
      return 'data.geographic_level != ?';
    case 'in':
      return `data.geographic_level IN (${placeholders(values)})`;
    case 'notIn':
      return `data.geographic_level NOT IN (${placeholders(values)})`;
  }
}

function parseLocationsFragment(
  comparator: keyof Required<DataSetQueryCriteriaLocations>,
  values: string[]
): string {
  switch (comparator) {
    case 'eq':
      return 'locations.id = ?';
    case 'notEq':
      return 'locations.id != ?';
    case 'in':
      return `locations.id IN (${placeholders(values)})`;
    case 'notIn':
      return `locations.id NOT IN (${placeholders(values)})`;
  }
}

async function getFilterItems(
  db: Database,
  dataSetDir: string,
  filterItemIds: number[]
): Promise<FilterItem[]> {
  if (!filterItemIds.length) {
    return [];
  }

  return await db.all<Filter>(
    `SELECT id, label, group_name
        FROM '${tableFile(dataSetDir, 'filters')}'
        WHERE id IN (${placeholders(filterItemIds)});
    `,
    filterItemIds
  );
}

async function getLocations(
  db: Database,
  dataSetDir: string,
  locationIds: string[],
  locationCols: string[]
): Promise<Location[]> {
  if (!locationIds.length) {
    return [];
  }

  const idPlaceholders = indexPlaceholders(locationIds);
  const allowedGeographicLevelCols = pickBy(geographicLevelColumns, (col) =>
    locationCols.includes(col.code)
  );

  return await db.all<Location>(
    `
      SELECT *
      FROM '${tableFile(dataSetDir, 'locations')}'
      WHERE id::VARCHAR IN (${idPlaceholders})
        OR ${Object.entries(allowedGeographicLevelCols)
          .map(([geographicLevel, col]) => {
            const label =
              geographicLevelCsvLabels[geographicLevel as GeographicLevel];

            return `(geographic_level = '${label}' AND ${col.code} IN (${idPlaceholders}))`;
          })
          .join(' OR ')}`,
    locationIds
  );
}
