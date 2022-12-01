import Hashids from 'hashids';
import {
  compact,
  groupBy,
  keyBy,
  mapValues,
  noop,
  pickBy,
  zipObject,
} from 'lodash';
import { ValueOf } from 'type-fest';
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

type CriteriaParser<TCriteria extends ValueOf<DataSetQueryCriteria>> = (
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

  // Perform a first pass to collect any ids so that we can
  // fetch the actual metadata entities. We'll need these
  // to be able to construct the actual parsed query.
  parseClause(query, {
    filters: createParser<DataSetQueryCriteriaFilters, string>({
      fragment: (comparator, values) => {
        values.forEach((value) => rawFilterItemIdSet.add(value));
        return '';
      },
    }),
    geographicLevels: noop,
    locations: createParser<DataSetQueryCriteriaLocations, string>({
      fragment: (comparator, values) => {
        values.forEach((value) => rawLocationIdSet.add(value));
        return '';
      },
    }),
    timePeriods: noop,
  });

  const [locationParser, filtersParser] = await Promise.all([
    createLocationParser(
      db,
      dataSetDir,
      [...rawLocationIdSet],
      locationIdHasher,
      locationCols
    ),
    createFiltersParser(
      db,
      dataSetDir,
      [...rawFilterItemIdSet],
      filterIdHasher
    ),
  ]);

  // Perform a second pass, which actually constructs the query.
  return parseClause(query, {
    filters: filtersParser,
    geographicLevels: createParser({
      fragment: parseGeographicLevelFragment,
      params: (values) => values.map((val) => geographicLevelCsvLabels[val]),
    }),
    locations: locationParser,
    timePeriods: createParser({
      fragment: parseTimePeriodFragment,
      params: (values) =>
        values.flatMap(({ year, code }) => [
          year,
          timePeriodCodeIdentifiers[code],
        ]),
    }),
  });
}

function parseClause(
  clause: DataSetQueryConditions | DataSetQueryCriteria,
  parsers: CriteriaParsers
): QueryFragmentParams {
  if ('and' in clause) {
    return parseSubConditions(clause.and, 'AND', parsers);
  } else if ('or' in clause) {
    return parseSubConditions(clause.or, 'OR', parsers);
  }

  return parseCriteria(clause, parsers);
}

function parseSubConditions(
  subClauses: (DataSetQueryConditions | DataSetQueryCriteria)[],
  condition: 'AND' | 'OR',
  parsers: CriteriaParsers
): QueryFragmentParams {
  return subClauses.reduce<QueryFragmentParams>(
    (acc, clause) => {
      const parsed = parseClause(clause, parsers);

      if (parsed.fragment) {
        acc.fragment = acc.fragment
          ? `(${acc.fragment}) ${condition} (${parsed.fragment})`
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

      if (parsed?.fragment) {
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

function createParser<
  TCriteria extends ValueOf<DataSetQueryCriteria>,
  TValue extends ValueOf<TCriteria> = ValueOf<TCriteria>,
  TComparator extends keyof TCriteria = keyof TCriteria
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

        if (fragment) {
          acc.fragment = acc.fragment
            ? `${acc.fragment} AND ${fragment}`
            : fragment;

          const params = options.params?.(values) ?? values;
          acc.params.push(...params);
        }

        return acc;
      },
      {
        fragment: '',
        params: [],
      }
    );
  };
}

async function createFiltersParser(
  db: Database,
  dataSetDir: string,
  rawFilterItemIds: string[],
  filterIdHasher: Hashids
): Promise<CriteriaParser<DataSetQueryCriteriaFilters>> {
  const filterItemIds = parseIdHashes(rawFilterItemIds, filterIdHasher);
  const filterItemIdsByRawId = zipObject(rawFilterItemIds, filterItemIds);

  const filterItems = await getFilterItems(db, dataSetDir, filterItemIds);
  const filterItemsById = keyBy(filterItems, (filter) => filter.id);

  return createParser<DataSetQueryCriteriaFilters, string>({
    fragment: (comparator, values) => {
      const matchingItems = compact(
        values.map((value) => {
          const id = filterItemIdsByRawId[value];
          return filterItemsById[id];
        })
      );

      const groupedMatchingItems = () =>
        groupBy(matchingItems, (item) => item.group_name);

      // Have to be more careful to handle negative cases as we
      // need matching filter items to construct the conditions.
      switch (comparator) {
        case 'eq':
          return matchingItems.length > 0
            ? `data."${matchingItems[0].group_name}" = ?`
            : 'false';
        case 'notEq':
          return matchingItems.length > 0
            ? `data."${matchingItems[0].group_name}" != ?`
            : 'true';
        case 'in':
          return matchingItems.length > 0
            ? `(${Object.entries(groupedMatchingItems())
                .map(
                  ([group, items]) =>
                    `data."${group}" IN (${placeholders(items)})`
                )
                .join(' AND ')})`
            : '';
        case 'notIn':
          return matchingItems.length > 0
            ? `(${Object.entries(groupedMatchingItems())
                .map(
                  ([group, items]) =>
                    `data."${group}" NOT IN (${placeholders(items)})`
                )
                .join(' AND ')})`
            : '';
      }
    },
    params: (values) =>
      compact(
        values.map((value) => {
          const id = filterItemIdsByRawId[value];
          return filterItemsById[id]?.label;
        })
      ),
  });
}

async function createLocationParser(
  db: Database,
  dataSetDir: string,
  rawLocationIds: string[],
  locationIdHasher: Hashids,
  locationCols: string[]
): Promise<CriteriaParser<DataSetQueryCriteriaLocations>> {
  const locationIds = parseIdLikeStrings(rawLocationIds, locationIdHasher);
  const locationIdsByRawId = zipObject(rawLocationIds, locationIds);

  const locations = await getLocations(
    db,
    dataSetDir,
    locationIds,
    locationCols
  );

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

  return createParser<DataSetQueryCriteriaLocations, string>({
    fragment: (comparator, values) => {
      switch (comparator) {
        case 'eq':
          return 'locations.id = ?';
        case 'notEq':
          return 'locations.id != ?';
        case 'in':
          return values.length > 0
            ? `locations.id IN (${placeholders(values)})`
            : '';
        case 'notIn':
          return values.length > 0
            ? `locations.id NOT IN (${placeholders(values)})`
            : '';
      }
    },
    params: (values) => values.map((val) => locationsByRawId[val]?.id ?? 0),
  });
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
      return values.length > 0
        ? `(data.time_period, data.time_identifier) IN (${values.map(
            (_) => '(?, ?)'
          )})`
        : '';
    case 'notIn':
      return values.length > 0
        ? `(data.time_period, data.time_identifier) NOT IN (${values.map(
            (_) => '(?, ?)'
          )})`
        : '';
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
      return values.length > 0
        ? `data.geographic_level IN (${placeholders(values)})`
        : '';
    case 'notIn':
      return values.length > 0
        ? `data.geographic_level NOT IN (${placeholders(values)})`
        : '';
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
