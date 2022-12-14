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
  DataSetQueryConditionAnd,
  DataSetQueryConditionNot,
  DataSetQueryConditionOr,
  DataSetQueryCriteria,
  DataSetQueryCriteriaFilters,
  DataSetQueryCriteriaGeographicLevels,
  DataSetQueryCriteriaLocations,
  DataSetQueryCriteriaTimePeriods,
  GeographicLevel,
  TimePeriodViewModel,
} from '../schema';
import { Filter, Location } from '../types/dbSchemas';
import { genericErrors } from '../validations/errors';
import { criteriaWarnings } from '../validations/warnings';
import DataSetQueryState from './DataSetQueryState';
import {
  columnsToGeographicLevel,
  geographicLevelColumns,
  geographicLevelCsvLabels,
} from './locationConstants';
import parseIdHashes from './parseIdHashes';
import parseIdLikeStrings from './parseIdLikeStrings';
import { indexPlaceholders, placeholders } from './queryUtils';
import { timePeriodCodeIdentifiers } from './timePeriodConstants';

interface QueryFragmentParams {
  fragment: string;
  params: any[];
}

type DataSetQueryConditions =
  | DataSetQueryConditionAnd
  | DataSetQueryConditionOr
  | DataSetQueryConditionNot;

type CriteriaParser<TCriteria extends ValueOf<DataSetQueryCriteria>> = (
  criteria: TCriteria,
  path: string
) => QueryFragmentParams | void;

type CriteriaParsers = Required<{
  [Key in keyof DataSetQueryCriteria]: CriteriaParser<
    Required<DataSetQueryCriteria>[Key]
  >;
}>;

export type FilterItem = Pick<Filter, 'id' | 'label' | 'group_name'>;

export default async function parseDataSetQueryConditions(
  state: DataSetQueryState,
  { facets }: DataSetQuery,
  locationCols: string[]
): Promise<QueryFragmentParams> {
  const rawFilterItemIdSet = new Set<string>();
  const rawLocationIdSet = new Set<string>();

  const collectLocationIds = createParser<DataSetQueryCriteriaFilters, string>({
    state,
    fragment: (comparator, values) => {
      values.forEach((value) => rawLocationIdSet.add(value));
      return '';
    },
  });

  // Perform a first pass to collect any ids so that we can
  // fetch the actual metadata entities. We'll need these
  // to be able to construct the actual parsed query.
  parseClause(facets, 'facets', {
    filters: createParser<DataSetQueryCriteriaFilters, string>({
      state,
      fragment: (comparator, values) => {
        values.forEach((value) => rawFilterItemIdSet.add(value));
        return '';
      },
    }),
    geographicLevels: noop,
    locations: collectLocationIds,
    parentLocations: collectLocationIds,
    timePeriods: noop,
  });

  const [{ parentLocationsParser, locationsParser }, filtersParser] =
    await Promise.all([
      createLocationParsers(state, [...rawLocationIdSet], locationCols),
      createFiltersParser(state, [...rawFilterItemIdSet]),
    ]);

  // Perform a second pass, which actually constructs the query.
  return parseClause(facets, 'facets', {
    filters: filtersParser,
    geographicLevels: createGeographicLevelsParser(state, locationCols),
    locations: locationsParser,
    parentLocations: parentLocationsParser,
    timePeriods: createTimePeriodsParser(state),
  });
}

function parseClause(
  clause: DataSetQueryConditions | DataSetQueryCriteria,
  path: string,
  parsers: CriteriaParsers
): QueryFragmentParams {
  if ('and' in clause) {
    return parseSubClauses(clause.and, `${path}.and`, 'AND', parsers);
  } else if ('or' in clause) {
    return parseSubClauses(clause.or, `${path}.or`, 'OR', parsers);
  } else if ('not' in clause) {
    return parseClause(clause.not, `${path}.not`, parsers);
  }

  return parseCriteria(clause, path, parsers);
}

function parseSubClauses(
  subClauses: (DataSetQueryConditions | DataSetQueryCriteria)[],
  path: string,
  condition: 'AND' | 'OR',
  parsers: CriteriaParsers
): QueryFragmentParams {
  return subClauses.reduce<QueryFragmentParams>(
    (acc, clause, index) => {
      const parsed = parseClause(clause, `${path}[${index}]`, parsers);

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
  path: string,
  parsers: CriteriaParsers
): QueryFragmentParams {
  return Object.entries(criteria).reduce<QueryFragmentParams>(
    (acc, [k, comparators]) => {
      const key = k as keyof DataSetQueryCriteria;
      const parser = parsers[key] as CriteriaParser<any>;

      if (!parser) {
        throw new Error(`No matching parser for '${key}'`);
      }

      const parsed = parser(comparators, `${path}.${key}`);

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
  state: DataSetQueryState;
  fragment: (
    comparator: TComparator,
    values: TValue[],
    meta: { path: string; criteria: TCriteria }
  ) => string;
  params?: (values: TValue[]) => (string | number | boolean)[];
}): CriteriaParser<TCriteria> {
  return (criteria, path) => {
    return Object.entries(criteria as any).reduce<QueryFragmentParams>(
      (acc, [key, value]) => {
        const comparator = key as TComparator;

        const values = Array.isArray(value) ? value : [value];

        if (values.length === 0) {
          options.state.appendWarning(path, criteriaWarnings.empty);
        }

        const fragment = options.fragment(comparator, values, {
          path: `${path}.${key}`,
          criteria,
        });

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
  state: DataSetQueryState,
  rawFilterItemIds: string[]
): Promise<CriteriaParser<DataSetQueryCriteriaFilters>> {
  const filterItemIds = parseIdHashes(rawFilterItemIds, state.filterIdHasher);
  const filterItemIdsByRawId = zipObject(rawFilterItemIds, filterItemIds);

  const filterItems = await getFilterItems(state, filterItemIds);
  const filterItemsById = keyBy(filterItems, (filter) => filter.id);

  return createParser<DataSetQueryCriteriaFilters, string>({
    state,
    fragment: (comparator, values, { path }) => {
      const matchingItems = compact(
        values.map((value) => {
          const id = filterItemIdsByRawId[value];
          return filterItemsById[id];
        })
      );

      if (matchingItems.length < values.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !filterItemIdsByRawId[value]),
          })
        );
      }

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

async function createLocationParsers(
  state: DataSetQueryState,
  rawLocationIds: string[],
  locationCols: string[]
): Promise<{
  locationsParser: CriteriaParser<DataSetQueryCriteriaLocations>;
  parentLocationsParser: CriteriaParser<DataSetQueryCriteriaLocations>;
}> {
  const locationIds = parseIdLikeStrings(
    rawLocationIds,
    state.locationIdHasher
  );
  const locationIdsByRawId = zipObject(rawLocationIds, locationIds);

  const locations = await getLocations(state, locationIds, locationCols);

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

  const locationsParser = createParser<DataSetQueryCriteriaLocations, string>({
    state,
    fragment: (comparator, values, { path }) => {
      const matchingLocations = compact(
        values.map((value) => locationsByRawId[value])
      );

      if (matchingLocations.length < values.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !locationsByRawId[value]),
          })
        );
      }

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
    params: (values) => values.map((value) => locationsByRawId[value]?.id ?? 0),
  });

  const parentLocationsParser = await createParentLocationParser(
    state,
    locations,
    locationsByRawId
  );

  return {
    locationsParser,
    parentLocationsParser,
  };
}

async function createParentLocationParser(
  state: DataSetQueryState,
  locations: Location[],
  locationsByRawId: Dictionary<Location | undefined>
): Promise<CriteriaParser<DataSetQueryCriteriaLocations>> {
  const { db, tableFile } = state;

  // Create a temporary table here so that we can reference it in the parent locations
  // query fragment.  We can't reference CTE tables in sub-queries, so the fragment
  // becomes really verbose without this. Additionally, if we load just the filtered
  // locations into memory, we don't need to join on all locations multiple times.
  await db.run(
    `
    CREATE TABLE locations_filtered AS 
    SELECT * FROM '${tableFile('locations')}' 
    WHERE ${
      // Default to false as there are no locations - hence the table should be empty
      locations.length > 0 ? `id IN (${placeholders(locations)})` : 'false'
    }`,
    locations.map((location) => location.id)
  );

  return createParser<DataSetQueryCriteriaLocations, string>({
    state,
    fragment: (comparator, values, { path }) => {
      const matchingLocations = compact(
        values.map((value) => locationsByRawId[value])
      );

      const getCols = (
        locations: Location[]
      ): { data: string[]; locations: string[] } => {
        const cols = locations.reduce<Set<string>>((acc, location) => {
          Object.entries(location).forEach(([col, value]) => {
            if (value && col !== 'geographic_level' && col !== 'id') {
              acc.add(col);
            }
          });

          return acc;
        }, new Set());

        return {
          data: [...cols].map((col) => `data.${col}`),
          locations: [...cols].map((col) => `locations_filtered.${col}`),
        };
      };

      const locationsByGeographicLevel = groupBy(
        matchingLocations,
        (location) => location.geographic_level
      );

      if (matchingLocations.length < values.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !locationsByRawId[value]),
          })
        );
      }

      // (...columns) in (select (...columns) from locations_filtered where id = ?)
      //    and data.geographic_level != '<level>'

      switch (comparator) {
        case 'eq': {
          if (!matchingLocations.length) {
            return 'false';
          }

          const [location] = matchingLocations;
          const cols = getCols([location]);

          return `(${cols.data}) = (SELECT (${cols.locations}) FROM locations_filtered WHERE id = ?)
            AND data.geographic_level != '${location.geographic_level}'`;
        }
        case 'notEq': {
          if (!matchingLocations.length) {
            return 'true';
          }

          const [location] = matchingLocations;
          const cols = getCols([location]);

          return `(${cols.data}) != (SELECT (${cols.locations}) FROM locations_filtered WHERE id = ?)
            AND data.geographic_level != '${location.geographic_level}'`;
        }
        case 'in': {
          if (!matchingLocations.length) {
            return '';
          }

          return Object.entries(locationsByGeographicLevel)
            .map(([geographicLevel, locations]) => {
              const cols = getCols(locations);

              return `((${cols.data}) IN (SELECT (${
                cols.locations
              }) FROM locations_filtered WHERE id IN (${placeholders(
                locations
              )})) AND data.geographic_level != '${geographicLevel}')`;
            })
            .join(' AND ');
        }
        case 'notIn': {
          if (!matchingLocations.length) {
            return '';
          }

          return Object.entries(locationsByGeographicLevel)
            .map(([geographicLevel, locations]) => {
              const cols = getCols(locations);

              return `((${cols.data}) NOT IN (SELECT (${
                cols.locations
              }) FROM locations_filtered WHERE id IN (${placeholders(
                locations
              )})) AND data.geographic_level != '${geographicLevel}')`;
            })
            .join(' AND ');
        }
      }
    },
    params: (values) => values.map((val) => locationsByRawId[val]?.id ?? 0),
  });
}

function createTimePeriodsParser(
  state: DataSetQueryState
): CriteriaParser<DataSetQueryCriteriaTimePeriods> {
  return createParser<DataSetQueryCriteriaTimePeriods, TimePeriodViewModel>({
    state,
    fragment: (comparator, values) => {
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
    },
    params: (values) =>
      values.flatMap(({ year, code }) => [
        year,
        timePeriodCodeIdentifiers[code],
      ]),
  });
}

function createGeographicLevelsParser(
  state: DataSetQueryState,
  locationCols: string[]
): CriteriaParser<DataSetQueryCriteriaGeographicLevels> {
  const allowedLevels = locationCols.reduce((acc, col) => {
    acc.add(columnsToGeographicLevel[col]);
    return acc;
  }, new Set<GeographicLevel>());

  return createParser<DataSetQueryCriteriaGeographicLevels, GeographicLevel>({
    state,
    fragment: (comparator, values, { path }) => {
      const matchingLevels = values.filter((value) => allowedLevels.has(value));

      if (!matchingLevels.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !allowedLevels.has(value)),
          })
        );
      }

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
    },
    params: (values) => values.map((value) => geographicLevelCsvLabels[value]),
  });
}

async function getFilterItems(
  { db, tableFile }: DataSetQueryState,
  filterItemIds: number[]
): Promise<FilterItem[]> {
  const ids = compact(filterItemIds);

  if (!ids.length) {
    return [];
  }

  return await db.all<Filter>(
    `SELECT id, label, group_name
        FROM '${tableFile('filters')}'
        WHERE id IN (${placeholders(ids)});
    `,
    ids
  );
}

async function getLocations(
  { db, tableFile }: DataSetQueryState,
  locationIds: string[],
  locationCols: string[]
): Promise<Location[]> {
  const ids = compact(locationIds);

  if (!ids.length) {
    return [];
  }

  const idPlaceholders = indexPlaceholders(ids);
  const allowedGeographicLevelCols = pickBy(geographicLevelColumns, (col) =>
    locationCols.includes(col.code)
  );

  return await db.all<Location>(
    `
      SELECT *
      FROM '${tableFile('locations')}'
      WHERE id::VARCHAR IN (${idPlaceholders})
        OR ${Object.entries(allowedGeographicLevelCols)
          .map(([geographicLevel, col]) => {
            const label =
              geographicLevelCsvLabels[geographicLevel as GeographicLevel];

            return `(geographic_level = '${label}' AND ${col.code} IN (${idPlaceholders}))`;
          })
          .join(' OR ')}`,
    ids
  );
}
