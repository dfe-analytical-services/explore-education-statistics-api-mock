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
  csvLabelsToGeographicLevels,
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
    parser: (comparator, values) => {
      values.forEach((value) => rawLocationIdSet.add(value));
    },
  });

  // Perform a first pass to collect any ids so that we can
  // fetch the actual metadata entities. We'll need these
  // to be able to construct the actual parsed query.
  parseClause(facets, 'facets', {
    filters: createParser<DataSetQueryCriteriaFilters, string>({
      state,
      parser: (comparator, values) => {
        values.forEach((value) => rawFilterItemIdSet.add(value));
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
  parser: (
    comparator: TComparator,
    values: TValue[],
    meta: { path: string; criteria: TCriteria }
  ) => {
    fragment: string;
    params?: (string | number | boolean)[];
  } | void;
}): CriteriaParser<TCriteria> {
  return (criteria, path) => {
    return Object.entries(criteria as any).reduce<QueryFragmentParams>(
      (acc, [key, value]) => {
        const comparator = key as TComparator;

        const values = Array.isArray(value) ? value : [value];

        if (values.length === 0) {
          options.state.appendWarning(path, criteriaWarnings.empty);
        }

        const result = options.parser(comparator, values, {
          path: `${path}.${key}`,
          criteria,
        });

        if (result) {
          const { fragment, params = [] } = result;

          acc.fragment = acc.fragment
            ? `${acc.fragment} AND ${fragment}`
            : fragment;
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
    parser: (comparator, values, { path }) => {
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

      const params = matchingItems.map((item) => item.label);

      // Have to be more careful to handle negative cases as we
      // need matching filter items to construct the conditions.
      switch (comparator) {
        case 'eq':
          return params.length > 0
            ? { fragment: `data."${matchingItems[0].group_name}" = ?`, params }
            : { fragment: 'false' };
        case 'notEq':
          return params.length > 0
            ? { fragment: `data."${matchingItems[0].group_name}" != ?`, params }
            : { fragment: 'true' };
        case 'in':
          return params.length > 0
            ? {
                fragment: `(${Object.entries(groupedMatchingItems())
                  .map(
                    ([group, items]) =>
                      `data."${group}" IN (${placeholders(items)})`
                  )
                  .join(' AND ')})`,
                params,
              }
            : undefined;
        case 'notIn':
          return params.length > 0
            ? {
                fragment: `(${Object.entries(groupedMatchingItems())
                  .map(
                    ([group, items]) =>
                      `data."${group}" NOT IN (${placeholders(items)})`
                  )
                  .join(' AND ')})`,
                params,
              }
            : undefined;
      }
    },
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
  const locationsByRawId = mapValues(locationIdsByRawId, (locationId) => {
    return locations.find((location) => {
      if (location.id === Number(locationId)) {
        return true;
      }

      const geographicLevel =
        csvLabelsToGeographicLevels[location.geographic_level];
      const codeCol = geographicLevelColumns[geographicLevel].code;

      return location[codeCol] === locationId;
    });
  });

  const locationsParser = createParser<DataSetQueryCriteriaLocations, string>({
    state,
    parser: (comparator, values, { path }) => {
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

      const params = matchingLocations.map((location) => location.id);

      switch (comparator) {
        case 'eq':
          return {
            fragment: 'locations.id = ?',
            params,
          };
        case 'notEq':
          return {
            fragment: 'locations.id != ?',
            params,
          };
        case 'in':
          return params.length > 0
            ? { fragment: `locations.id IN (${placeholders(values)})`, params }
            : undefined;
        case 'notIn':
          return values.length > 0
            ? {
                fragment: `locations.id NOT IN (${placeholders(values)})`,
                params,
              }
            : undefined;
      }
    },
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
    parser: (comparator, values, { path }) => {
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

      const params = matchingLocations.map((location) => location.id);

      // (...columns) in (select (...columns) from locations_filtered where id = ?)
      //    and data.geographic_level != '<level>'

      switch (comparator) {
        case 'eq': {
          if (!params.length) {
            return { fragment: 'false' };
          }

          const [location] = matchingLocations;
          const cols = getCols([location]);

          return {
            fragment: `(${cols.data}) = (SELECT (${cols.locations}) FROM locations_filtered WHERE id = ?)
              AND data.geographic_level != '${location.geographic_level}'`,
            params,
          };
        }
        case 'notEq': {
          if (!params.length) {
            return { fragment: 'true' };
          }

          const [location] = matchingLocations;
          const cols = getCols([location]);

          return {
            fragment: `(${cols.data}) != (SELECT (${cols.locations}) FROM locations_filtered WHERE id = ?)
              AND data.geographic_level != '${location.geographic_level}'`,
            params,
          };
        }
        case 'in': {
          if (!params.length) {
            return undefined;
          }

          return {
            fragment: Object.entries(locationsByGeographicLevel)
              .map(([geographicLevel, locations]) => {
                const cols = getCols(locations);

                return `((${cols.data}) IN (SELECT (${
                  cols.locations
                }) FROM locations_filtered WHERE id IN (${placeholders(
                  locations
                )})) AND data.geographic_level != '${geographicLevel}')`;
              })
              .join(' AND '),
            params,
          };
        }
        case 'notIn': {
          if (!params.length) {
            return undefined;
          }

          return {
            fragment: Object.entries(locationsByGeographicLevel)
              .map(([geographicLevel, locations]) => {
                const cols = getCols(locations);

                return `((${cols.data}) NOT IN (SELECT (${
                  cols.locations
                }) FROM locations_filtered WHERE id IN (${placeholders(
                  locations
                )})) AND data.geographic_level != '${geographicLevel}')`;
              })
              .join(' AND '),
            params,
          };
        }
      }
    },
  });
}

function createTimePeriodsParser(
  state: DataSetQueryState
): CriteriaParser<DataSetQueryCriteriaTimePeriods> {
  return createParser<DataSetQueryCriteriaTimePeriods, TimePeriodViewModel>({
    state,
    parser: (comparator, values) => {
      const params = values.flatMap(({ year, code }) => [
        year,
        timePeriodCodeIdentifiers[code],
      ]);
      const placeholders = values.map((_) => '(?, ?)');

      switch (comparator) {
        case 'eq':
          return {
            fragment: '(data.time_period = ? AND data.time_identifier = ?)',
            params,
          };
        case 'notEq':
          return {
            fragment: '(data.time_period != ? AND data.time_identifier = ?)',
            params,
          };
        case 'gte':
          return {
            fragment: '(data.time_period >= ? AND data.time_identifier = ?)',
            params,
          };
        case 'gt':
          return {
            fragment: '(data.time_period > ? AND data.time_identifier = ?)',
            params,
          };
        case 'lte':
          return {
            fragment: '(data.time_period <= ? AND data.time_identifier = ?)',
            params,
          };
        case 'lt':
          return {
            fragment: '(data.time_period < ? AND data.time_identifier = ?)',
            params,
          };
        case 'in':
          return params.length > 0
            ? {
                fragment: `(data.time_period, data.time_identifier) IN (${placeholders})`,
                params,
              }
            : undefined;
        case 'notIn':
          return params.length > 0
            ? {
                fragment: `(data.time_period, data.time_identifier) NOT IN (${placeholders})`,
                params,
              }
            : undefined;
      }
    },
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
    parser: (comparator, values, { path }) => {
      const params = values
        .filter((value) => allowedLevels.has(value))
        .map((value) => geographicLevelCsvLabels[value]);

      if (!params.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !allowedLevels.has(value)),
          })
        );
      }

      switch (comparator) {
        case 'eq':
          return { fragment: 'data.geographic_level = ?', params };
        case 'notEq':
          return { fragment: 'data.geographic_level != ?', params };
        case 'in':
          return params.length > 0
            ? {
                fragment: `data.geographic_level IN (${placeholders(params)})`,
                params,
              }
            : undefined;
        case 'notIn':
          return params.length > 0
            ? {
                fragment: `data.geographic_level NOT IN (${placeholders(
                  params
                )})`,
                params,
              }
            : undefined;
      }
    },
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
