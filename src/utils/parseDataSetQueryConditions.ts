import { compact, groupBy, keyBy, noop, partition, zipObject } from 'lodash';
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
import { Filter } from '../types/dbSchemas';
import { genericErrors } from '../validations/errors';
import { criteriaWarnings } from '../validations/warnings';
import DataSetQueryState from './DataSetQueryState';
import { parseIdHashes, parseIdHashesAndCodes } from './idParsers';
import {
  geographicLevelColumns,
  geographicLevelCsvLabels,
} from './locationConstants';
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
  geographicLevels: Set<GeographicLevel>
): Promise<QueryFragmentParams> {
  const rawFilterItemIds = new Set<string>();
  const rawLocationIds = new Set<string>();

  const collectLocationIds = createParser<
    DataSetQueryCriteriaLocations,
    string
  >({
    state,
    parser: (comparator, values) => {
      values.forEach((value) => rawLocationIds.add(value));
    },
  });

  // Perform a first pass to collect any ids so that we can
  // fetch the actual metadata entities. We'll need these
  // to be able to construct the actual parsed query.
  parseClause(facets, 'facets', {
    filters: createParser<DataSetQueryCriteriaFilters, string>({
      state,
      parser: (comparator, values) => {
        values.forEach((value) => rawFilterItemIds.add(value));
      },
    }),
    geographicLevels: noop,
    locations: collectLocationIds,
    timePeriods: noop,
  });

  const [locationsParser, filtersParser] = await Promise.all([
    createLocationsParser(state, [...rawLocationIds], geographicLevels),
    createFiltersParser(state, [...rawFilterItemIds]),
  ]);

  // Perform a second pass, which actually constructs the query.
  return parseClause(facets, 'facets', {
    filters: filtersParser,
    geographicLevels: createGeographicLevelsParser(state, geographicLevels),
    locations: locationsParser,
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

        return undefined;
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

async function createLocationsParser(
  state: DataSetQueryState,
  rawLocationIds: string[],
  geographicLevels: Set<GeographicLevel>
) {
  const { tableFile } = state;

  const parsedIds = parseIdHashesAndCodes(
    rawLocationIds,
    state.locationIdHasher
  );

  const rawToParsedIds = zipObject(rawLocationIds, parsedIds);

  const [ids, codes] = partition(parsedIds, (id) => typeof id === 'number') as [
    number[],
    string[]
  ];

  const locationCodeCols = [...geographicLevels].map(
    (level) => geographicLevelColumns[level].code
  );

  const [matchingLocationIds, matchingLocationCodes] = await Promise.all([
    getMatchingLocationIds(state, ids),
    getMatchingLocationCodes(state, codes, locationCodeCols),
  ]);

  return createParser<DataSetQueryCriteriaLocations, string>({
    state,
    parser: (comparator, values, { path }) => {
      const parsedIds = values.map((value) => rawToParsedIds[value]);

      const matchingValues = parsedIds.filter((parsedId) => {
        return typeof parsedId === 'number'
          ? matchingLocationIds.has(parsedId)
          : matchingLocationCodes.has(parsedId);
      });

      if (matchingValues.length < values.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !matchingValues.includes(value)),
          })
        );

        return undefined;
      }

      const [idParams, codeValues] = partition(
        parsedIds,
        (parsedId) => typeof parsedId === 'number'
      );

      const codeParams =
        codeValues.length > 0 ? locationCodeCols.flatMap(() => codeValues) : [];

      const params = [...idParams, ...codeParams];

      const createFragment = (options: {
        idFragment: string;
        codeFragment: string;
        join: string;
      }): string => {
        const fragments = compact([
          idParams.length > 0 ? options.idFragment : '',
          codeParams.length > 0 ? options.codeFragment : '',
        ]);

        return `(${fragments.join(` ${options.join} `)})`;
      };

      // We use a sub-query to get the ids of locations matched using codes.
      // This is necessary as adding constraints on location code columns
      // to the outer query's WHERE causes no results to be returned.
      // Not super sure why this happens, but probably related to the way
      // we join the locations table to the data table using structs.

      switch (comparator) {
        case 'eq': {
          return {
            fragment: createFragment({
              idFragment: 'locations.id = ?',
              codeFragment: `locations.id IN (
                SELECT id 
                FROM '${tableFile('locations')}'
                WHERE ${locationCodeCols
                  .map((col) => `${col} = ?`)
                  .join(' OR ')}
              )`,
              join: 'OR',
            }),
            params,
          };
        }
        case 'notEq':
          return {
            fragment: createFragment({
              idFragment: 'locations.id != ?',
              codeFragment: `locations.id NOT IN (
                SELECT id 
                FROM '${tableFile('locations')}'
                WHERE ${locationCodeCols
                  .map((col) => `${col} = ?`)
                  .join(' OR ')}
              )`,
              join: 'AND',
            }),
            params,
          };
        case 'in':
          return params.length > 0
            ? {
                fragment: createFragment({
                  idFragment: `locations.id IN (${placeholders(idParams)})`,
                  codeFragment: `locations.id IN (
                    SELECT id 
                    FROM '${tableFile('locations')}'
                    WHERE ${locationCodeCols
                      .map((col) => `${col} IN (${placeholders(codeValues)})`)
                      .join(' OR ')}
                  )`,
                  join: 'OR',
                }),
                params,
              }
            : undefined;
        case 'notIn':
          return values.length > 0
            ? {
                fragment: createFragment({
                  idFragment: `locations.id NOT IN (${placeholders(idParams)})`,
                  codeFragment: `locations.id NOT IN (
                    SELECT id 
                    FROM '${tableFile('locations')}'
                    WHERE ${locationCodeCols
                      .map((col) => `${col} IN (${placeholders(codeValues)})`)
                      .join(' OR ')}
                  )`,
                  join: 'AND',
                }),
                params,
              }
            : undefined;
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
  geographicLevels: Set<GeographicLevel>
): CriteriaParser<DataSetQueryCriteriaGeographicLevels> {
  return createParser<DataSetQueryCriteriaGeographicLevels, GeographicLevel>({
    state,
    parser: (comparator, values, { path }) => {
      const params = values
        .filter((value) => geographicLevels.has(value))
        .map((value) => geographicLevelCsvLabels[value]);

      if (!params.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !geographicLevels.has(value)),
          })
        );

        return undefined;
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

async function getMatchingLocationIds(
  { db, tableFile }: DataSetQueryState,
  locationIds: number[]
): Promise<Set<number>> {
  const ids = compact(locationIds);

  if (!ids.length) {
    return new Set();
  }

  const rows = await db.all<{ id: number }>(
    `
      SELECT id
      FROM '${tableFile('locations')}'
      WHERE id IN (${placeholders(ids)})`,
    ids
  );

  return new Set(rows.map((row) => row.id));
}

async function getMatchingLocationCodes(
  { db, tableFile }: DataSetQueryState,
  locationCodes: string[],
  locationCodeCols: string[]
): Promise<Set<string>> {
  const codes = compact(locationCodes);

  if (!codes.length) {
    return new Set();
  }

  const codePlaceholders = indexPlaceholders(codes);

  const rows = await db.all<Dictionary<string>>(
    `
      SELECT DISTINCT ${locationCodeCols}
      FROM '${tableFile('locations')}'
      WHERE ${locationCodeCols
        .map((col) => `${col} IN (${codePlaceholders})`)
        .join(' OR ')}`,
    codes
  );

  return rows.reduce<Set<string>>((acc, row) => {
    locationCodeCols.forEach((col) => {
      if (row[col]) {
        acc.add(row[col]);
      }
    });

    return acc;
  }, new Set());
}
