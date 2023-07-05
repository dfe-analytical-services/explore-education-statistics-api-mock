import { compact, groupBy, keyBy, noop, zipObject } from 'lodash';
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
import { FilterRow, LocationRow } from '../types/dbSchemas';
import { genericErrors } from '../validations/errors';
import DataSetQueryState from './DataSetQueryState';
import { parseIdHashes, parseIdHashesAndCodes } from './idParsers';
import {
  geographicLevelColumns,
  geographicLevelCsvLabels,
} from './locationConstants';
import { placeholders } from './queryUtils';
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

type AllDataSetQueryCriteria = Required<DataSetQueryCriteria>;

type CriteriaParsers = {
  [Key in keyof AllDataSetQueryCriteria]: CriteriaParser<
    AllDataSetQueryCriteria[Key] extends any[]
      ? AllDataSetQueryCriteria[Key][number]
      : AllDataSetQueryCriteria[Key]
  >;
};

export type FilterItem = Pick<FilterRow, 'id' | 'label' | 'group_name'>;

export default async function parseDataSetQueryConditions(
  state: DataSetQueryState,
  { facets }: DataSetQuery,
  geographicLevels: Set<GeographicLevel>
): Promise<QueryFragmentParams> {
  const rawFilterItemIds = new Set<string>();
  const rawLocationIds = new Set<string>();

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
    locations: createParser<DataSetQueryCriteriaLocations, string>({
      state,
      parser: (comparator, values) => {
        values.forEach((value) => rawLocationIds.add(value));
      },
    }),
    timePeriods: noop,
  });

  const [locationsParser, filtersParser] = await Promise.all([
    createLocationsParser(state, [...rawLocationIds]),
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
    (acc, [k, value]) => {
      const key = k as keyof DataSetQueryCriteria;

      if (!parsers[key]) {
        throw new Error(`No matching parser for '${key}'`);
      }

      const parser = parsers[key] as CriteriaParser<Dictionary<unknown>>;

      const appendComparators = (comparators: Dictionary<unknown>) => {
        const parsed = parser(comparators, `${path}.${key}`);

        if (parsed?.fragment) {
          acc.fragment = acc.fragment
            ? `${acc.fragment} AND ${parsed.fragment}`
            : parsed.fragment;
          acc.params.push(...parsed.params);
        }
      };

      if (Array.isArray(value)) {
        value.forEach(appendComparators);
      } else {
        appendComparators(value);
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
>({
  parser,
}: {
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
        const result = parser(comparator, values, {
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
        state.appendWarning(
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
            : { fragment: 'false' };
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
            : { fragment: 'false' };
      }
    },
  });
}

async function createLocationsParser(
  state: DataSetQueryState,
  rawLocationIds: string[]
): Promise<CriteriaParser<DataSetQueryCriteriaLocations>> {
  const parsedIds = parseIdHashesAndCodes(
    rawLocationIds,
    state.locationIdHasher
  );

  const [ids, codes] = parsedIds.reduce(
    (acc, parsedId) => {
      if (typeof parsedId === 'number') {
        acc[0].add(parsedId);
      } else {
        acc[1].add(parsedId);
      }

      return acc;
    },
    [new Set<number>(), new Set<string>()]
  );

  const locations = await getLocations(state, [...ids], [...codes]);

  const rawToParsedIds = zipObject(parsedIds, rawLocationIds);
  const locationsByRawId = groupBy(locations, (location) => {
    return rawToParsedIds[ids.has(location.id) ? location.id : location.code];
  });

  return createParser<DataSetQueryCriteriaLocations, string>({
    state,
    parser: (comparator, values, { path }) => {
      const matchingLocations = compact(
        values.flatMap((value) => locationsByRawId[value] ?? [])
      );

      if (matchingLocations.length < values.length) {
        state.appendWarning(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !locationsByRawId[value]),
          })
        );
      }

      const locationsByLevel = groupBy(
        matchingLocations,
        (location) => location.level
      );

      const createFragment = ({
        comparator,
        join,
      }: {
        comparator: 'IN' | 'NOT IN';
        join: 'AND' | 'OR';
      }) => {
        const fragment = Object.entries(locationsByLevel)
          .map(([level, locations]) => {
            const cols = geographicLevelColumns[level as GeographicLevel];

            return `(${cols.code}, ${cols.name}) ${comparator} (${locations.map(
              (_) => '(?, ?)'
            )})`;
          })
          .join(` ${join} `);

        return `(${fragment})`;
      };

      const params = matchingLocations.flatMap((location) => [
        location.code,
        location.name,
      ]);

      switch (comparator) {
        case 'eq': {
          return params.length > 0
            ? {
                fragment: createFragment({ comparator: 'IN', join: 'OR' }),
                params,
              }
            : { fragment: 'false' };
        }
        case 'notEq':
          return params.length > 0
            ? {
                fragment: createFragment({ comparator: 'NOT IN', join: 'AND' }),
                params,
              }
            : { fragment: 'true' };
        case 'in':
          return params.length > 0
            ? {
                fragment: createFragment({ comparator: 'IN', join: 'OR' }),
                params,
              }
            : { fragment: 'false' };
        case 'notIn':
          return params.length > 0
            ? {
                fragment: createFragment({ comparator: 'NOT IN', join: 'AND' }),
                params,
              }
            : { fragment: 'true' };
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
          return {
            fragment: `(data.time_period, data.time_identifier) IN (${values.map(
              (_) => '(?, ?)'
            )})`,
            params,
          };
        case 'notIn':
          return {
            fragment: `(data.time_period, data.time_identifier) NOT IN (${values.map(
              (_) => '(?, ?)'
            )})`,
            params,
          };
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
      const params: string[] = values
        .filter((value) => geographicLevels.has(value))
        .map((value) => geographicLevelCsvLabels[value]);

      if (params.length < values.length) {
        state.appendWarning(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !geographicLevels.has(value)),
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

  return await db.all<FilterRow>(
    `SELECT id, label, group_name
        FROM '${tableFile('filters')}'
        WHERE id IN (${placeholders(ids)});
    `,
    ids
  );
}

async function getLocations(
  { db, tableFile }: DataSetQueryState,
  locationIds: number[],
  locationCodes: string[]
): Promise<LocationRow[]> {
  const ids = compact(locationIds);
  const codes = compact(locationCodes);

  if (!ids.length && !codes.length) {
    return [];
  }

  return await db.all<LocationRow>(
    `
      SELECT *
      FROM '${tableFile('locations')}'
      WHERE ${compact([
        ids.length > 0 ? `id IN (${placeholders(ids)})` : '',
        codes.length > 0 ? `code IN (${placeholders(codes)})` : '',
      ]).join(' OR ')}
      `,
    [...ids, ...codes]
  );
}
