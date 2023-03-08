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
  DataSetQueryCriteriaLocationAttributes,
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
  const rawFilterItemIds = new Set<string>();
  const rawLocationIds = new Set<string>();
  const rawLocationAttributeCodes = new Set<string>();

  const collectLocationIds = createParser<
    DataSetQueryCriteriaLocations,
    string
  >({
    state,
    parser: (comparator, values) => {
      values.forEach((value) => rawLocationIds.add(value));
    },
  });

  const collectLocationAttributeCodes = createParser<
    DataSetQueryCriteriaLocationAttributes,
    string
  >({
    state,
    parser: (comparator, values) => {
      values.forEach((value) => rawLocationAttributeCodes.add(value));
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
    locationAttributes: collectLocationAttributeCodes,
    timePeriods: noop,
  });

  const [locationsParser, locationAttributesParser, filtersParser] =
    await Promise.all([
      createLocationsParser(state, [...rawLocationIds], locationCols),
      createLocationAttributesParser(
        state,
        [...rawLocationAttributeCodes],
        locationCols
      ),
      createFiltersParser(state, [...rawFilterItemIds]),
    ]);

  // Perform a second pass, which actually constructs the query.
  return parseClause(facets, 'facets', {
    filters: filtersParser,
    geographicLevels: createGeographicLevelsParser(state, locationCols),
    locations: locationsParser,
    locationAttributes: locationAttributesParser,
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

async function createLocationsParser(
  state: DataSetQueryState,
  rawLocationIds: string[],
  locationCols: string[]
) {
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

  return createParser<DataSetQueryCriteriaLocations, string>({
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
}

async function createLocationAttributesParser(
  state: DataSetQueryState,
  locationAttributeCodes: string[],
  locationCols: string[]
): Promise<CriteriaParser<DataSetQueryCriteriaLocationAttributes>> {
  const locationCodeCols = Object.values(geographicLevelColumns)
    .filter((cols) => locationCols.includes(cols.code))
    .map((cols) => cols.code);

  const locations = await getLocationsByAttributes(
    state,
    locationAttributeCodes,
    locationCodeCols
  );
  const locationsByCode = locationAttributeCodes.reduce<
    Record<string, Location | undefined>
  >((acc, attributeCode) => {
    acc[attributeCode] = locations.find((location) =>
      locationCodeCols.some((col) => location[col] === attributeCode)
    );

    return acc;
  }, {});

  return createParser<DataSetQueryCriteriaLocationAttributes, string>({
    state,
    parser: (comparator, values, { path }) => {
      const matchingValues = values.filter((value) => !!locationsByCode[value]);

      if (matchingValues.length < values.length) {
        state.appendError(
          path,
          genericErrors.notFound({
            items: values.filter((value) => !locationsByCode[value]),
          })
        );
      }

      const params = locationCodeCols.flatMap((_) => matchingValues);

      switch (comparator) {
        case 'eq': {
          return {
            fragment: `(${locationCodeCols
              .map((col) => `locations.${col} = ?`)
              .join(' OR ')})`,
            params,
          };
        }
        case 'notEq': {
          return {
            fragment: `(${locationCodeCols
              .map((col) => `locations.${col} = ?`)
              .join(' OR ')})`,
            params,
          };
        }
        case 'in': {
          return params.length > 0
            ? {
                fragment: `(${locationCodeCols
                  .map(
                    (col) =>
                      `locations.${col} IN (${placeholders(matchingValues)})`
                  )
                  .join(' OR ')})`,
                params,
              }
            : undefined;
        }
        case 'notIn': {
          return params.length > 0
            ? {
                fragment: `(${locationCodeCols
                  .map(
                    (col) =>
                      `locations.${col} NOT IN (${placeholders(
                        matchingValues
                      )})`
                  )
                  .join(' OR ')})`,
                params,
              }
            : undefined;
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

async function getLocationsByAttributes(
  { db, tableFile }: DataSetQueryState,
  locationAttributeCodes: string[],
  locationCodeCols: string[]
): Promise<Location[]> {
  const codes = compact(locationAttributeCodes);

  if (!codes.length) {
    return [];
  }

  const codePlaceholders = indexPlaceholders(codes);

  return await db.all<Location>(
    `
      SELECT *
      FROM '${tableFile('locations')}'
      WHERE ${locationCodeCols
        .map((col) => `${col} IN (${codePlaceholders})`)
        .join(' OR ')}`,
    codes
  );
}
