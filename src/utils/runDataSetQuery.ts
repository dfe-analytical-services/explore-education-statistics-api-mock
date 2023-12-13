import { compact, keyBy, orderBy, uniq } from 'lodash';
import Papa from 'papaparse';
import { ValidationError } from '../errors';
import {
  DataSetQuery,
  DataSetQueryResultsViewModel,
  GeographicLevel,
  PagingViewModel,
} from '../schema';
import {
  DataRow,
  FilterRow,
  IndicatorRow,
  LocationRow,
} from '../types/dbSchemas';
import { arrayErrors, genericErrors } from '../validations/errors';
import { tableFile } from './dataSetPaths';
import { DataSetQueryMeta } from './DataSetQueryMeta';
import DataSetQueryState from './DataSetQueryState';
import {
  filterIdColumn,
  filterOrderColumn,
  locationIdColumn,
  locationOrderColumn,
} from './dataSetQueryUtils';
import getDataSetDir from './getDataSetDir';
import { createIndicatorIdHasher } from './idHashers';
import { parseIdLikeStrings } from './idParsers';
import {
  csvLabelsToGeographicLevels,
  geographicLevelColumns,
} from './locationConstants';
import parseDataSetQueryConditions from './parseDataSetQueryConditions';
import parseTimePeriodCode from './parseTimePeriodCode';
import { indexPlaceholders, placeholders } from './queryUtils';

const DEBUG_DELIMITER = ' :: ';

export async function runDataSetQuery(
  dataSetId: string,
  query: DataSetQuery,
  {
    debug,
    page,
    pageSize,
  }: { debug?: boolean; page: number; pageSize: number },
): Promise<Omit<DataSetQueryResultsViewModel, '_links'>> {
  const dataSetDir = getDataSetDir(dataSetId);
  const state = new DataSetQueryState(dataSetDir);

  try {
    const { results, resultsMeta, total, queryMeta } = await runQuery<DataRow>(
      state,
      { ...query, page, pageSize },
      {
        debug,
      },
    );

    if (results.length === 0) {
      state.prependWarning('facets', {
        message:
          'No results matched the facet criteria. You may need to refine your query.',
        code: 'results.empty',
      });
    }

    const filterCols = queryMeta.filterCols.sort();
    const indicators = orderBy(
      queryMeta.indicators,
      (indicator) => indicator.name,
    );
    const geographicLevels = [...queryMeta.geographicLevels].sort();

    return {
      paging: {
        page,
        pageSize,
        totalResults: Number(total),
        totalPages: Math.ceil(Number(total) / pageSize),
      },
      footnotes: [],
      warnings: state.getWarnings(),
      results: results.map((result) => {
        return {
          filters: filterCols.reduce<Dictionary<string>>((acc, col) => {
            const id = Number(result[filterIdColumn(col)]);

            if (Number.isNaN(id)) {
              return acc;
            }

            const hashedId = state.filterIdHasher.encode(id);

            if (debug && resultsMeta) {
              const filter = resultsMeta.filters[id];

              acc[col] = filter
                ? hashedId + DEBUG_DELIMITER + filter.label
                : hashedId;
            } else {
              acc[col] = hashedId;
            }

            return acc;
          }, {}),
          timePeriod: {
            code: parseTimePeriodCode(result.time_identifier),
            year: Number(result.time_period),
          },
          geographicLevel: csvLabelsToGeographicLevels[result.geographic_level],
          locations: geographicLevels.reduce<Dictionary<string>>(
            (acc, level) => {
              const id = Number(result[locationIdColumn(level)]);

              if (Number.isNaN(id)) {
                return acc;
              }

              const location = resultsMeta.locations[id];
              const hashedId = state.locationIdHasher.encode(id);

              acc[level] =
                debug && location
                  ? [hashedId, location.name, location.code].join(
                      DEBUG_DELIMITER,
                    )
                  : hashedId;

              return acc;
            },
            {},
          ),
          values: indicators.reduce<Dictionary<string>>((acc, indicator) => {
            acc[indicator.name] = result[indicator.name].toString();
            return acc;
          }, {}),
        };
      }),
    };
  } finally {
    state.db.close();
  }
}

interface CsvReturn {
  csv: string;
  paging: PagingViewModel;
}

export async function runDataSetQueryToCsv(
  dataSetId: string,
  query: DataSetQuery,
  { page, pageSize }: { page: number; pageSize: number },
): Promise<CsvReturn> {
  const dataSetDir = getDataSetDir(dataSetId);
  const state = new DataSetQueryState(dataSetDir);

  try {
    const { results, resultsMeta, queryMeta, total } = await runQuery<DataRow>(
      state,
      { ...query, page, pageSize },
      { formatCsv: true },
    );

    const rows = results.map((result) => {
      const row: DataRow = {
        time_period: result.time_period,
        time_identifier: result.time_identifier,
        geographic_level: result.geographic_level,
      };

      queryMeta.geographicLevels.forEach((level) => {
        const locationId = Number(result[locationIdColumn(level)]);
        const location = resultsMeta.locations[locationId];

        const levelCols = geographicLevelColumns[level];

        if (location) {
          row[levelCols.code] = location.code;
          row[levelCols.name] = location.name;
        }
      });

      queryMeta.filterCols.forEach((filterCol) => {
        const filterId = Number(result[filterIdColumn(filterCol)]);
        const filter = resultsMeta.filters[filterId];

        if (filter) {
          row[filterCol] = filter.label;
        }
      });

      queryMeta.indicators.forEach((indicator) => {
        row[indicator.name] = result[indicator.name];
      });

      return row;
    });

    return {
      csv: Papa.unparse(rows),
      paging: {
        page,
        pageSize,
        totalResults: total,
        totalPages: Math.ceil(total / pageSize),
      },
    };
  } finally {
    state.db.close();
  }
}

type FilterResultMeta = Pick<FilterRow, 'id' | 'label' | 'group_name'>;
type LocationResultMeta = Pick<LocationRow, 'id' | 'code' | 'name'>;

interface DataSetResultsMeta {
  filters: Record<number, FilterResultMeta>;
  locations: Record<number, LocationResultMeta>;
}

interface RunQueryOptions {
  debug?: boolean;
  formatCsv?: boolean;
}

interface RunQueryReturn<TRow extends DataRow = DataRow> {
  results: TRow[];
  resultsMeta: DataSetResultsMeta;
  total: number;
  queryMeta: DataSetQueryMeta;
}

async function runQuery<TRow extends DataRow = DataRow>(
  state: DataSetQueryState,
  query: DataSetQuery & { page: number; pageSize: number },
  options: RunQueryOptions = {},
): Promise<RunQueryReturn<TRow>> {
  const { db, tableFile } = state;
  const { debug, formatCsv } = options;
  const { page, pageSize } = query;

  const indicatorIds = parseIdLikeStrings(
    query.indicators ?? [],
    createIndicatorIdHasher(state.dataSetDir),
  );

  const [locationCols, geographicLevels, filterCols, indicators] =
    await Promise.all([
      getLocationColumns(state),
      getGeographicLevels(state),
      getFilterColumns(state),
      getIndicators(state, indicatorIds),
    ]);

  const queryMeta: DataSetQueryMeta = {
    geographicLevels,
    locationCols,
    filterCols,
    indicators,
  };

  const where = await parseDataSetQueryConditions(
    state,
    query,
    geographicLevels,
  );

  const totalQuery = `
      SELECT CAST(count(*) AS INTEGER) AS total
      FROM '${tableFile('data')}' AS data
      ${where.fragment ? `WHERE ${where.fragment}` : ''}
  `;

  const locationIdCols = [...geographicLevels].map(
    (level) => `data."${locationIdColumn(level)}"`,
  );

  // We essentially split this query into two sub-queries:
  // 1. The main query which is offset paginated and gathers the result ids (i.e. a 'deferred' join)
  // 2. A query to actually get the data using the ids
  // This is more efficient than a single query as offset pagination is really expensive,
  // especially if working with lots of columns. Once we know the rows we're interested in (via
  // their ids), we can select on all the columns we want (without the performance penalty).
  const resultsQuery = `
      WITH
          data_ids AS (
            SELECT data.id
            FROM '${tableFile('data')}' AS data
            JOIN '${tableFile('time_periods')}' AS time_periods
                ON (time_periods.year, time_periods.identifier)
                = (data.time_period, data.time_identifier)
            ${where.fragment ? `WHERE ${where.fragment}` : ''}
            ORDER BY ${getOrderings(query, filterCols, geographicLevels)}
            LIMIT ?
            OFFSET ?
          )
      SELECT data.time_period,
             data.time_identifier,
             data.geographic_level,
             ${[
               ...locationIdCols,
               ...filterCols.map((col) => `data."${filterIdColumn(col)}"`),
               ...indicators.map((i) => `data."${i.name}"`),
             ]}
      FROM '${tableFile('data')}' AS data
      JOIN data_ids ON data_ids.id = data.id
      JOIN '${tableFile('time_periods')}' AS time_periods
        ON (time_periods.year, time_periods.identifier) = (data.time_period, data.time_identifier)
      ORDER BY ${getOrderings(query, filterCols, geographicLevels)}
  `;

  // Tried cursor/keyset pagination, but it's probably too difficult to implement.
  // Might need to revisit this in the future if performance is an issue.
  // - Ordering is a real headache as we'd need to perform struct comparisons across
  //   non-indicator columns. This could potentially be even worse in terms of performance!
  // - We would most likely need to create new columns for row ids and row structs (of
  //   non-indicator columns). This would blow up the size of the Parquet file.
  // - The WHERE clause we would need to generate would be actually horrendous, especially
  //   if we want to allow users to specify custom sorting.
  // - The cursor token we'd generate for clients could potentially become big as
  //   it'd rely on a bunch of columns being combined.
  // - If we scale this horizontally, offset pagination is probably fine even if it's
  //   not as fast on paper. Cursor pagination may be a premature optimisation.
  const pageOffset = (page - 1) * pageSize;

  // Bail before executing any queries if there
  // have been any errors that have accumulated.
  if (state.hasErrors()) {
    throw new ValidationError({
      errors: state.getErrors(),
    });
  }

  const [{ total }, results] = await Promise.all([
    db.first<{ total: number }>(totalQuery, where.params),
    db.all<TRow>(resultsQuery, [...where.params, pageSize, pageOffset], {
      debug: true,
    }),
  ]);

  let resultsMeta: DataSetResultsMeta = {
    filters: {},
    locations: {},
  };

  if (debug || formatCsv) {
    resultsMeta = await getResultsMeta({
      state,
      results,
      geographicLevels,
      filterCols,
    });
  }

  return {
    results,
    resultsMeta,
    total,
    queryMeta,
  };
}

function getOrderings(
  query: DataSetQuery,
  filterCols: string[],
  geographicLevels: Set<GeographicLevel>,
): string[] {
  // Default to ordering by descending time periods
  if (!query.sort) {
    return ['time_periods.ordering DESC'];
  }

  if (!query.sort.length) {
    throw ValidationError.atPath('sort', arrayErrors.notEmpty);
  }

  // Remove quotes wrapping column name
  const allowedFilterCols = new Set([...filterCols]);

  const sorts: string[] = [];
  const invalidSorts = new Set<string>();

  query.sort.forEach((sort) => {
    const direction = sort.direction === 'Desc' ? 'DESC' : 'ASC';

    if (sort.name === 'TimePeriod') {
      sorts.push(`time_periods.ordering ${direction}`);
      return;
    }

    if (geographicLevels.has(sort.name as GeographicLevel)) {
      const level = sort.name as GeographicLevel;
      sorts.push(`data."${locationOrderColumn(level)}" ${direction}`);
      return;
    }

    if (allowedFilterCols.has(sort.name)) {
      sorts.push(`data."${filterOrderColumn(sort.name)}" ${direction}`);
      return;
    }

    invalidSorts.add(sort.name);
  });

  if (invalidSorts.size > 0) {
    throw ValidationError.atPath('sort', {
      message: 'Could not sort fields as they are not allowed.',
      code: 'sort.notAllowed',
      details: {
        items: [...invalidSorts],
        allowed: ['TimePeriod', ...allowedFilterCols, ...geographicLevels],
      },
    });
  }

  return uniq(sorts);
}

async function getLocationColumns({
  db,
  dataSetDir,
}: DataSetQueryState): Promise<string[]> {
  return (
    await db.all<{ level: GeographicLevel }>(
      `SELECT DISTINCT level FROM '${tableFile(dataSetDir, 'locations')}'`,
    )
  ).flatMap((row) => {
    const cols = geographicLevelColumns[row.level];
    return [cols.code, cols.name, ...(cols.other ?? [])];
  });
}

async function getGeographicLevels({
  db,
  dataSetDir,
}: DataSetQueryState): Promise<Set<GeographicLevel>> {
  const rows = await db.all<{ geographic_level: string }>(
    `SELECT DISTINCT geographic_level FROM '${tableFile(dataSetDir, 'data')}'`,
  );

  return new Set<GeographicLevel>(
    compact(
      rows.map((row) => csvLabelsToGeographicLevels[row.geographic_level]),
    ),
  );
}

async function getFilterColumns({
  db,
  dataSetDir,
}: DataSetQueryState): Promise<string[]> {
  return (
    await db.all<{ group_name: string }>(`
        SELECT DISTINCT group_name
        FROM '${tableFile(dataSetDir, 'filters')}';
    `)
  ).map((row) => row.group_name);
}

async function getIndicators(
  state: DataSetQueryState,
  indicatorIds: string[],
): Promise<IndicatorRow[]> {
  const { db, indicatorIdHasher, tableFile } = state;

  if (!indicatorIds.length) {
    throw ValidationError.atPath('indicators', arrayErrors.notEmpty);
  }

  const ids = compact(indicatorIds);

  if (!ids.length) {
    throw ValidationError.atPath('indicators', arrayErrors.noBlankStrings);
  }

  const idPlaceholders = indexPlaceholders(ids);

  const indicators = await db.all<IndicatorRow>(
    `SELECT *
     FROM '${tableFile('indicators')}'
     WHERE id::VARCHAR IN (${idPlaceholders}) 
        OR name IN (${idPlaceholders});`,
    ids,
  );

  if (indicators.length < ids.length) {
    const allowed = indicators.reduce<Set<string>>((acc, i) => {
      acc.add(indicatorIdHasher.encode(i.id));
      acc.add(i.name);

      return acc;
    }, new Set());

    throw ValidationError.atPath(
      'indicators',
      genericErrors.notFound({
        items: uniq(ids.filter((id) => !allowed.has(id))),
      }),
    );
  }

  return indicators;
}

async function getResultsMeta<TRow extends DataRow>({
  state,
  results,
  geographicLevels,
  filterCols,
}: {
  state: DataSetQueryState;
  results: TRow[];
  geographicLevels: Set<GeographicLevel>;
  filterCols: string[];
}): Promise<DataSetResultsMeta> {
  const ids = results.reduce(
    (acc, row) => {
      for (const geographicLevel of geographicLevels) {
        const locationId = row[locationIdColumn(geographicLevel)];

        if (typeof locationId === 'number') {
          acc.locationIds.add(locationId);
        }
      }

      for (const filterCol of filterCols) {
        const filterId = row[filterIdColumn(filterCol)];

        if (typeof filterId === 'number') {
          acc.filterIds.add(filterId);
        }
      }

      return acc;
    },
    {
      filterIds: new Set<number>(),
      locationIds: new Set<number>(),
    },
  );

  const { db, tableFile } = state;

  const filterIds = [...ids.filterIds];
  const locationIds = [...ids.locationIds];

  const [locationRows, filterRows] = await Promise.all([
    locationIds.length > 0
      ? db.all<LocationResultMeta>(
          `
            SELECT id, name, code 
            FROM '${tableFile('locations')}'
            WHERE id IN (${placeholders([...locationIds])})
          `,
          locationIds,
        )
      : Promise.resolve([]),
    filterIds.length > 0
      ? db.all<FilterResultMeta>(
          `
            SELECT id, label, group_name 
            FROM '${tableFile('filters')}'
            WHERE id IN (${placeholders([...filterIds])})
          `,
          filterIds,
        )
      : Promise.resolve([]),
  ]);

  return {
    locations: keyBy(locationRows, (row) => row.id),
    filters: keyBy(filterRows, (row) => row.id),
  };
}
