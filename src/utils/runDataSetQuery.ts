import { compact, keyBy, mapValues, snakeCase, uniq } from 'lodash';
import NodeCache from 'node-cache';
import Papa from 'papaparse';
import { ValidationError } from '../errors';
import {
  DataSetQuery,
  DataSetResultsViewModel,
  GeographicLevel,
  PagingCursorViewModel,
} from '../schema';
import { DataRow, IndicatorRow } from '../types/dbSchemas';
import { arrayErrors, genericErrors } from '../validations/errors';
import { tableFile } from './dataSetPaths';
import { DataSetQueryMeta } from './DataSetQueryMeta';
import DataSetQueryState from './DataSetQueryState';
import getDataSetDir from './getDataSetDir';
import { createIndicatorIdHasher, IdHasher } from './idHashers';
import { parseIdLikeStrings } from './idParsers';
import {
  csvLabelsToGeographicLevels,
  geographicLevelColumns,
} from './locationConstants';
import parseDataSetQueryConditions, {
  QueryFragmentParams,
} from './parseDataSetQueryConditions';
import parseTimePeriodCode from './parseTimePeriodCode';
import { indexPlaceholders } from './queryUtils';

const DEBUG_DELIMITER = ' :: ';

const cursorCache = new NodeCache();

export async function runDataSetQuery(
  dataSetId: string,
  query: DataSetQuery,
  {
    debug,
    cursor,
    pageSize,
  }: { debug?: boolean; cursor?: string; pageSize: number }
): Promise<Omit<DataSetResultsViewModel, '_links'>> {
  const dataSetDir = getDataSetDir(dataSetId);
  const state = new DataSetQueryState(dataSetDir);

  try {
    const { results, total, meta } = await runQuery<DataRow>(
      state,
      { ...query, cursor, pageSize },
      {
        debug,
      }
    );

    const unquotedFilterCols = meta.filterCols.map((col) => col.slice(1, -1));
    const indicatorsById = keyBy(meta.indicators, (indicator) =>
      indicator.name.toString()
    );

    if (results.length === 0) {
      state.prependWarning('facets', {
        message:
          'No results matched the facet criteria. You may need to refine your query.',
        code: 'results.empty',
      });
    }

    const hashedId = (id: number | string, hasher: IdHasher) => {
      if (debug) {
        const [idPart, label] = id.toString().split(DEBUG_DELIMITER, 2);
        return hasher.encode(Number(idPart)) + DEBUG_DELIMITER + label;
      }

      return hasher.encode(Number(id));
    };

    return {
      paging: {
        cursor,
        pageSize,
        totalResults: total,
        totalPages: pageSize > 0 ? Math.ceil(total / pageSize) : pageSize,
      },
      footnotes: [],
      warnings: state.getWarnings(),
      results: results.map((result) => {
        return {
          filters: unquotedFilterCols.reduce<Dictionary<string>>((acc, col) => {
            acc[col] = hashedId(result[col], state.filterIdHasher);
            return acc;
          }, {}),
          timePeriod: {
            code: parseTimePeriodCode(result.time_identifier),
            year: Number(result.time_period),
          },
          geographicLevel: csvLabelsToGeographicLevels[result.geographic_level],
          locations: [...meta.geographicLevels].reduce<Dictionary<string>>(
            (acc, level) => {
              const cols = geographicLevelColumns[level];
              const alias = geographicLevelAlias(level);
              const id = result[`${alias}_id`] as number;

              if (id) {
                const hashedId = state.locationIdHasher.encode(id);

                acc[level] = debug
                  ? [hashedId, result[cols.name], result[cols.code]].join(
                      DEBUG_DELIMITER
                    )
                  : hashedId;
              }

              return acc;
            },
            {}
          ),
          values: mapValues(indicatorsById, (indicator) =>
            result[indicator.name].toString()
          ),
        };
      }),
    };
  } finally {
    state.db.close();
  }
}

interface CsvReturn {
  csv: string;
  paging: PagingCursorViewModel;
}

export async function runDataSetQueryToCsv(
  dataSetId: string,
  query: DataSetQuery,
  { cursor, pageSize }: { cursor?: string; pageSize: number }
): Promise<CsvReturn> {
  const dataSetDir = getDataSetDir(dataSetId);
  const state = new DataSetQueryState(dataSetDir);

  try {
    const { results, total } = await runQuery<DataRow>(
      state,
      { ...query, cursor, pageSize },
      { formatCsv: true }
    );

    return {
      csv: Papa.unparse(results),
      paging: {
        cursor,
        // nextCursor,
        pageSize,
        totalResults: total,
        totalPages: Math.ceil(total / pageSize),
      },
    };
  } finally {
    state.db.close();
  }
}

interface RunQueryOptions {
  debug?: boolean;
  formatCsv?: boolean;
}

interface RunQueryReturn<TRow extends DataRow = DataRow> {
  results: TRow[];
  total: number;
  meta: DataSetQueryMeta;
}

async function runQuery<TRow extends DataRow = DataRow>(
  state: DataSetQueryState,
  query: DataSetQuery & { cursor?: string; pageSize: number },
  options: RunQueryOptions = {}
): Promise<RunQueryReturn<TRow>> {
  const { db, tableFile } = state;
  const { debug, formatCsv } = options;
  const { cursor, pageSize } = query;

  const indicatorIds = parseIdLikeStrings(
    query.indicators ?? [],
    createIndicatorIdHasher(state.dataSetDir)
  );

  const [locationCols, geographicLevels, filterCols, indicators] =
    await Promise.all([
      getLocationColumns(state),
      getGeographicLevels(state),
      getFilterColumns(state),
      getIndicators(state, indicatorIds),
    ]);

  const meta: DataSetQueryMeta = {
    geographicLevels,
    locationCols,
    filterCols,
    indicators,
  };

  const where = await parseDataSetQueryConditions(
    state,
    query,
    geographicLevels
  );

  const totalQuery = `
      SELECT count(*) AS total
      FROM '${tableFile('data')}' AS data
      ${where.fragment ? `WHERE ${where.fragment}` : ''}
  `;

  const locationIdCols = [...geographicLevels].map((level) => {
    const alias = geographicLevelAlias(level);
    return `${alias}.id AS ${alias}_id`;
  });

  const orderings = getOrderings(query, state, filterCols, geographicLevels);

  // We essentially split this query into two parts:
  // 1. The inner main query which is offset paginated
  // 2. The outer query which uses the results from the inner query
  //
  // We do this as we generate joins to the `filters` table on each filter column
  // for the filter item IDs. This can be very expensive if there are lots of them,
  // so by only doing this on the paginated results, we can limit this overhead
  // substantially and the query performs a lot better.
  //
  // We could alternatively fetch all the filter items into memory and pair these
  // up with the filter column values in our application level code. There might
  // some other approaches too, but I just settled on the current implementation
  // for now as it seemed to provide adequate response times (< 1s).
  const resultsQuery = `
      WITH data AS (
          SELECT data.time_period,
                 data.time_identifier,
                 data.geographic_level,
                 ${[
                   ...locationCols.map((col) => `data.${col}`),
                   ...locationIdCols,
                   ...filterCols.map((col) => `data.${col} AS ${col}`),
                   ...indicators.map((i) => `data."${i.name}"`),
                 ]}
          FROM '${tableFile('data')}' AS data
          JOIN '${tableFile('time_periods')}' AS time_periods
            ON (time_periods.year, time_periods.identifier) 
                = (data.time_period, data.time_identifier)
          ${getLocationJoins(state, geographicLevels)}
          ${getWhereClause(where, orderings, cursor)}
          ORDER BY ${orderings}
          LIMIT ?
      )
      SELECT data.* REPLACE(
        ${filterCols.map((col) => {
          if (formatCsv) {
            return `${col}.label AS ${col}`;
          }

          return `${
            debug
              ? `concat(${col}.id, '${DEBUG_DELIMITER}', ${col}.label)`
              : `${col}.id`
          } AS ${col}`;
        })}
      )
      FROM data ${filterCols
        .map(
          (filter) =>
            `JOIN '${tableFile('filters')}' AS ${filter} 
                ON ${filter}.label = data.${filter} 
                AND ${filter}.group_name = '${filter.slice(1, -1)}'`
        )
        .join('\n')}
  `;

  // Bail before executing any queries if there
  // have been any errors that have accumulated.
  if (state.hasErrors()) {
    throw new ValidationError({
      errors: state.getErrors(),
    });
  }

  const [{ total }, results] = await Promise.all([
    db.first<{ total: number }>(totalQuery, where.params),
    db.all<TRow>(resultsQuery, [...where.params, pageSize], {
      debug: true,
    }),
  ]);

  return {
    results,
    total,
    meta,
  };
}

function getLocationJoins(
  { tableFile }: DataSetQueryState,
  geographicLevels: Set<GeographicLevel>
): string {
  return [...geographicLevels]
    .map((level) => {
      const levelAlias = geographicLevelAlias(level);
      const levelCols = geographicLevelColumns[level];

      return `LEFT JOIN '${tableFile('locations')}' AS ${levelAlias} 
          ON ${levelAlias}.code = data.${levelCols.code} 
            AND ${levelAlias}.name = data.${levelCols.name}`;
    })
    .join('\n');
}

function getWhereClause(
  where: QueryFragmentParams,
  orderings: string[],
  cursor?: string
): string {
  if (!orderings.length || !cursor) {
    return where.fragment ? `WHERE ${where.fragment}` : '';
  }

  const cursorFragment = orderings
    .map((ordering) => {
      const index = ordering.lastIndexOf(' ');
      const column = ordering.slice(0, index);
      const direction = ordering.slice(index);

      return direction === 'ASC' ? `${column} > ?` : `${column} < ?`;
    })
    .join(' AND ');

  if (!cursorFragment && !where.fragment) {
    return '';
  }

  return `WHERE ${where.fragment} AND (${cursorFragment})`;
}

function getOrderings(
  query: DataSetQuery,
  state: DataSetQueryState,
  filterCols: string[],
  geographicLevels: Set<GeographicLevel>
): string[] {
  // Use default orderings if no sorts
  if (!query.sort) {
    return ['time_periods.ordering DESC', 'data.id ASC'];
  }

  if (!query.sort.length) {
    throw ValidationError.atPath('sort', arrayErrors.notEmpty);
  }

  // Remove quotes wrapping column name
  const allowedFilterCols = new Set(filterCols.map((col) => col.slice(1, -1)));

  const sorts: string[] = [];
  const invalidSorts = new Set<string>();

  query.sort.forEach((sort) => {
    const direction = sort.order === 'Desc' ? 'DESC' : 'ASC';

    if (sort.name === 'TimePeriod') {
      sorts.push(`time_periods.ordering ${direction}`);
      return;
    }

    if (geographicLevels.has(sort.name as GeographicLevel)) {
      const level = sort.name as GeographicLevel;
      sorts.push(`${geographicLevelColumns[level].name} ${direction}`);
      return;
    }

    if (allowedFilterCols.has(sort.name)) {
      sorts.push(`${sort.name} ${direction}`);
      return;
    }

    invalidSorts.add(sort.name);
  });

  // Always finish by ordering on ID to provide
  // consistent ordering in tie-break situations.
  sorts.push('data.id ASC');

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

  return sorts;
}

async function getLocationColumns({
  db,
  dataSetDir,
}: DataSetQueryState): Promise<string[]> {
  return (
    await db.all<{ level: GeographicLevel }>(
      `SELECT DISTINCT level FROM '${tableFile(dataSetDir, 'locations')}'`
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
    `SELECT DISTINCT geographic_level FROM '${tableFile(dataSetDir, 'data')}'`
  );

  return new Set<GeographicLevel>(
    compact(
      rows.map((row) => csvLabelsToGeographicLevels[row.geographic_level])
    )
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
  ).map((row) => `"${row.group_name}"`);
}

async function getIndicators(
  state: DataSetQueryState,
  indicatorIds: string[]
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
    ids
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
      })
    );
  }

  return indicators;
}

function geographicLevelAlias(level: GeographicLevel): string {
  return snakeCase(level);
}
