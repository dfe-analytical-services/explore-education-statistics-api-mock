import { compact, keyBy, mapValues, pickBy, uniq } from 'lodash';
import Papa from 'papaparse';
import { ValidationError } from '../errors';
import {
  DataSetQuery,
  DataSetResultsViewModel,
  PagingViewModel,
} from '../schema';
import { DataRow, Indicator } from '../types/dbSchemas';
import { arrayErrors, genericErrors } from '../validations/errors';
import { tableFile } from './dataSetPaths';
import DataSetQueryState from './DataSetQueryState';
import getDataSetDir from './getDataSetDir';
import { createIndicatorIdHasher } from './idHashers';
import {
  csvLabelsToGeographicLevels,
  geographicLevelColumns,
} from './locationConstants';
import parseDataSetQueryConditions from './parseDataSetQueryConditions';
import parseIdLikeStrings from './parseIdLikeStrings';
import parseTimePeriodCode from './parseTimePeriodCode';
import { indexPlaceholders } from './queryUtils';

interface DataRowWithLocation extends DataRow {
  location_id: number;
}

export async function runDataSetQuery(
  dataSetId: string,
  query: DataSetQuery,
  { debug, page, pageSize }: { debug?: boolean; page: number; pageSize: number }
): Promise<Omit<DataSetResultsViewModel, '_links'>> {
  const dataSetDir = getDataSetDir(dataSetId);
  const state = new DataSetQueryState(dataSetDir);

  try {
    const { results, total, filterCols, indicators } =
      await runQuery<DataRowWithLocation>(
        state,
        { ...query, page, pageSize },
        {
          debug,
        }
      );

    const unquotedFilterCols = filterCols.map((col) => col.slice(1, -1));
    const indicatorsById = keyBy(indicators, (indicator) =>
      indicator.name.toString()
    );

    if (results.length === 0) {
      state.prependWarning('facets', {
        message:
          'No results matched the facet criteria. You may need to refine your query.',
        code: 'results.empty',
      });
    }

    return {
      paging: {
        page,
        pageSize,
        totalResults: total,
        totalPages: pageSize > 0 ? Math.ceil(total / pageSize) : pageSize,
      },
      footnotes: [],
      warnings: state.hasWarnings() ? state.getWarnings() : undefined,
      results: results.map((result) => {
        return {
          filters: unquotedFilterCols.reduce<Dictionary<string>>((acc, col) => {
            acc[col] = debug
              ? result[col].toString()
              : state.filterIdHasher.encode(Number(result[col]));

            return acc;
          }, {}),
          timePeriod: {
            code: parseTimePeriodCode(result.time_identifier),
            year: Number(result.time_period),
          },
          geographicLevel: csvLabelsToGeographicLevels[result.geographic_level],
          locationId: debug
            ? result.location_id.toString()
            : state.locationIdHasher.encode(result.location_id),
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
  paging: PagingViewModel;
}

export async function runDataSetQueryToCsv(
  dataSetId: string,
  query: DataSetQuery,
  { page, pageSize }: { page: number; pageSize: number }
): Promise<CsvReturn> {
  const dataSetDir = getDataSetDir(dataSetId);
  const state = new DataSetQueryState(dataSetDir);

  try {
    const { results, total } = await runQuery<DataRow>(
      state,
      { ...query, page, pageSize },
      { formatCsv: true }
    );

    return {
      csv: Papa.unparse(results),
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

interface RunQueryOptions {
  debug?: boolean;
  formatCsv?: boolean;
}

interface RunQueryReturn<TRow extends DataRow = DataRow> {
  results: TRow[];
  total: number;
  locationCols: string[];
  filterCols: string[];
  indicators: Indicator[];
}

async function runQuery<TRow extends DataRow>(
  state: DataSetQueryState,
  query: DataSetQuery & { page: number; pageSize: number },
  options: RunQueryOptions = {}
): Promise<RunQueryReturn<TRow>> {
  const { db, tableFile } = state;
  const { debug, formatCsv } = options;
  const { page, pageSize } = query;

  const indicatorIds = parseIdLikeStrings(
    query.indicators ?? [],
    createIndicatorIdHasher(state.dataSetDir)
  );

  const [locationCols, filterCols, indicators] = await Promise.all([
    getLocationColumns(state),
    getFilterColumns(state),
    getIndicators(state, indicatorIds),
  ]);

  const where = await parseDataSetQueryConditions(state, query, locationCols);

  const totalQuery = `
      SELECT count(*) AS total
      FROM '${tableFile('data')}' AS data
      JOIN '${tableFile('locations')}' AS locations
        ON (${locationCols.map((col) => `locations.${col}`)})
          = (${locationCols.map((col) => `data.${col}`)})
      ${where.fragment ? `WHERE ${where.fragment}` : ''}
  `;

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
                 ${[
                   ...(formatCsv
                     ? locationCols.map((col) => `data.${col}`)
                     : [
                         'data.geographic_level',
                         'locations.id AS location_id',
                       ]),
                   ...filterCols.map((col) => `data.${col} as ${col}`),
                   ...indicators.map((i) => `data."${i.name}"`),
                 ]}
          FROM '${tableFile('data')}' AS data
          JOIN '${tableFile('locations')}' AS locations
            ON (${locationCols.map((col) => `locations.${col}`)})
                = (${locationCols.map((col) => `data.${col}`)})
          JOIN '${tableFile('time_periods')}' AS time_periods
            ON (time_periods.year, time_periods.identifier) 
                = (data.time_period, data.time_identifier)
          ${where.fragment ? `WHERE ${where.fragment}` : ''}
          ORDER BY ${getOrderings(query, state, locationCols, filterCols)}
          LIMIT ?
          OFFSET ? 
      )
      SELECT data.* REPLACE(${filterCols.map((col) => {
        if (formatCsv) {
          return `${col}.label AS ${col}`;
        }

        return `${
          debug ? `concat(${col}.id, '::', ${col}.label)` : `${col}.id`
        } AS ${col}`;
      })})
      FROM data ${filterCols
        .map(
          (filter) =>
            `JOIN '${tableFile('filters')}' AS ${filter} 
                ON ${filter}.label = data.${filter} 
                AND ${filter}.group_name = '${filter.slice(1, -1)}'`
        )
        .join('\n')}
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
    db.all<TRow>(resultsQuery, [...where.params, pageSize, pageOffset]),
  ]);

  return {
    results,
    total,
    locationCols,
    filterCols,
    indicators,
  };
}

function getOrderings(
  query: DataSetQuery,
  state: DataSetQueryState,
  locationCols: string[],
  filterCols: string[]
): string[] {
  // Default to ordering by descending time periods
  if (!query.sort) {
    return ['time_periods.ordering DESC'];
  }

  if (!query.sort.length) {
    throw ValidationError.atPath('sort', arrayErrors.notEmpty);
  }

  // Remove quotes wrapping column name
  const allowedFilterCols = new Set(filterCols.map((col) => col.slice(1, -1)));
  const allowedGeographicLevelCols = pickBy(geographicLevelColumns, (col) =>
    locationCols.includes(col.code)
  );

  const sorts: string[] = [];
  const invalidSorts = new Set<string>();

  query.sort.forEach((sort) => {
    const direction = sort.order === 'Desc' ? 'DESC' : 'ASC';

    if (sort.name === 'TimePeriod') {
      sorts.push(`time_periods.ordering ${direction}`);
      return;
    }

    if (allowedGeographicLevelCols[sort.name]) {
      sorts.push(`${allowedGeographicLevelCols[sort.name].name} ${direction}`);
      return;
    }

    if (allowedFilterCols.has(sort.name)) {
      sorts.push(`${sort.name} ${direction}`);
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
        allowed: [
          'TimePeriod',
          ...allowedFilterCols,
          ...Object.keys(allowedGeographicLevelCols),
        ],
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
    await db.all<{ column_name: string }>(
      `DESCRIBE SELECT * EXCLUDE id FROM '${tableFile(
        dataSetDir,
        'locations'
      )}';`
    )
  ).map((row) => row.column_name);
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
): Promise<Indicator[]> {
  const { db, indicatorIdHasher, tableFile } = state;

  if (!indicatorIds.length) {
    throw ValidationError.atPath('indicators', arrayErrors.notEmpty);
  }

  const ids = compact(indicatorIds);

  if (!ids.length) {
    throw ValidationError.atPath('indicators', arrayErrors.noBlankStrings);
  }

  const idPlaceholders = indexPlaceholders(ids);

  const indicators = await db.all<Indicator>(
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
