import Hashids from 'hashids';
import { compact, keyBy, mapValues, pickBy } from 'lodash';
import Papa from 'papaparse';
import {
  DataSetQuery,
  DataSetResultsViewModel,
  PagingViewModel,
} from '../schema';
import { DataRow, Indicator } from '../types/dbSchemas';
import Database from './Database';
import { tableFile } from './dataSetPaths';
import getDataSetDir from './getDataSetDir';
import {
  createFilterIdHasher,
  createIndicatorIdHasher,
  createLocationIdHasher,
} from './idHashers';
import {
  csvLabelsToGeographicLevels,
  geographicLevelColumns,
} from './locationConstants';
import parseDataSetQueryConditions from './parseDataSetQueryConditions';
import { parseIdLikeStrings } from './parseIdLikeStrings';
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

  const db = new Database();
  const filterIdHasher = createFilterIdHasher(dataSetDir);
  const locationIdHasher = createLocationIdHasher(dataSetDir);

  try {
    const { results, total, filterCols, indicators } =
      await runQuery<DataRowWithLocation>(
        db,
        dataSetDir,
        { ...query, page, pageSize },
        {
          debug,
          filterIdHasher,
        }
      );

    const unquotedFilterCols = filterCols.map((col) => col.slice(1, -1));
    const indicatorsById = keyBy(indicators, (indicator) =>
      indicator.name.toString()
    );

    return {
      paging: {
        page,
        pageSize,
        totalResults: total,
        totalPages: pageSize > 0 ? Math.ceil(total / pageSize) : pageSize,
      },
      footnotes: [],
      warnings:
        results.length === 0
          ? [
              'No results matched the query criteria. You may need to refine your query.',
            ]
          : undefined,
      results: results.map((result) => {
        return {
          filters: unquotedFilterCols.reduce<Dictionary<string>>((acc, col) => {
            acc[col] = debug
              ? result[col].toString()
              : filterIdHasher.encode(Number(result[col]));

            return acc;
          }, {}),
          timePeriod: {
            code: parseTimePeriodCode(result.time_identifier),
            year: Number(result.time_period),
          },
          geographicLevel: csvLabelsToGeographicLevels[result.geographic_level],
          locationId: debug
            ? result.location_id.toString()
            : locationIdHasher.encode(result.location_id),
          values: mapValues(indicatorsById, (indicator) =>
            result[indicator.name].toString()
          ),
        };
      }),
    };
  } finally {
    db.close();
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
  const db = new Database();

  try {
    const { results, total } = await runQuery<DataRow>(db, dataSetDir, {
      ...query,
      page,
      pageSize,
    });

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
    db.close();
  }
}

interface RunQueryOptions {
  debug?: boolean;
  formatCsv?: boolean;
  filterIdHasher?: Hashids;
  locationIdHasher?: Hashids;
  indicatorIdHasher?: Hashids;
}

interface RunQueryReturn<TRow extends DataRow = DataRow> {
  results: TRow[];
  total: number;
  locationCols: string[];
  filterCols: string[];
  indicators: Indicator[];
}

async function runQuery<TRow extends DataRow>(
  db: Database,
  dataSetDir: string,
  query: DataSetQuery & { page: number; pageSize: number },
  options: RunQueryOptions = {}
): Promise<RunQueryReturn<TRow>> {
  const {
    debug,
    formatCsv,
    filterIdHasher = createFilterIdHasher(dataSetDir),
    locationIdHasher = createLocationIdHasher(dataSetDir),
    indicatorIdHasher = createIndicatorIdHasher(dataSetDir),
  } = options;

  const { page, pageSize } = query;

  const indicatorIds = parseIdLikeStrings(
    query.indicators ?? [],
    indicatorIdHasher
  );

  const [locationCols, filterCols, indicators] = await Promise.all([
    getLocationColumns(db, dataSetDir),
    getFilterColumns(db, dataSetDir),
    getIndicators(db, dataSetDir, indicatorIds),
  ]);

  const where = await parseDataSetQueryConditions(
    db,
    dataSetDir,
    query,
    locationCols,
    filterIdHasher,
    locationIdHasher
  );

  console.log(where);

  const totalQuery = `
      SELECT count(*) AS total
      FROM '${tableFile(dataSetDir, 'data')}' AS data
          JOIN '${tableFile(dataSetDir, 'locations')}' AS locations
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
                 data.geographic_level,
                 ${compact([
                   formatCsv ? '' : 'locations.id AS location_id',
                   ...filterCols.map((col) => `data.${col} as ${col}`),
                   ...indicators.map((i) => `data."${i.name}"`),
                 ])}
          FROM '${tableFile(dataSetDir, 'data')}' AS data
          JOIN '${tableFile(dataSetDir, 'locations')}' AS locations
            ON (${locationCols.map((col) => `locations.${col}`)})
                = (${locationCols.map((col) => `data.${col}`)})
          JOIN '${tableFile(dataSetDir, 'time_periods')}' AS time_periods
            ON (time_periods.year, time_periods.identifier) 
                = (data.time_period, data.time_identifier)
          ${where.fragment ? `WHERE ${where.fragment}` : ''}
          ORDER BY ${getOrderings(query, locationCols, filterCols)}
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
            `JOIN '${tableFile(dataSetDir, 'filters')}' AS ${filter} 
                ON ${filter}.label = data.${filter} 
                AND ${filter}.group_name = '${filter.slice(1, -1)}'`
        )
        .join('\n')}
  `;

  console.log(resultsQuery);

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
  locationCols: string[],
  filterCols: string[]
): string[] {
  const orderings: string[] = [];

  if (query.sort) {
    // Remove quotes wrapping column name
    const allowedFilterCols = filterCols.map((col) => col.slice(1, -1));
    const allowedGeographicLevelCols = pickBy(geographicLevelColumns, (col) =>
      locationCols.includes(col.code)
    );

    query.sort.forEach((sort) => {
      const direction = sort.order === 'Desc' ? 'DESC' : 'ASC';

      if (sort.name === 'TimePeriod') {
        orderings.push(`time_periods.ordering ${direction}`);
        return;
      }

      if (allowedGeographicLevelCols[sort.name]) {
        orderings.push(
          `${allowedGeographicLevelCols[sort.name].name} ${direction}`
        );
        return;
      }

      if (allowedFilterCols.includes(sort.name)) {
        orderings.push(`${sort.name} ${direction}`);
        return;
      }

      // TODO: Add error handling for invalid fields that cannot be ordered
    });
  }

  // Default to ordering by descending time periods
  if (!orderings.length) {
    return ['time_periods.ordering DESC'];
  }

  return orderings;
}

async function getLocationColumns(
  db: Database,
  dataSetDir: string
): Promise<string[]> {
  return (
    await db.all<{ column_name: string }>(
      `DESCRIBE SELECT * EXCLUDE id FROM '${tableFile(
        dataSetDir,
        'locations'
      )}';`
    )
  ).map((row) => row.column_name);
}

async function getFilterColumns(
  db: Database,
  dataSetDir: string
): Promise<string[]> {
  return (
    await db.all<{ group_name: string }>(`
        SELECT DISTINCT group_name
        FROM '${tableFile(dataSetDir, 'filters')}';
    `)
  ).map((row) => `"${row.group_name}"`);
}

async function getIndicators(
  db: Database,
  dataSetDir: string,
  indicatorIds: string[]
): Promise<Indicator[]> {
  if (!indicatorIds.length) {
    return [];
  }

  const idPlaceholders = indexPlaceholders(indicatorIds);

  return await db.all<Indicator>(
    `SELECT *
     FROM '${tableFile(dataSetDir, 'indicators')}'
     WHERE id::VARCHAR IN (${idPlaceholders}) 
        OR name IN (${idPlaceholders});`,
    indicatorIds
  );
}
