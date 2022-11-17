import Hashids from 'hashids';
import { compact, groupBy, keyBy, mapValues } from 'lodash';
import Papa from 'papaparse';
import {
  DataSetQuery,
  DataSetResultsViewModel,
  PagingViewModel,
} from '../schema';
import { DataRow, Filter, Indicator } from '../types/dbSchemas';
import Database from './Database';
import getDataSetDir from './getDataSetDir';
import { tableFile } from './dataSetPaths';
import {
  createFilterIdHasher,
  createIndicatorIdHasher,
  createLocationIdHasher,
} from './idHashers';
import {
  csvLabelsToGeographicLevels,
  geographicLevelColumns,
} from './locationConstants';
import parseTimePeriodCode from './parseTimePeriodCode';
import { timePeriodCodeIdentifiers } from './timePeriodConstants';

type FilterItem = Pick<Filter, 'id' | 'label' | 'group_name'>;

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
  const locationIdHasher = createFilterIdHasher(dataSetDir);

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
  filterItems: FilterItem[];
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
  const filterItemIds = parseIds(query.filterItems ?? [], filterIdHasher);
  const indicatorIds = parseIdStrings(
    query.indicators ?? [],
    indicatorIdHasher
  );

  const [locationCols, filterCols, indicators, filterItems] = await Promise.all(
    [
      getLocationColumns(db, dataSetDir),
      getFilterColumns(db, dataSetDir),
      getIndicators(db, dataSetDir, indicatorIds),
      getFilterItems(db, dataSetDir, filterItemIds),
    ]
  );

  const locationIds = await getLocationIds(
    db,
    dataSetDir,
    query,
    locationCols,
    locationIdHasher
  );

  const groupedFilterItems = groupBy(
    filterItems,
    (filter) => filter.group_name
  );

  const whereCondition = compact([
    getTimePeriodCondition(query),
    locationIds.length > 0
      ? `locations.id IN (${placeholders(locationIds)})`
      : '',
    getFiltersCondition(dataSetDir, groupedFilterItems),
  ]).join(' AND ');

  const totalQuery = `
      SELECT count(*) AS total
      FROM '${tableFile(dataSetDir, 'data')}' AS data
      JOIN '${tableFile(dataSetDir, 'locations')}' AS locations
        ON (${locationCols.map((col) => `locations.${col}`)})
            = (${locationCols.map((col) => `data.${col}`)})
      ${whereCondition ? `WHERE ${whereCondition}` : ''}
  `;

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
          ${whereCondition ? `WHERE ${whereCondition}` : ''}
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
      FROM data
      ${filterCols
        .map(
          (filter) =>
            `JOIN '${tableFile(dataSetDir, 'filters')}' AS ${filter} 
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

  const baseParams = [
    ...getTimePeriodParams(query),
    ...locationIds,
    ...Object.values(groupedFilterItems).flatMap((items) =>
      items.map((item) => item.label)
    ),
  ];

  const [{ total }, results] = await Promise.all([
    db.first<{ total: number }>(totalQuery, baseParams),
    db.all<TRow>(resultsQuery, [...baseParams, pageSize, pageOffset]),
  ]);

  return {
    results,
    total,
    locationCols,
    filterCols,
    indicators,
    filterItems,
  };
}

function getTimePeriodCondition({ timePeriod }: DataSetQuery): string {
  const conditions = [];

  // TODO: Implement start/end codes properly

  if (timePeriod?.startCode) {
    conditions.push('data.time_identifier = ?');
  }

  if (timePeriod?.startYear) {
    conditions.push('data.time_period >= ?');
  }

  if (timePeriod?.endYear) {
    conditions.push('data.time_period <= ?');
  }

  return conditions.join(' AND ');
}

function getTimePeriodParams({
  timePeriod,
}: DataSetQuery): (string | number)[] {
  const params = [];

  // TODO: Implement start/end codes properly

  if (timePeriod?.startCode) {
    params.push(timePeriodCodeIdentifiers[timePeriod.startCode]);
  }

  if (timePeriod?.startYear) {
    params.push(timePeriod.startYear);
  }

  if (timePeriod?.endYear) {
    params.push(timePeriod.endYear);
  }

  return params;
}

function getFiltersCondition(
  dataSetDir: string,
  groupedFilterItems: Dictionary<FilterItem[]>
): string {
  if (!Object.keys(groupedFilterItems).length) {
    return '';
  }

  return Object.entries(groupedFilterItems)
    .map(
      ([groupName, filterItems]) =>
        `data."${groupName}" IN (${placeholders(filterItems)})`
    )
    .join(' AND ');
}

async function getLocationIds(
  db: Database,
  dataSetDir: string,
  query: DataSetQuery,
  locationCols: string[],
  locationIdHasher: Hashids
): Promise<number[]> {
  const codeCols = Object.values(geographicLevelColumns)
    .map((col) => col.code)
    .filter((col) => locationCols.includes(col));

  const ids = parseIdStrings(query.locations ?? [], locationIdHasher);
  const idPlaceholders = indexPlaceholders(ids);

  if (!ids.length) {
    return [];
  }

  const locations = await db.all<{ id: number }>(
    `
      SELECT id
      FROM '${tableFile(dataSetDir, 'locations')}'
      WHERE id::VARCHAR IN (${idPlaceholders})
        OR ${codeCols
          .map((col) => `${col} IN (${idPlaceholders})`)
          .join(' OR ')}`,
    ids
  );

  return locations.map((location) => location.id);
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

async function getFilterItems(
  db: Database,
  dataSetDir: string,
  filterItemIds: number[]
): Promise<FilterItem[]> {
  if (!filterItemIds.length) {
    return [];
  }

  return await db.all<Filter>(
    `SELECT *
        FROM '${tableFile(dataSetDir, 'filters')}'
        WHERE id IN (${placeholders(filterItemIds)});
    `,
    filterItemIds
  );
}

function parseIds(ids: string[], idHasher: Hashids): number[] {
  return compact(
    ids.map((id) => {
      try {
        return idHasher.decode(id)[0] as number;
      } catch (err) {
        return Number.NaN;
      }
    })
  );
}

function parseIdStrings(ids: string[], idHasher: Hashids): string[] {
  return compact(
    ids.map((id) => {
      try {
        return idHasher.decode(id)[0].toString();
      } catch (err) {
        // If the id is NaN, then allow this as it could be a
        // code or other identifier that can be used instead.
        // Plain numbers shouldn't be accepted to avoid
        return Number.isNaN(Number(id)) ? id : '';
      }
    })
  );
}

function placeholders(value: unknown[]): string[] {
  return value.map(() => '?');
}

function indexPlaceholders(value: unknown[]): string[] {
  return value.map((_, index) => `$${index + 1}`);
}
