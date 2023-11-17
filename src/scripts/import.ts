import fs from 'fs-extra';
import { chunk, orderBy, partition } from 'lodash';
import path from 'path';
import { GeographicLevel } from '../schema';
import { DataRow } from '../types/dbSchemas';
import { MetaFileRow } from '../types/metaFile';
import Database from '../utils/Database';
import {
  columnsToGeographicLevel,
  geographicLevelColumns,
} from '../utils/locationConstants';
import parseCsv from '../utils/parseCsv';
import { placeholders } from '../utils/queryUtils';

process.env.NODE_ENV = 'development';

const projectRoot = path.resolve(__dirname, '../..');
const dataImportsDir = path.resolve(projectRoot, 'data-imports');
const dataOutputDir = path.resolve(projectRoot, 'src/data');

async function runImport() {
  const targetFiles = process.argv.slice(2);

  await fs.ensureDir(dataOutputDir);
  await fs.ensureDir(dataImportsDir);

  const files = (await fs.readdir(dataImportsDir)).filter((file) => {
    if (!file.endsWith('.csv')) {
      return false;
    }

    if (targetFiles.length > 0) {
      const trimmedFile = file.replace(/(\.meta\.csv|\.csv)$/, '');
      return targetFiles.some((target) => trimmedFile === target);
    }

    return true;
  });

  if (!files.length) {
    throw new Error(
      'No data files to import. Place some in the `data-imports` directory.',
    );
  }

  const [metaFiles, dataFiles] = partition(files, (file) =>
    file.endsWith('.meta.csv'),
  );

  for (const dataFile of dataFiles) {
    console.log(`Importing data file: ${dataFile}`);

    const fileBaseName = path.basename(dataFile, '.csv');

    const metaFile = metaFiles.find(
      (file) => file === `${fileBaseName}.meta.csv`,
    );

    if (!metaFile) {
      throw new Error(`Could not find meta file for: ${fileBaseName}`);
    }

    const outputDir = path.resolve(dataOutputDir, fileBaseName);

    // Clean output directory
    await fs.ensureDir(outputDir);
    await fs.emptyDir(outputDir);

    console.log(`=> Importing to directory: ${outputDir}`);

    const dataFilePath = path.resolve(dataImportsDir, dataFile);
    const metaFilePath = path.resolve(dataImportsDir, metaFile);

    const timeLabel = '=> Finished importing to directory';
    console.time(timeLabel);

    const db = new Database();

    const columns = (
      await db.all<{ column_name: string }>(
        `DESCRIBE SELECT * FROM '${dataFilePath}'`,
      )
    ).map((col) => col.column_name);

    await extractData(db, dataFilePath);
    await extractMeta(db, metaFilePath, columns);
    await extractNormalisedData(db, columns);

    await db.run(`EXPORT DATABASE '${outputDir}' (FORMAT PARQUET, CODEC ZSTD)`);

    await fs.copy(metaFilePath, `${outputDir}/meta.csv`);
    await db.run(
      `
      COPY (SELECT * FROM read_csv_auto('${dataFilePath}', ALL_VARCHAR=TRUE)) 
      TO '${outputDir}/data.csv.gz' WITH (HEADER, COMPRESSION gzip)
      `,
    );

    console.timeEnd(timeLabel);

    db.close();
  }
}

runImport().then(() => {
  console.log('DONE: All imports completed!');
});

async function extractData(db: Database, csvPath: string) {
  const timeLabel = '=> Imported data';
  console.time(timeLabel);

  try {
    await db.run(`CREATE SEQUENCE data_seq START 1`);
    await db.run(`CREATE TABLE data AS 
        SELECT nextval('data_seq') AS id, * FROM read_csv_auto('${csvPath}', ALL_VARCHAR=TRUE)`);

    console.timeEnd(timeLabel);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

async function extractMeta(
  db: Database,
  metaFilePath: string,
  columns: string[],
) {
  const metaFileRows = await parseCsv<MetaFileRow>(metaFilePath);

  try {
    await extractTimePeriods(db);
    await extractLocations(db, columns);
    await extractFilters(db, metaFileRows);
    await extractIndicators(db, metaFileRows);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

async function extractTimePeriods(db: Database): Promise<void> {
  const timeLabel = '=> Imported time periods meta';
  console.time(timeLabel);

  await db.run('CREATE SEQUENCE time_periods_seq START 1;');
  await db.run(
    `CREATE TABLE time_periods(
        id UINTEGER PRIMARY KEY DEFAULT nextval('time_periods_seq'),
        year VARCHAR NOT NULL,
        identifier VARCHAR,
        ordering UINTEGER NOT NULL
     );`,
  );

  const timePeriods = await db.all<{ year: string; identifier: string }>(`
    SELECT DISTINCT
          time_period AS year, 
          time_identifier AS identifier
    FROM data
    ORDER BY time_period ASC, time_identifier ASC;
  `);

  // TODO - implement actual ordering for time periods
  let index = 0;

  for (const timePeriod of timePeriods) {
    index += 1;

    await db.run(
      `
        INSERT INTO time_periods(year, identifier, ordering) VALUES (?, ?, ?)`,
      [timePeriod.year, timePeriod.identifier, index],
    );
  }

  console.timeEnd(timeLabel);
}

async function extractLocations(
  db: Database,
  columns: string[],
): Promise<void> {
  const timeLabel = '=> Imported locations meta';
  console.time(timeLabel);

  const locationCols = columns.filter(
    (column) => columnsToGeographicLevel[column],
  );
  const geographicLevels = locationCols.reduce(
    (acc, column) => acc.add(columnsToGeographicLevel[column]),
    new Set<GeographicLevel>(),
  );

  await db.run('CREATE SEQUENCE locations_seq START 1;');
  await db.run(
    `CREATE TABLE locations(
       id UINTEGER PRIMARY KEY DEFAULT nextval('locations_seq'),
       level VARCHAR NOT NULL,
       code VARCHAR DEFAULT '',
       name VARCHAR,
       ordering UINTEGER
     );`,
  );

  for (const geographicLevel of geographicLevels) {
    const cols = geographicLevelColumns[geographicLevel];

    const rows = await db.all<DataRow>(
      `
        SELECT ${cols.code}, ${cols.name} 
        FROM data
        WHERE ${cols.name} != ''
        GROUP BY ${cols.name}, ${cols.code}
        ORDER BY ${cols.name}, ${cols.code};
      `,
    );

    const chunks = chunk(rows, 100);

    let ordering = 0;

    for (const chunk of chunks) {
      const chunkParams: unknown[] = [];

      const inserts = chunk.map((row) => {
        ordering += 1;

        const params = [
          geographicLevel,
          row[cols.code],
          row[cols.name],
          ordering,
        ];

        chunkParams.push(...params);

        return `(${placeholders(params)})`;
      });

      await db.run(
        `
          INSERT INTO locations(level, code, name, ordering)
          VALUES ${inserts}
        `,
        chunkParams,
      );
    }
  }

  console.timeEnd('=> Imported locations meta');
}

async function extractFilters(
  db: Database,
  metaFileRows: MetaFileRow[],
): Promise<void> {
  const timeLabel = '=> Imported filters meta';
  console.time(timeLabel);

  await db.run('CREATE SEQUENCE filters_seq START 1;');
  await db.run(
    `CREATE TABLE filters(
       id UINTEGER PRIMARY KEY DEFAULT nextval('filters_seq'),
       label VARCHAR NOT NULL,
       group_label VARCHAR NOT NULL,
       group_name VARCHAR NOT NULL,
       group_hint VARCHAR,
       is_aggregate BOOLEAN DEFAULT FALSE,
       ordering UINTEGER
     );`,
  );

  const filters = orderBy(
    metaFileRows.filter((row) => row.col_type === 'Filter'),
    (row) => row.col_name,
  );

  for (const filter of filters) {
    const rows = await db.all<{ label: string }>(`
          SELECT DISTINCT ${filter.col_name} AS label
          FROM data
          ORDER BY ${filter.col_name}
      `);

    const chunks = chunk(rows, 100);

    let ordering = 0;

    for (const chunk of chunks) {
      const chunkParams: unknown[] = [];

      const inserts = chunk.map((row) => {
        ordering += 1;

        const params = [
          row.label,
          filter.label,
          filter.col_name,
          filter.filter_hint,
          filter.label === 'Total',
          ordering,
        ];

        chunkParams.push(...params);

        return `(${placeholders(params)})`;
      });

      await db.run(
        `
        INSERT INTO filters(label, group_label, group_name, group_hint, is_aggregate, ordering)
        VALUES ${inserts}
      `,
        chunkParams,
      );
    }
  }

  console.timeEnd(timeLabel);
}

async function extractIndicators(
  db: Database,
  metaFileRows: MetaFileRow[],
): Promise<void> {
  const timeLabel = '=> Imported indicators meta';
  console.time(timeLabel);

  await db.run('CREATE SEQUENCE indicators_seq START 1;');
  await db.run(
    `CREATE TABLE indicators(
       id UINTEGER PRIMARY KEY DEFAULT nextval('indicators_seq'),
       label VARCHAR NOT NULL,
       name VARCHAR NOT NULL,
       decimal_places INT,
       unit VARCHAR
     );`,
  );

  const indicators = orderBy(
    metaFileRows.filter((row) => row.col_type === 'Indicator'),
    (row) => row.label,
  );

  for (const indicator of indicators) {
    await db.run(
      `INSERT INTO indicators(label, name, decimal_places, unit) VALUES ($1, $2, $3, $4);`,
      [
        indicator.label,
        indicator.col_name,
        indicator.indicator_dp,
        indicator.indicator_unit,
      ],
    );
  }

  console.timeEnd(timeLabel);
}

async function extractNormalisedData(db: Database, columns: string[]) {
  const timeLabel = '=> Imported data_normalised';
  console.time(timeLabel);

  const locationColumns = columns.filter(
    (column) => columnsToGeographicLevel[column],
  );

  const filterColumns = (
    await db.all<{ group_name: string }>(
      'SELECT DISTINCT group_name FROM filters',
    )
  ).map((row) => row.group_name);

  const indicatorColumns = (
    await db.all<{ name: string }>('SELECT name FROM indicators')
  ).map((row) => row.name);

  const geographicLevels = locationColumns.reduce<GeographicLevel[]>(
    (acc, column) => {
      if (!acc.includes(columnsToGeographicLevel[column])) {
        acc.push(columnsToGeographicLevel[column]);
      }

      return acc;
    },
    [],
  );

  try {
    await db.run(
      `CREATE TABLE data_normalised(
      id UINTEGER PRIMARY KEY DEFAULT nextval('data_seq'),
      time_period VARCHAR,
      time_identifier VARCHAR,
      geographic_level VARCHAR,
      ${[
        ...geographicLevels.flatMap((level) => [
          `"${level} :: id" UINTEGER`,
          `"${level} :: ordering" UINTEGER`,
        ]),
        ...filterColumns.flatMap((column) => [
          `"${column} :: id" UINTEGER`,
          `"${column} :: ordering" UINTEGER`,
        ]),
        ...indicatorColumns.map((column) => `"${column}" VARCHAR`),
      ]}
    )`,
    );

    await db.run(
      `
        INSERT INTO data_normalised
        SELECT
            data.id,
            data.time_period,
            data.time_identifier,
            data.geographic_level,
            ${[
              ...geographicLevels.flatMap((level) => [
                `"${level}".id AS "${level} :: id"`,
                `"${level}".ordering AS "${level} :: ordering"`,
              ]),
              ...filterColumns.flatMap((column) => [
                `"${column}".id AS "${column} :: id"`,
                `"${column}".ordering AS "${column} :: ordering"`,
              ]),
              ...indicatorColumns.map((column) => `data."${column}"`),
            ]}
        FROM data
        ${[...geographicLevels]
          .map((level) => {
            const levelCols = geographicLevelColumns[level];

            return `LEFT JOIN locations AS "${level}"
              ON "${level}".level = '${level}'
              AND "${level}".code = data.${levelCols.code} 
              AND "${level}".name = data.${levelCols.name}`;
          })
          .join('\n')}
        ${[...filterColumns]
          .map(
            (column) =>
              `LEFT JOIN filters AS "${column}" 
              ON "${column}".label = data."${column}" 
              AND "${column}".group_name = '${column}'`,
          )
          .join('\n')}
        `,
    );

    console.timeEnd(timeLabel);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}
