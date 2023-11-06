import fs from 'fs-extra';
import { orderBy, partition, snakeCase } from 'lodash';
import path from 'path';
import { GeographicLevel } from '../schema';
import { MetaFileRow } from '../types/metaFile';
import {
  columnsToGeographicLevel,
  geographicLevelColumns,
} from '../utils/locationConstants';
import Database from '../utils/Database';
import parseCsv from '../utils/parseCsv';

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

    await finaliseData(db, columns);

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
    await db.run(`CREATE TABLE data_temp AS 
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
        year UINTEGER NOT NULL,
        identifier VARCHAR,
        ordering UINTEGER NOT NULL
     );`,
  );

  const timePeriods = await db.all<{ year: string; identifier: string }>(`
    SELECT DISTINCT
          time_period AS year, 
          time_identifier AS identifier
    FROM data_temp
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
       name VARCHAR
     );`,
  );

  for (const geographicLevel of geographicLevels) {
    const cols = geographicLevelColumns[geographicLevel];

    await db.run(
      `INSERT INTO locations(level, code, name)
        SELECT DISTINCT ? AS level, ${cols.code}, ${cols.name}
        FROM data_temp
        WHERE ${cols.name} != ''
        ORDER BY level, ${cols.code}, ${cols.name};`,
      [geographicLevel],
    );
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
       is_aggregate BOOLEAN DEFAULT FALSE
     );`,
  );

  const filters = orderBy(
    metaFileRows.filter((row) => row.col_type === 'Filter'),
    (row) => row.col_name,
  );

  for (const filter of filters) {
    await db.run(
      `INSERT INTO
        filters(label, group_label, group_name, group_hint, is_aggregate)
        SELECT label, $1, $2, $3, CASE WHEN label = 'Total' THEN TRUE END
        FROM (
            SELECT DISTINCT ${filter.col_name} AS label
            FROM data_temp
            ORDER BY ${filter.col_name}
        );`,
      [filter.label, filter.col_name, filter.filter_hint],
    );
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

async function finaliseData(db: Database, columns: string[]) {
  const [locationColumns, otherColumns] = partition(
    columns,
    (column) => columnsToGeographicLevel[column],
  );

  const [filterIndicatorColumns] = partition(
    otherColumns,
    (column) =>
      column !== 'time_period' &&
      column !== 'time_identifier' &&
      column !== 'geographic_level',
  );

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
    await db.run(`CREATE TABLE data(
      id UINTEGER PRIMARY KEY DEFAULT nextval('data_seq'),
      time_period VARCHAR,
      time_identifier VARCHAR,
      geographic_level VARCHAR,
      ${[
        ...geographicLevels.map((level) => `${snakeCase(level)}_id UINTEGER`),
        ...filterIndicatorColumns.map((column) => `"${column}" VARCHAR`),
      ]}
    )`);

    await db.run(
      `
        INSERT INTO data
        SELECT
            data_temp.id,
            data_temp.time_period,
            data_temp.time_identifier,
            data_temp.geographic_level,
            ${[
              ...geographicLevels.map(
                (level) => `${snakeCase(level)}.id AS ${snakeCase(level)}_id`,
              ),
              ...filterIndicatorColumns.map(
                (column) => `data_temp."${column}"`,
              ),
            ]}
        FROM data_temp
        ${[...geographicLevels]
          .map((level) => {
            const levelAlias = snakeCase(level);
            const levelCols = geographicLevelColumns[level];

            return `LEFT JOIN locations AS ${levelAlias}
              ON ${levelAlias}.level = '${level}'
              AND ${levelAlias}.code = data_temp.${levelCols.code} 
              AND ${levelAlias}.name = data_temp.${levelCols.name}`;
          })
          .join('\n')}`,
      [],
      {
        debug: true,
      },
    );

    // await db.run('DROP TABLE data_temp');
  } catch (err) {
    console.error(err);
    // process.exit(1);
  }
}
