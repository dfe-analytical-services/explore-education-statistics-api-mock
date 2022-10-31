import fs from 'fs-extra';
import { partition } from 'lodash';
import path from 'path';
import { MetaFileRow } from '../types/metaFile';
import { geographicLevelColumns } from '../utils/locationConstants';
import Database from '../utils/Database';
import parseCsv from '../utils/parseCsv';

const projectRoot = path.resolve(__dirname, '../..');
const dataImportsDir = path.resolve(projectRoot, 'data-imports');
const dataOutputDir = path.resolve(projectRoot, 'src/data');

async function runImport() {
  await fs.ensureDir(dataOutputDir);
  await fs.ensureDir(dataImportsDir);

  const files = (await fs.readdir(dataImportsDir)).filter((file) =>
    file.endsWith('.csv')
  );

  if (!files.length) {
    throw new Error(
      'No data files to import. Place some in the `data-imports` directory.'
    );
  }

  const [metaFiles, dataFiles] = partition(files, (file) =>
    file.endsWith('.meta.csv')
  );

  for (const dataFile of dataFiles) {
    console.log(`Importing data file: ${dataFile}`);

    const fileBaseName = path.basename(dataFile, '.csv');

    const metaFile = metaFiles.find(
      (file) => file === `${fileBaseName}.meta.csv`
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

    const db = new Database();

    await extractData(db, dataFilePath);
    await extractMeta(db, metaFilePath);

    await db.run(`EXPORT DATABASE '${outputDir}' (FORMAT PARQUET, CODEC ZSTD)`);

    db.close();
  }
}

runImport().then(() => {
  console.log('DONE: All imports completed!');
});

async function extractData(db: Database, csvPath: string) {
  try {
    await db.run(`CREATE TABLE data AS SELECT * FROM '${csvPath}';`);

    console.log('=> Imported data');
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

async function extractMeta(db: Database, metaFilePath: string) {
  const metaFileRows = await parseCsv<MetaFileRow>(metaFilePath);

  try {
    const columns = (
      await db.all<{ column_name: string }>(`DESCRIBE data;`)
    ).map((col) => col.column_name);

    await extractTimePeriods(db);
    await extractLocations(db, columns);
    await extractFilters(db, columns, metaFileRows);
    await extractIndicators(db, columns, metaFileRows);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}
async function extractTimePeriods(db: Database): Promise<void> {
  await db.run(
    `CREATE TABLE time_periods(
        year INT NOT NULL,
        identifier VARCHAR
     );`
  );

  await db.run(
    `INSERT INTO time_periods(year, identifier) 
      SELECT DISTINCT
        time_period AS year, 
        time_identifier AS identifier
      FROM data
      ORDER BY time_period ASC;`
  );

  console.log('=> Imported time periods meta');
}

async function extractLocations(
  db: Database,
  columns: string[]
): Promise<void> {
  const allowedCols = Object.values(geographicLevelColumns).reduce(
    (acc, cols) => {
      [cols.code, cols.name, ...(cols.other ?? [])].forEach((col) =>
        acc.add(col)
      );
      return acc;
    },
    new Set()
  );
  const locationCols = [
    'geographic_level',
    ...columns.filter((column) => allowedCols.has(column)),
  ];

  await db.run('CREATE SEQUENCE locations_seq START 1;');
  await db.run(
    `CREATE TABLE locations(
       id INT PRIMARY KEY DEFAULT nextval('locations_seq'),
       ${locationCols.map((col) => `${col} VARCHAR`)}
     );`
  );
  await db.run(
    `INSERT INTO locations(${locationCols}) SELECT DISTINCT ${locationCols} FROM data;`
  );

  console.log('=> Imported locations meta');
}

async function extractFilters(
  db: Database,
  columns: string[],
  metaFileRows: MetaFileRow[]
): Promise<void> {
  await db.run('CREATE SEQUENCE filters_seq START 1;');
  await db.run(
    `CREATE TABLE filters(
       id INT PRIMARY KEY DEFAULT nextval('filters_seq'),
       label VARCHAR NOT NULL,
       group_label VARCHAR NOT NULL,
       group_name VARCHAR NOT NULL,
       group_hint VARCHAR,
       is_aggregate BOOLEAN DEFAULT FALSE
     );`
  );

  const filters = metaFileRows.filter((row) => row.col_type === 'Filter');

  for (const filter of filters) {
    await db.run(
      `INSERT INTO
        filters(label, group_label, group_name, group_hint, is_aggregate)
        SELECT label, $1, $2, $3, CASE WHEN label = 'Total' THEN TRUE END
            FROM (SELECT DISTINCT ${filter.col_name} AS label FROM data);`,
      [filter.label, filter.col_name, filter.filter_hint]
    );
  }

  console.log('=> Imported filters meta');
}

async function extractIndicators(
  db: Database,
  columns: string[],
  metaFileRows: MetaFileRow[]
): Promise<void> {
  await db.run('CREATE SEQUENCE indicators_seq START 1;');
  await db.run(
    `CREATE TABLE indicators(
       id INT PRIMARY KEY DEFAULT nextval('indicators_seq'),
       label VARCHAR NOT NULL,
       name VARCHAR NOT NULL,
       decimal_places INT,
       unit VARCHAR
     );`
  );

  const indicators = metaFileRows.filter((row) => row.col_type === 'Indicator');

  for (const indicator of indicators) {
    await db.run(
      `INSERT INTO indicators(label, name, decimal_places, unit) VALUES ($1, $2, $3, $4);`,
      [
        indicator.label,
        indicator.col_name,
        indicator.indicator_dp,
        indicator.indicator_unit,
      ]
    );
  }

  console.log('=> Imported indicators meta');
}