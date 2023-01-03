# Explore Education Statistics Public API Mock

This is a mock implementation of the upcoming public API for the Explore Education Statistics service.

## Getting started

1. Install Node.js v18. You can use NVM to do this.
2. Run `npm ci` from the project root.
3. Run `npm run schema` to generate API schema types.
4. Run `npm start` to start the dev server.

## Overview

The API is an implementation of the OpenAPI specification found in `src/openapi.yaml`. This is
actually validated at runtime by the server itself, meaning that requests and responses are expected
to respect the specification. We use [express-openapi-validator](https://github.com/cdimascio/express-openapi-validator)
to do this.

Data is queried using [DuckDB](https://duckdb.org/), a powerful in-process OLAP database that
allows us to query from data files directly. This allows us to circumvent the need for a traditional
database server as we can deploy the data files as part of the deployment artifact.

### Investigating DuckDB

As part of this, this API serves as an investigation into the viability of DuckDB as a complete
replacement for SQL Server in the current service. This is a potentially very exciting prospect as:

- Horizontal scaling is trivial, especially if deployed in a completely on-demand runtime e.g.
  Azure Function or Azure Container App.
- Performance is very good because DuckDB is aimed at OLAP workloads such as analyzing big data sets.
  Comparatively, SQL Server is aimed at OLTP workloads and doesn't perform as well without using
  vertical scaling and a bunch of hacks/tricks.
- Storage is no longer a problem as data can be stored in Blob Storage very cheaply and flexibly (no
  need to remember to increase database disk size).
- It is very cheap compared to the current architecture as Azure SQL is very expensive.
- The data model is super simple. Querying data is essentially the same as querying a CSV directly.
- Importing data is **much** faster i.e. usually less than a minute in most cases.

To achieve most of these benefits, the data is imported into a very space efficient Parquet format.
This is further compressed using ZStandard compression to produce ridiculous levels of compression.
For example:

- `e-and-t-geography-detailed_6years_reordered`: ~1GB csv compresses to 3.4MB
- `qua01`: ~715MB csv compresses to 9.4MB

As the data can be compressed to such an extent, it becomes practical to store and query from Blob
Storage. In our investigation so far, the easiest way of doing this has been to mount Blob Storage
directly into App Services using path mappings.

## Scripts

- `npm run schema` - generates schema types from the OpenAPI specification
- `npm run build` - builds the production server
- `npm start` - starts the dev server
- `npm run start:prod` - starts the production server (needs `npm run build` to be ran first)
- `npm run import` - imports data sets from the `data-imports` directory

## Importing new data sets

New data sets can be imported by:

1. Adding the CSV files into the `data-imports` directory e.g.

   ```bash
   cp your_data.csv data-imports/
   cp your_data.meta.csv data-imports/
   ```

   Files must follow the typical data/meta file conventions used in EES itself.

2. Run `npm run import`. This will generate a new directory in `src/data` containing imported data
   files like the following:

   - `data.parquet`
   - `filters.parquet`
   - `indicators.parquet`
   - `load.sql`
   - `locations.parquet`
   - `schema.sql`
   - `time_periods.parquet`

3. Create a new publication (if required) for the data set to make it discoverable via the
   publication endpoints. You can do this manually in `src/mocks/publications.ts`.

   ```ts
   // src/mocks/publications.ts

   export const yourPublication = createPublication({
     id: 'your-uuid',
     title: 'Your publication',
   });
   
   export const allPublications = [
     someOtherPublication,
     yourPublication,
   ];
   ```

4. Manually add data set details to `src/mocks/dataSets.ts` e.g.

   ```ts
   // src/mocks/dataSets.ts
   import { yourPublication } from './publications';
   
   export const yourDataSet = createDataSet({
     id: 'your-uuid',
     content: '<p>Your content.</p>',
     name: 'Your data set',
     geographicLevels: ['...'],
     timePeriods: {
       start: '2015/16',
       end: '2021/22',
     },
     publication: yourPublication
   });

   export const yourPublicationDataSets = [yourDataSet];
   
   export const allDataSets = [
     ...someOtherDataSets,
     ...yourPublicationDataSets,
   ];
   ```

5. Add the data set, and its directory to `src/utils/getDataSetDir.ts`:

   ```ts
   import { yourDataSet } from '../mocks/dataSets';

   export const dataSetDirs = {
     [yourDataSet.id]: 'your_data_directory',
     ...
   };
   ```

