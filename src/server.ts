import * as appInsights from 'applicationinsights';
import bodyParser from 'body-parser';
import compression from 'compression';
import express, { ErrorRequestHandler } from 'express';
import 'express-async-errors';
import * as OpenApiValidator from 'express-openapi-validator';
import {
  BadRequest,
  NotFound,
} from 'express-openapi-validator/dist/framework/types';
import expressWinston from 'express-winston';
import { omit, pick } from 'lodash';
import path from 'path';
import winston from 'winston';
import { InternalServerError, ValidationError } from './errors';
import ApiError from './errors/ApiError';
import NotFoundError from './errors/NotFoundError';
import getDataSetMetaHandler from './handlers/getDataSetMetaHandler';
import { queryDataSetHandler } from './handlers/queryDataSetHandler';
import { allDataSets } from './mocks/dataSets';
import { allDataSetVersions } from './mocks/dataSetVersions';
import { allPublications } from './mocks/publications';
import {
  ApiErrorViewModel,
  ChangeSetViewModel,
  DataSetLatestVersionViewModel,
  DataSetViewModel,
  PagedDataSetsViewModel,
  PagedDataSetVersionsViewModel,
} from './schema';
import createLinks from './utils/createLinks';
import getDataSetVersionDetails from './utils/getDataSetVersionDetails';
import { dataSetDirs } from './utils/getDataSetDir';
import {
  getDataSetCsvFileStream,
  getDataSetZipFileStream,
} from './utils/getDataSetFile';
import getDataSetMeta from './utils/getDataSetMeta';
import parsePaginationParams from './utils/parsePaginationParams';
import { parseQueryString } from './utils/queryStringParsers';
import { getFullRequestUrl } from './utils/requestUtils';
import { addHostUrlToLinks } from './utils/responseUtils';

const isProd = process.env.NODE_ENV === 'production';

if (process.env.APPLICATIONINSIGHTS_CONNECTION_STRING) {
  appInsights
    .setup()
    .setAutoCollectRequests(true)
    .setAutoCollectPerformance(true, true)
    .setAutoCollectExceptions(true)
    .setAutoCollectDependencies(true)
    .setAutoCollectConsole(true, true)
    .setAutoCollectPreAggregatedMetrics(true)
    .setSendLiveMetrics(false)
    .setInternalLogging(false, true)
    .enableWebInstrumentation(false)
    .start();
}

process.chdir(__dirname);

const apiSpec = path.resolve(__dirname, './openapi.yaml');

const app = express();

app.set('trust proxy', 2);
app.set('query parser', parseQueryString);

// Middleware

app.use(
  expressWinston.logger({
    transports: [new winston.transports.Console()],
    format: isProd
      ? winston.format.combine(
          winston.format.timestamp(),
          winston.format.json(),
        )
      : winston.format.combine(
          winston.format.timestamp(),
          winston.format.colorize(),
          winston.format.simple(),
        ),
    meta: isProd,
    msg: 'HTTP {{req.method}} {{req.url}}',
    expressFormat: true,
    colorize: !isProd,
  }),
);

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.text());
app.use(bodyParser.json());
app.use(compression());
app.use(
  OpenApiValidator.middleware({
    apiSpec,
    validateApiSpec: true,
    validateFormats: false,
    validateResponses: process.env.NODE_ENV === 'development',
    ignorePaths: /\/docs/,
  }),
);

app.use('/docs', express.static(apiSpec));

// Routes

app.get('/api/v1/publications', (req, res) => {
  const { search } = req.query;
  const { page = 1, pageSize = 20 } = parsePaginationParams(req.query);

  const filteredPublications = (
    typeof search === 'string'
      ? allPublications.filter((publication) =>
          publication.title.toLowerCase().includes(search.toLowerCase()),
        )
      : allPublications
  ).map((publication) => ({
    ...publication,
    _links: addHostUrlToLinks(publication._links, req),
  }));

  const start = (page - 1) * pageSize;
  const totalPages =
    pageSize > 0 ? Math.ceil(filteredPublications.length / pageSize) : 0;

  res.status(200).json({
    _links: createLinks({
      self: {
        url: getFullRequestUrl(req),
        method: req.method,
      },
      paging: {
        query: req.query,
        page,
        totalPages,
      },
    }),
    paging: {
      page,
      pageSize,
      totalPages: totalPages,
      totalResults: filteredPublications.length,
    },
    results: filteredPublications.slice(start, start + pageSize),
  });
});

app.get('/api/v1/publications/:publicationId', (req, res) => {
  const publication = allPublications.find(
    (publication) => publication.id === req.params.publicationId,
  );

  if (!publication) {
    throw new NotFoundError();
  }

  res.status(200).json({
    ...publication,
    _links: addHostUrlToLinks(publication._links, req),
  });
});

app.get('/api/v1/publications/:publicationId/data-sets', async (req, res) => {
  const publication = allPublications.find(
    (publication) => publication.id === req.params.publicationId,
  );

  if (!publication) {
    throw new NotFoundError();
  }

  const { page = 1, pageSize = 10 } = parsePaginationParams(req.query);

  const matchingDataSets = allDataSets
    .filter((dataSet) => dataSet.publication.id === publication.id)
    .map((dataSet) => dataSet.viewModel);

  const start = (page - 1) * pageSize;
  const totalPages =
    pageSize > 0 ? Math.ceil(matchingDataSets.length / pageSize) : 0;

  const results: DataSetViewModel[] = await Promise.all(
    matchingDataSets
      .slice(start, start + pageSize)
      .map(async ({ _links, ...dataSet }) => {
        const [latestVersion] = await getDataSetVersionDetails(dataSet.id, [
          allDataSetVersions[dataSet.id][0],
        ]);

        return {
          ...dataSet,
          latestVersion: pick(latestVersion, [
            'number',
            'published',
            'totalResults',
            'filters',
            'timePeriods',
            'geographicLevels',
            'indicators',
          ]) satisfies DataSetLatestVersionViewModel,
          _links,
        };
      }),
  );

  return res.status(200).json({
    _links: createLinks({
      self: {
        url: getFullRequestUrl(req),
        method: req.method,
      },
      paging: {
        query: req.query,
        page,
        totalPages,
      },
    }),
    paging: {
      page,
      pageSize,
      totalPages: totalPages,
      totalResults: matchingDataSets.length,
    },
    results,
  } satisfies PagedDataSetsViewModel);
});

app.get('/api/v1/data-sets/:dataSetId', async (req, res) => {
  const { dataSetId } = req.params;

  const matchingDataSet = allDataSets.find(
    (dataSet) => dataSet.id === dataSetId,
  );

  if (!matchingDataSet) {
    throw new NotFoundError();
  }

  const { viewModel } = matchingDataSet;

  const [latestVersion] = await getDataSetVersionDetails(dataSetId, [
    allDataSetVersions[viewModel.id][0],
  ]);

  return res.status(200).json({
    ...viewModel,
    latestVersion: pick(latestVersion, [
      'number',
      'published',
      'totalResults',
      'filters',
      'timePeriods',
      'geographicLevels',
      'indicators',
    ]) satisfies DataSetLatestVersionViewModel,
    _links: addHostUrlToLinks(viewModel._links, req),
  } satisfies DataSetViewModel);
});

app.get('/api/v1/data-sets/:dataSetId/meta', getDataSetMetaHandler());
app.get(
  '/api/v1/data-sets/:dataSetId/meta/filters',
  getDataSetMetaHandler('filters'),
);
app.get(
  '/api/v1/data-sets/:dataSetId/meta/indicators',
  getDataSetMetaHandler('indicators'),
);
app.get(
  '/api/v1/data-sets/:dataSetId/meta/geographic',
  getDataSetMetaHandler('geographic'),
);
app.get(
  '/api/v1/data-sets/:dataSetId/meta/time-periods',
  getDataSetMetaHandler('timePeriods'),
);

app.get('/api/v1/data-sets/:dataSetId/query', queryDataSetHandler);
app.post('/api/v1/data-sets/:dataSetId/query', queryDataSetHandler);

app.get('/api/v1/data-sets/:dataSetId/file', async (req, res) => {
  const { dataSetId } = req.params;
  const { dataSetVersion } = req.query;

  if (!dataSetDirs[dataSetId]) {
    throw new NotFoundError();
  }

  if (
    dataSetVersion &&
    !allDataSetVersions[dataSetId].some(
      (version) => version.number === dataSetVersion,
    )
  ) {
    throw new NotFoundError();
  }

  const fileName = `dataset_${dataSetId}`;
  const accepts = req.accepts('text/csv', 'application/zip');

  if (accepts === 'application/zip') {
    const meta = await getDataSetMeta(dataSetId);
    const stream = await getDataSetZipFileStream(dataSetId, meta);

    res
      .status(200)
      .contentType('application/zip')
      .setHeader(
        'Content-Disposition',
        `attachment; filename="${fileName}.zip"`,
      );

    return stream.pipe(res);
  }

  const stream = await getDataSetCsvFileStream(dataSetId);

  res
    .status(200)
    .contentType('text/csv')
    .setHeader('Content-Disposition', `attachment; filename="${fileName}.csv"`);

  return stream.pipe(res);
});

app.get('/api/v1/data-sets/:dataSetId/versions', async (req, res) => {
  const { dataSetId } = req.params;

  if (!dataSetDirs[dataSetId]) {
    throw new NotFoundError();
  }

  if (!allDataSetVersions[dataSetId]) {
    throw new NotFoundError();
  }

  const matchingVersions = (
    await getDataSetVersionDetails(dataSetId, allDataSetVersions[dataSetId])
  ).map(({ _links, ...version }) => {
    return {
      _links: addHostUrlToLinks(_links, req),
      ...omit(version, 'changes'),
    };
  });

  const { page = 1, pageSize = 20 } = parsePaginationParams(req.query);

  let response: PagedDataSetVersionsViewModel;

  const start = (page - 1) * pageSize;
  const totalPages =
    pageSize > 0 ? Math.ceil(matchingVersions.length / pageSize) : 0;

  response = {
    _links: createLinks({
      self: {
        url: getFullRequestUrl(req),
        method: req.method,
      },
      paging: {
        query: req.query,
        page,
        totalPages,
      },
    }),
    paging: {
      page,
      pageSize,
      totalPages,
      totalResults: matchingVersions.length,
    },
    results: matchingVersions.slice(start, start + pageSize),
  };

  return res.status(200).json(response);
});

app.get(
  '/api/v1/data-sets/:dataSetId/versions/:dataSetVersion',
  async (req, res) => {
    const { dataSetId, dataSetVersion } = req.params;

    if (!dataSetDirs[dataSetId]) {
      throw new NotFoundError();
    }

    if (!allDataSetVersions[dataSetId]) {
      throw new NotFoundError();
    }

    const matchingVersion = allDataSetVersions[dataSetId].find(
      (version) => version.number === dataSetVersion,
    );

    if (!matchingVersion) {
      throw new NotFoundError();
    }

    const [{ _links, ...version }] = await getDataSetVersionDetails(dataSetId, [
      matchingVersion,
    ]);

    return res.status(200).json({
      ...omit(version, 'changes'),
      _links: addHostUrlToLinks(_links, req),
    });
  },
);

app.get(
  '/api/v1/data-sets/:dataSetId/versions/:dataSetVersion/changes',
  async (req, res) => {
    const { dataSetId, dataSetVersion } = req.params;

    if (!dataSetDirs[dataSetId]) {
      throw new NotFoundError();
    }

    if (!allDataSetVersions[dataSetId]) {
      throw new NotFoundError();
    }

    const version = allDataSetVersions[dataSetId].find(
      (version) => version.number === dataSetVersion,
    );

    if (!version) {
      throw new NotFoundError();
    }

    return res.status(200).json({
      _links: createLinks({
        self: {
          url: getFullRequestUrl(req),
          method: req.method,
        },
        links: {
          version: {
            href: `/api/data-sets/${dataSetId}/versions/${dataSetVersion}`,
          },
        },
      }),
      changes: version.changes ?? [],
    } satisfies ChangeSetViewModel);
  },
);

// Error handling

const errorHandler: ErrorRequestHandler<{}, ApiErrorViewModel | string> = (
  err,
  req,
  res,
  _,
) => {
  const handleApiError = (err: unknown) => {
    if (err instanceof BadRequest) {
      return ValidationError.fromBadRequest(err, req);
    }

    if (err instanceof ApiError) {
      return err;
    }

    if (err instanceof NotFound) {
      return new ApiError({
        title: 'The requested resource could not be found.',
        status: 404,
        type: 'Not Found',
      });
    }

    if (
      err instanceof SyntaxError &&
      'statusCode' in err &&
      err.statusCode === 400
    ) {
      return new ApiError({
        title: 'Malformed request could not be parsed due to syntax errors.',
        status: 400,
        type: 'Bad Request',
      });
    }

    return new InternalServerError();
  };

  const apiError = handleApiError(err);

  res.status(apiError.status);

  if (res.req.accepts('application/json')) {
    return res.send(apiError);
  }

  if (res.req.accepts('text/html', 'text/*')) {
    return res.send(apiError.title);
  }

  return res.send('');
};

app.use(
  expressWinston.errorLogger({
    transports: [new winston.transports.Console()],
    format: isProd
      ? winston.format.combine(
          winston.format.timestamp(),
          winston.format.json(),
        )
      : winston.format.combine(
          winston.format.timestamp(),
          winston.format.prettyPrint({
            colorize: !isProd,
          }),
        ),
    blacklistedMetaFields: isProd
      ? ['os', 'process', 'trace']
      : ['message', 'stack', 'os', 'process', 'trace'],
    meta: isProd,
    msg: '{{err.message}}',
  }),
);
app.use(errorHandler);

const port = process.env.PORT || 8080;

app.listen(port, () => console.log(`Server is running on port ${port}`));

export default app;
