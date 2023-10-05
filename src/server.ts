import bodyParser from 'body-parser';
import compression from 'compression';
import express, { ErrorRequestHandler } from 'express';
import 'express-async-errors';
import * as OpenApiValidator from 'express-openapi-validator';
import { BadRequest } from 'express-openapi-validator/dist/framework/types';
import { omit, pick } from 'lodash';
import morgan from 'morgan';
import path from 'path';
import { InternalServerError, ValidationError } from './errors';
import ApiError from './errors/ApiError';
import NotFoundError from './errors/NotFoundError';
import { queryDataSet } from './handlers/queryDataSet';
import { allDataSets } from './mocks/dataSets';
import { allDataSetVersions } from './mocks/dataSetVersions';
import { allPublications } from './mocks/publications';
import {
  ApiErrorViewModel,
  DataSetVersionViewModel,
  PagedDataSetVersionsViewModel,
} from './schema';
import createLinks from './utils/createLinks';
import getDataSetDetails from './utils/getDataSetDetails';
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

process.chdir(__dirname);

const apiSpec = path.resolve(__dirname, './openapi.yaml');

const app = express();

app.set('trust proxy', 2);
app.set('query parser', parseQueryString);

// Middleware

app.use(morgan('dev'));
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

  const matchingDataSets = allDataSets
    .filter((dataSet) => dataSet.publication.id === publication.id)
    .map((dataSet) => dataSet.viewModel);

  const dataSets = await getDataSetDetails(matchingDataSets);

  return res.status(200).json(
    dataSets.map(({ _links, ...dataSet }) => {
      return {
        ...dataSet,
        _links: addHostUrlToLinks(_links, req),
      };
    }),
  );
});

app.get('/api/v1/data-sets/:dataSetId', async (req, res) => {
  const { dataSetId } = req.params;
  const { dataSetVersion } = req.query;

  const matchingDataSet = allDataSets.find(
    (dataSet) => dataSet.id === dataSetId,
  );

  if (!matchingDataSet) {
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

  const { viewModel } = matchingDataSet;
  const [{ _links, ...dataSet }] = await getDataSetDetails([viewModel]);

  return res.status(200).json({
    ...dataSet,
    _links: addHostUrlToLinks(_links, req),
  });
});

app.get('/api/v1/data-sets/:dataSetId/meta', async (req, res) => {
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

  const meta = await getDataSetMeta(dataSetId);

  return res.status(200).json({
    _links: createLinks({
      self: {
        url: getFullRequestUrl(req),
        method: req.method,
      },
      links: {
        query: {
          href: `/api/v1/data-sets/${dataSetId}/query`,
          method: 'POST',
        },
        file: {
          href: `/api/v1/data-sets/${dataSetId}/file`,
        },
      },
    }),
    ...meta,
  });
});

app.get('/api/v1/data-sets/:dataSetId/query', queryDataSet);
app.post('/api/v1/data-sets/:dataSetId/query', queryDataSet);

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

  const matchingVersions: DataSetVersionViewModel[] = allDataSetVersions[
    dataSetId
  ].map((version) => {
    return {
      _links: addHostUrlToLinks(version._links, req),
      ...omit(version, ['changes', '_links']),
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

    return res.status(200).json(matchingVersion);
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

    console.error(err);

    return new InternalServerError();
  };

  const apiError = handleApiError(err);

  res.status(apiError.status);

  if (res.req.accepts('application/json')) {
    return res.send(this);
  }

  if (res.req.accepts('text/html', 'text/*')) {
    return res.send(apiError.title);
  }

  return res.send('');
};

app.use(errorHandler);

const port = process.env.PORT || 8080;

app.listen(port, () => console.log(`Server is running on port ${port}`));

export default app;
