import bodyParser from 'body-parser';
import compression from 'compression';
import express, { ErrorRequestHandler } from 'express';
import 'express-async-errors';
import * as OpenApiValidator from 'express-openapi-validator';
import { BadRequest } from 'express-openapi-validator/dist/framework/types';
import { mapValues } from 'lodash';
import path from 'path';
import { InternalServerError, ValidationError } from './errors';
import ApiError from './errors/ApiError';
import NotFoundError from './errors/NotFoundError';
import { allDataSets } from './mocks/dataSets';
import { allPublications } from './mocks/publications';
import { ApiErrorViewModel } from './schema';
import createPaginationLinks from './utils/createPaginationLinks';
import createSelfLink from './utils/createSelfLink';
import { dataSetDirs } from './utils/getDataSetDir';
import {
  getDataSetCsvFileStream,
  getDataSetZipFileStream,
} from './utils/getDataSetFile';
import getDataSetMeta from './utils/getDataSetMeta';
import parsePaginationParams from './utils/parsePaginationParams';
import { addHostUrlToLinks } from './utils/responseUtils';
import { runDataSetQuery, runDataSetQueryToCsv } from './utils/runDataSetQuery';

const apiSpec = path.resolve(__dirname, './openapi.yaml');

const app = express();

app.set('trust proxy', 2);

// Middleware

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.text());
app.use(bodyParser.json());
app.use(compression());
app.use(
  OpenApiValidator.middleware({
    apiSpec,
    validateApiSpec: true,
    validateFormats: false,
    validateRequests: {
      allowUnknownQueryParameters: true,
    },
    validateResponses: true,

    ignorePaths: /\/docs/,
  })
);

app.use('/docs', express.static(apiSpec));

// Routes

app.get('/api/v1/publications', (req, res) => {
  const { search } = req.query;
  const { page = 1, pageSize = 20 } = parsePaginationParams(req);

  const filteredPublications = (
    typeof search === 'string'
      ? allPublications.filter((publication) =>
          publication.title.toLowerCase().includes(search.toLowerCase())
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
    _links: {
      self: createSelfLink(req),
      ...createPaginationLinks(req, {
        page,
        totalPages,
      }),
    },
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
    (publication) => publication.id === req.params.publicationId
  );

  if (!publication) {
    throw new NotFoundError();
  }

  res.status(200).json({
    ...publication,
    _links: addHostUrlToLinks(publication._links, req),
  });
});

app.get('/api/v1/publications/:publicationId/data-sets', (req, res) => {
  const publication = allPublications.find(
    (publication) => publication.id === req.params.publicationId
  );

  if (!publication) {
    throw new NotFoundError();
  }

  const dataSets = allDataSets.filter(
    (dataSet) => dataSet.publication.id === publication.id
  );

  res.status(200).json(
    dataSets.map(({ viewModel }) => ({
      ...viewModel,
      _links: addHostUrlToLinks(viewModel._links, req),
    }))
  );
});

app.get('/api/v1/data-sets/:dataSetId', async (req, res) => {
  const dataSetId = req.params.dataSetId;

  const matchingDataSet = allDataSets.find(
    (dataSet) => dataSet.id === dataSetId
  );

  if (!matchingDataSet) {
    throw new NotFoundError();
  }

  const { viewModel } = matchingDataSet;

  return res.status(200).json({
    ...viewModel,
    _links: addHostUrlToLinks(viewModel._links, req),
  });
});

app.get('/api/v1/data-sets/:dataSetId/meta', async (req, res) => {
  const dataSetId = req.params.dataSetId;

  if (dataSetDirs[dataSetId]) {
    const meta = await getDataSetMeta(dataSetId);

    return res.status(200).json({
      _links: {
        self: createSelfLink(req),
        ...addHostUrlToLinks(
          {
            query: {
              href: `/api/v1/data-sets/${dataSetId}/query`,
              method: 'POST',
            },
            file: {
              href: `/api/v1/data-sets/${dataSetId}/file`,
            },
          },
          req
        ),
      },
      ...meta,
    });
  }

  throw new NotFoundError();
});

app.post('/api/v1/data-sets/:dataSetId/query', async (req, res) => {
  const { dataSetId } = req.params;

  if (!dataSetDirs[dataSetId]) {
    throw new NotFoundError();
  }

  const { page = 1, pageSize = 500 } = parsePaginationParams(req);

  const acceptsCsv = req.accepts('application/json', 'text/csv') === 'text/csv';

  if (acceptsCsv) {
    const {
      csv,
      paging: { totalPages, totalResults },
    } = await runDataSetQueryToCsv(dataSetId, req.body, {
      page,
      pageSize,
    });

    const links = mapValues(
      createPaginationLinks(req, {
        page,
        totalPages,
      }),
      (link) => link.href
    );

    return res
      .status(200)
      .contentType('text/csv')
      .setHeader('Page', page)
      .setHeader('Page-Size', pageSize)
      .setHeader('Total-Results', totalResults)
      .setHeader('Total-Pages', totalPages)
      .links(links)
      .send(csv);
  }

  const response = await runDataSetQuery(dataSetId, req.body, {
    page,
    pageSize,
    debug: typeof req.query.debug !== 'undefined',
  });

  return res.status(200).send({
    _links: {
      self: createSelfLink(req),
      ...createPaginationLinks(req, {
        page,
        totalPages: response.paging.totalPages,
      }),
      ...addHostUrlToLinks(
        {
          file: {
            href: `/api/v1/data-sets/${dataSetId}/file`,
          },
          meta: {
            href: `/api/v1/data-sets/${dataSetId}/meta`,
          },
        },
        req
      ),
    },
    ...response,
  });
});

app.get('/api/v1/data-sets/:dataSetId/file', async (req, res) => {
  const { dataSetId } = req.params;

  if (!dataSetDirs[dataSetId]) {
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
        `attachment; filename="${fileName}.zip"`
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

// Error handling

const errorHandler: ErrorRequestHandler<{}, ApiErrorViewModel> = (
  err,
  req,
  res,
  _
) => {
  if (err instanceof BadRequest) {
    return ValidationError.fromBadRequest(err, req).toResponse(res);
  }

  if (err instanceof ApiError) {
    return err.toResponse(res);
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
    }).toResponse(res);
  }

  console.error(err);

  return new InternalServerError().toResponse(res);
};

app.use(errorHandler);

const port = process.env.PORT || 8080;

app.listen(port, () => console.log(`Server is running on port ${port}`));

export default app;
