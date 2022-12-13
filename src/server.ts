import bodyParser from 'body-parser';
import compression from 'compression';
import express, { ErrorRequestHandler, Request } from 'express';
import 'express-async-errors';
import * as OpenApiValidator from 'express-openapi-validator';
import { BadRequest } from 'express-openapi-validator/dist/framework/types';
import { mapValues } from 'lodash';
import path from 'path';
import ApiError from './errors/ApiError';
import { InternalServerError, ValidationError } from './errors';
import { allDataSets, spcDataSets } from './mocks/dataSets';
import { publications, spcPublication } from './mocks/publications';
import { ApiErrorViewModel, LinksViewModel } from './schema';
import createPaginationLinks from './utils/createPaginationLinks';
import createSelfLink from './utils/createSelfLink';
import { dataSetDirs } from './utils/getDataSetDir';
import getDataSetMeta from './utils/getDataSetMeta';
import parsePaginationParams from './utils/parsePaginationParams';
import { getHostUrl } from './utils/requestUtils';
import { runDataSetQuery, runDataSetQueryToCsv } from './utils/runDataSetQuery';

const apiSpec = path.resolve(__dirname, './openapi.yaml');

const app = express();

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
      ? publications.filter((publication) =>
          publication.title.toLowerCase().includes(search.toLowerCase())
        )
      : publications
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
  const publication = publications.find(
    (publication) => publication.id === req.params.publicationId
  );

  if (!publication) {
    return res.status(404).json(notFoundError());
  }

  res.status(200).json({
    ...publication,
    _links: addHostUrlToLinks(publication._links, req),
  });
});

app.get('/api/v1/publications/:publicationId/data-sets', (req, res) => {
  switch (req.params.publicationId) {
    case spcPublication.id:
      res.status(200).json(
        spcDataSets.map((dataSet) => ({
          ...dataSet,
          _links: addHostUrlToLinks(dataSet._links, req),
        }))
      );
      break;
    default:
      res.status(404).json(notFoundError());
  }
});

app.get('/api/v1/data-sets/:dataSetId', async (req, res) => {
  const dataSetId = req.params.dataSetId;

  const matchingDataSet = allDataSets.find(
    (dataSet) => dataSet.id === dataSetId
  );

  if (!matchingDataSet) {
    return res.status(404).json(notFoundError());
  }

  return res.status(200).json({
    ...matchingDataSet,
    _links: addHostUrlToLinks(matchingDataSet._links, req),
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

  res.status(404).json(notFoundError());
});

app.post('/api/v1/data-sets/:dataSetId/query', async (req, res) => {
  const dataSetId = req.params.dataSetId;

  const { page = 1, pageSize = 500 } = parsePaginationParams(req);

  if (dataSetDirs[dataSetId]) {
    if (req.accepts().includes('text/csv')) {
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
  }

  res.status(404).json(notFoundError());
});

app.get('/api/v1/data-sets/:dataSetId/file', (req, res) => {
  if (allDataSets.some((dataSet) => dataSet.id === req.params.dataSetId)) {
    return res
      .status(200)
      .sendFile(path.resolve(__dirname, '../mocks/dataSetFile.zip'));
  }

  res.status(404).json(notFoundError());
});

// Error handling

const errorHandler: ErrorRequestHandler<{}, ApiErrorViewModel> = (
  err,
  req,
  res,
  _
) => {
  console.error(err);

  if (err instanceof BadRequest) {
    return ValidationError.fromBadRequest(err).toResponse(res);
  }

  if (err instanceof ApiError) {
    return err.toResponse(res);
  }

  return new InternalServerError().toResponse(res);
};

app.use(errorHandler);

const port = process.env.PORT || 8080;

app.listen(port, () => console.log(`Server is running on port ${port}`));

export default app;

function notFoundError(): ApiErrorViewModel {
  return {
    status: 404,
    type: 'https://datatracker.ietf.org/doc/html/rfc7231#section-6.5.4',
    title: 'Not Found',
  };
}

function addHostUrlToLinks(
  links: LinksViewModel,
  req: Request
): LinksViewModel {
  const hostUrl = getHostUrl(req);

  return mapValues(links, (link) => {
    return {
      ...link,
      href: `${hostUrl}${link.href}`,
    };
  });
}
