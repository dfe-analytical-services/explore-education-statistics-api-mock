import bodyParser from 'body-parser';
import compression from 'compression';
import express, { ErrorRequestHandler, Request } from 'express';
import * as OpenApiValidator from 'express-openapi-validator';
import { mapValues } from 'lodash';
import path from 'path';
import { allDataSets, spcDataSets } from './mocks/dataSets';
import { publications, spcPublication } from './mocks/publications';
import { ApiErrorViewModel, LinksViewModel } from './schema';
import { dataSetDirs } from './utils/getDataSetDir';
import getDataSetMeta from './utils/getDataSetMeta';
import normalizeApiErrors from './utils/normalizeApiErrors';
import paginateResults from './utils/paginateResults';
import queryDataSetData from './utils/queryDataSetData';

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

  res.status(200).json(
    paginateResults(filteredPublications, {
      ...req.query,
      baseUrl: `${getHostUrl(req)}/api/v1/publications`,
    })
  );
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

app.get('/api/v1/data-sets/:dataSetId/meta', async (req, res) => {
  if (dataSetDirs[req.params.dataSetId]) {
    let meta = await getDataSetMeta(req.params.dataSetId);
    res.status(200).json(meta);
    return;
  }

  res.status(404).json(notFoundError());
});

app.post('/api/v1/data-sets/:dataSetId/query', async (req, res) => {
  if (dataSetDirs[req.params.dataSetId]) {
    const formatCsv = req.accepts().includes('text/csv');
    const results = await queryDataSetData(req.params.dataSetId, req.body, {
      debug: typeof req.query.debug !== 'undefined',
      formatCsv,
    });

    res.contentType(formatCsv ? 'text/csv' : 'application/json');

    return res.status(200).send(
      typeof results === 'string'
        ? results
        : {
            ...results,
            _links: addHostUrlToLinks(results._links, req),
          }
    );
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

const errorHandler: ErrorRequestHandler = (err, req, res, _) => {
  console.error(err);
  res.status(err.status || 500).json({
    status: err.status,
    title: err.message,
    type: err.name,
    errors: normalizeApiErrors(err.errors),
  });
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

function getHostUrl(req: Request) {
  const host = req.get('host');
  return host ? `${req.protocol}://${host}` : '';
}
