import {
  app,
  HttpRequest,
  HttpResponseInit,
  InvocationContext,
} from '@azure/functions';
import { omitBy } from 'lodash';
import path from 'path';
import { ParsedQs } from 'qs';
import NotFoundError from '../errors/NotFoundError';
import { allDataSetVersions } from '../mocks/dataSetVersions';
import { DataSetQuery } from '../schema';
import createLinks from '../utils/createLinks';
import createPaginationLinks from '../utils/createPaginationLinks';
import { dataSetDirs } from '../utils/getDataSetDir';
import parsePaginationParams from '../utils/parsePaginationParams';
import { parseQueryStringFromUrl } from '../utils/queryStringParsers';
import {
  runDataSetQuery,
  runDataSetQueryToCsv,
} from '../utils/runDataSetQuery';

process.chdir(path.resolve(__dirname, '..'));

app.http('queryDataSet', {
  methods: ['GET', 'POST'],
  route: 'v1/data-sets/{dataSetId}/query',
  authLevel: 'anonymous',
  handler: queryDataSet,
});

export async function queryDataSet(
  request: HttpRequest,
  context: InvocationContext,
): Promise<HttpResponseInit> {
  const { dataSetId } = request.params;
  const { dataSetVersion, ...query } = parseQueryStringFromUrl(request.url);

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

  const { page = 1, pageSize = 500 } = parsePaginationParams(query);
  const dataSetQuery = await parseDataSetQuery(request, query);

  const acceptsCsv = !!request.headers.get('accept')?.includes('text/csv');

  if (acceptsCsv) {
    const {
      csv,
      paging: { totalPages, totalResults },
    } = await runDataSetQueryToCsv(dataSetId, dataSetQuery, {
      page,
      pageSize,
    });

    const links = createPaginationLinks({
      self: {
        url: request.url,
        method: request.method,
      },
      paging: {
        page,
        totalPages,
      },
    });

    return {
      status: 200,
      body: csv,
      headers: {
        'Content-Type': 'text/csv; charset=utf-8',
        Link: Object.entries(links)
          .map(([rel, link]) => `<${link.href}>; rel=${rel}`)
          .join(', '),
        Page: page.toString(),
        'Page-Size': pageSize.toString(),
        'Total-Results': totalResults.toString(),
        'Total-Pages': totalPages.toString(),
      },
    };
  }

  const response = await runDataSetQuery(dataSetId, dataSetQuery, {
    page,
    pageSize,
    debug: !!query.debug,
  });

  return {
    status: 200,
    jsonBody: {
      _links: createLinks({
        self: {
          url: request.url,
          method: request.method,
        },
        paging: {
          query,
          page,
          totalPages: response.paging.totalPages,
        },
        links: {
          file: {
            href: `/api/v1/data-sets/${dataSetId}/file`,
          },
          meta: {
            href: `/api/v1/data-sets/${dataSetId}/meta`,
          },
        },
      }),
      ...response,
    },
  };
}

async function parseDataSetQuery(
  request: HttpRequest,
  parsedQuery: ParsedQs,
): Promise<DataSetQuery> {
  if (request.method === 'GET') {
    const {
      filters,
      geographicLevels,
      locations,
      timePeriods,
      indicators,
      sort,
    } = parsedQuery as any;

    return {
      facets: omitBy(
        {
          filters,
          geographicLevels,
          locations,
          timePeriods,
        },
        (value) => value === undefined,
      ),
      indicators,
      sort,
    };
  }

  return (await request.json()) as DataSetQuery;
}
