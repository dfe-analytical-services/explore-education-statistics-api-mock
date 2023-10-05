import { Request, Response } from 'express';
import { mapValues, omitBy } from 'lodash';
import NotFoundError from '../errors/NotFoundError';
import { allDataSetVersions } from '../mocks/dataSetVersions';
import { DataSetQuery } from '../schema';
import createLinks from '../utils/createLinks';
import createPaginationLinks from '../utils/createPaginationLinks';
import { dataSetDirs } from '../utils/getDataSetDir';
import parsePaginationParams from '../utils/parsePaginationParams';
import { getFullRequestUrl } from '../utils/requestUtils';
import {
  runDataSetQuery,
  runDataSetQueryToCsv,
} from '../utils/runDataSetQuery';

export async function queryDataSet(req: Request, res: Response) {
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

  const { page = 1, pageSize = 500 } = parsePaginationParams(req.query);

  const dataSetQuery = parseDataSetQuery(req);

  const acceptsCsv = req.accepts('application/json', 'text/csv') === 'text/csv';

  if (acceptsCsv) {
    const {
      csv,
      paging: { totalPages, totalResults },
    } = await runDataSetQueryToCsv(dataSetId, dataSetQuery, {
      page,
      pageSize,
    });

    const links = mapValues(
      createPaginationLinks({
        self: {
          url: getFullRequestUrl(req),
          method: req.method,
        },
        paging: {
          page,
          totalPages,
        },
      }),
      (link) => link.href,
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

  const response = await runDataSetQuery(dataSetId, dataSetQuery, {
    page,
    pageSize,
    debug: !!req.query.debug,
  });

  return res.status(200).send({
    _links: createLinks({
      self: {
        url: getFullRequestUrl(req),
        method: req.method,
      },
      paging: {
        query: req.query,
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
  });
}

function parseDataSetQuery(req: Request): DataSetQuery {
  if (req.method === 'GET') {
    const {
      filters,
      geographicLevels,
      locations,
      timePeriods,
      indicators,
      sort,
    } = req.query as any;

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

  return req.body;
}
