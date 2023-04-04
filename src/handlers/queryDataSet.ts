import { Request, Response } from 'express';
import { mapValues } from 'lodash';
import NotFoundError from '../errors/NotFoundError';
import { DataSetQuery } from '../schema';
import createPaginationLinks from '../utils/createPaginationLinks';
import createSelfLink from '../utils/createSelfLink';
import { dataSetDirs } from '../utils/getDataSetDir';
import parsePaginationParams from '../utils/parsePaginationParams';
import { addHostUrlToLinks } from '../utils/responseUtils';
import {
  runDataSetQuery,
  runDataSetQueryToCsv,
} from '../utils/runDataSetQuery';

export async function queryDataSet(
  query: DataSetQuery,
  req: Request,
  res: Response
) {
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
    } = await runDataSetQueryToCsv(dataSetId, query, {
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

  const response = await runDataSetQuery(dataSetId, query, {
    page,
    pageSize,
    debug: !!req.query.debug,
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
