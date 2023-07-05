import { Request, Response } from 'express';
import { mapValues } from 'lodash';
import NotFoundError from '../errors/NotFoundError';
import { allDataSetVersions } from '../mocks/dataSetVersions';
import { DataSetQuery } from '../schema';
import { createCursorPaginationLinks } from '../utils/createPaginationLinks';
import createSelfLink from '../utils/createSelfLink';
import { dataSetDirs } from '../utils/getDataSetDir';
import { parseCursorPaginationParams } from '../utils/parsePaginationParams';
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
  const { dataSetVersion } = req.query;

  if (!dataSetDirs[dataSetId]) {
    throw new NotFoundError();
  }

  if (
    dataSetVersion &&
    !allDataSetVersions[dataSetId].some(
      (version) => version.number === dataSetVersion
    )
  ) {
    throw new NotFoundError();
  }

  const { cursor, pageSize = 500 } = parseCursorPaginationParams(req);

  const acceptsCsv = req.accepts('application/json', 'text/csv') === 'text/csv';

  if (acceptsCsv) {
    const {
      csv,
      paging: { totalPages, totalResults },
    } = await runDataSetQueryToCsv(dataSetId, query, {
      cursor,
      pageSize,
    });

    const links = mapValues(
      createCursorPaginationLinks(req, {}),
      (link) => link.href
    );

    res
      .status(200)
      .contentType('text/csv')
      .setHeader('Page-Size', pageSize)
      .setHeader('Total-Results', totalResults)
      .setHeader('Total-Pages', totalPages);

    if (cursor) {
      res.setHeader('Cursor', cursor);
    }

    res.links(links).send(csv);
  }

  const response = await runDataSetQuery(dataSetId, query, {
    cursor,
    pageSize,
    debug: !!req.query.debug,
  });

  return res.status(200).send({
    _links: {
      self: createSelfLink(req),
      ...createCursorPaginationLinks(req, {}),
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
