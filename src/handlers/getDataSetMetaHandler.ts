import { RequestHandler } from 'express';
import { pick } from 'lodash';
import NotFoundError from '../errors/NotFoundError';
import { allDataSetVersions } from '../mocks/dataSetVersions';
import { DataSetMetaViewModel } from '../schema';
import createLinks from '../utils/createLinks';
import { dataSetDirs } from '../utils/getDataSetDir';
import getDataSetMeta from '../utils/getDataSetMeta';
import { getFullRequestUrl } from '../utils/requestUtils';

export type MetaType = 'filters' | 'indicators' | 'geographic' | 'timePeriods';

export default function getDataSetMetaHandler(
  metaType?: MetaType,
): RequestHandler {
  return async (req, res) => {
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
      ...(metaType ? pick(meta, getMetaFilters(metaType)) : meta),
    } satisfies DataSetMetaViewModel);
  };
}

function getMetaFilters(
  metaType: MetaType,
): (keyof Omit<DataSetMetaViewModel, '_links'>)[] {
  switch (metaType) {
    case 'filters':
      return ['filters'];
    case 'indicators':
      return ['indicators'];
    case 'geographic':
      return ['geographicLevels', 'locations'];
    case 'timePeriods':
      return ['timePeriods'];
    default:
      return [];
  }
}
