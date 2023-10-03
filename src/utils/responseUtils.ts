import { Request } from 'express';
import { mapValues } from 'lodash';
import { LinksViewModel } from '../schema';
import { getHostUrl } from './requestUtils';

export function addHostUrlToLinks(
  links: LinksViewModel,
  req: Request,
): LinksViewModel {
  const hostUrl = getHostUrl(req);

  return mapValues(links, (link) => {
    return {
      ...link,
      href: `${hostUrl}${link.href}`,
    };
  });
}
