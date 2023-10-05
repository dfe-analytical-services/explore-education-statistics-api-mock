import { LinksViewModel } from '../schema';
import parsePaginationParams from './parsePaginationParams';
import { parseQueryString, toQueryString } from './queryStringParsers';

interface Options {
  self: {
    url: string;
    method: string;
  };
  paging: {
    page: number;
    totalPages: number;
  };
}

export default function createPaginationLinks({
  self,
  paging: { page, totalPages },
}: Options): LinksViewModel {
  const url = new URL(self.url);

  const query = parseQueryString(url.search.slice(1));
  const { pageSize } = parsePaginationParams(query);
  const method = self.method !== 'GET' ? self.method : undefined;

  const links: LinksViewModel = {};

  if (page > 1) {
    links.prev = {
      href: `${url.origin}${url.pathname}?${toQueryString({
        ...query,
        page: page - 1,
        pageSize,
      })}`,
      method,
    };
  }

  if (page < totalPages) {
    links.next = {
      href: `${url.origin}${url.pathname}?${toQueryString({
        ...query,
        page: page + 1,
        pageSize,
      })}`,
      method,
    };
  }

  return links;
}
