import { Request } from 'express';
import qs from 'qs';
import { LinksViewModel } from '../schema';
import { parsePaginationParams } from './parsePaginationParams';
import { getFullRequestPath } from './requestUtils';

export function createPaginationLinks(
  req: Request,
  paging: {
    page: number;
    totalPages: number;
  }
): LinksViewModel {
  const { page, totalPages } = paging;
  const { pageSize } = parsePaginationParams(req);

  const links: LinksViewModel = {};
  const method = req.method !== 'GET' ? req.method : undefined;

  if (page > 1) {
    links.prev = {
      href: `${getFullRequestPath(req)}?${qs.stringify({
        ...req.query,
        page: page - 1,
        pageSize,
      })}`,
      method,
    };
  }

  if (page < totalPages) {
    links.next = {
      href: `${getFullRequestPath(req)}?${qs.stringify({
        ...req.query,
        page: page + 1,
        pageSize,
      })}`,
      method,
    };
  }

  return links;
}

export function createCursorPaginationLinks(
  req: Request,
  paging: {
    nextCursor?: string;
  }
): LinksViewModel {
  const { nextCursor } = paging;
  const { pageSize } = parsePaginationParams(req);

  const links: LinksViewModel = {};
  const method = req.method !== 'GET' ? req.method : undefined;

  if (nextCursor) {
    links.next = {
      href: `${getFullRequestPath(req)}?${qs.stringify({
        ...req.query,
        nextCursor,
        pageSize,
      })}`,
      method,
    };
  }

  return links;
}
