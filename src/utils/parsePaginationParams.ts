import { Request } from 'express';

export function parsePaginationParams(req: Request): {
  page?: number;
  pageSize?: number;
} {
  const { query } = req;
  let page: number | undefined;
  let pageSize: number | undefined;

  if (
    typeof query.page === 'number' ||
    (typeof query.page === 'string' && isInt(query.page))
  ) {
    page = Number(query.page);
  }

  if (
    typeof query.pageSize === 'number' ||
    (typeof query.pageSize === 'string' && isInt(query.pageSize))
  ) {
    pageSize = Number(query.pageSize);
  }

  return {
    page,
    pageSize,
  };
}

export function parseCursorPaginationParams(req: Request): {
  cursor?: string;
  pageSize?: number;
} {
  const { query } = req;
  let cursor: string | undefined;
  let pageSize: number | undefined;

  if (typeof query.cursor === 'string') {
    cursor = query.cursor;
  }

  if (
    typeof query.pageSize === 'number' ||
    (typeof query.pageSize === 'string' && isInt(query.pageSize))
  ) {
    pageSize = Number(query.pageSize);
  }

  return {
    cursor,
    pageSize,
  };
}

function isInt(value: string): boolean {
  const number = Number(value);

  return !Number.isNaN(number) && Number.isInteger(number);
}
