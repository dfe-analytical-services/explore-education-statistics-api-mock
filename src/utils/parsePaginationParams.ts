import type { ParsedQs } from 'qs';

interface PaginationParams {
  page?: number;
  pageSize?: number;
}

export default function parsePaginationParams(
  query: ParsedQs,
): PaginationParams {
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

function isInt(value: string): boolean {
  const number = Number(value);

  return !Number.isNaN(number) && Number.isInteger(number);
}
