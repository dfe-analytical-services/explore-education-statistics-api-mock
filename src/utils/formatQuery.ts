import { format, ParamItems } from 'sql-formatter';

export default function formatQuery(query: string, params?: unknown[]): string {
  return format(query, {
    params: params?.map((param) =>
      typeof param === 'string' ? `'${param}'` : String(param)
    ),
    expressionWidth: 100,
    linesBetweenQueries: 1,
  });
}
