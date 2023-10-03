import qs, { ParsedQs } from 'qs';

export default function parseQueryString(query: string): ParsedQs {
  return qs.parse(query, {
    allowDots: true,
    comma: true,
  });
}
