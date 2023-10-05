import qs, { IParseOptions, IStringifyOptions, ParsedQs } from 'qs';

export const queryStringOptions: Omit<
  IParseOptions & IStringifyOptions,
  'decoder'
> = {
  arrayFormat: 'comma',
  comma: true,
  // Not in type declaration, just ignore it for now.
  // @ts-ignore
  commaRoundTrip: true,
  encodeValuesOnly: true,
};

export function parseQueryString(query: string): ParsedQs {
  return qs.parse(query, queryStringOptions);
}

export function parseQueryStringFromUrl(url: string): ParsedQs {
  return parseQueryString(url.includes('?') ? url.split('?', 2)[1] : '');
}

export function toQueryString(query: unknown): string {
  return qs.stringify(query, queryStringOptions);
}
