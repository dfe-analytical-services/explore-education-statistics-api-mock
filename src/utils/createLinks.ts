import { mapValues } from 'lodash';
import { ParsedQs } from 'qs';
import { LinksViewModel } from '../schema';
import createPaginationLinks from './createPaginationLinks';

interface Options {
  self: {
    url: string;
    method: string;
  };
  paging?: {
    query: ParsedQs;
    page: number;
    totalPages: number;
  };
  links?: LinksViewModel;
}

export default function createLinks(options: Options): LinksViewModel {
  const { self, paging } = options;
  const baseUrl = new URL(self.url).origin;

  const links = mapValues(options.links, (link) => {
    return {
      ...link,
      href: `${baseUrl}${link.href}`,
    };
  });

  const paginationLinks = paging
    ? createPaginationLinks({
        paging,
        self,
      })
    : {};

  return {
    self:
      self.method !== 'GET'
        ? {
            href: self.url,
          }
        : {
            href: self.url,
            method: self.method,
          },
    ...paginationLinks,
    ...links,
  };
}
