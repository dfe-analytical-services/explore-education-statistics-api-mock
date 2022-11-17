import { Request } from 'express';
import { LinkViewModel } from '../schema';
import { getFullRequestUrl } from './requestUtils';

export default function createSelfLink(req: Request): LinkViewModel {
  const href = getFullRequestUrl(req);
  const method = req.method !== 'GET' ? req.method : undefined;

  return method ? { href, method } : { href };
}
