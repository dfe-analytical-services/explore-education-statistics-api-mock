import { Request } from 'express';

export function getHostUrl(req: Request) {
  const host = req.get('host');
  return host ? `${req.protocol}://${host}` : '';
}

export function getFullRequestUrl(req: Request) {
  const host = getHostUrl(req);
  return `${host}${req.originalUrl}`;
}

export function getFullRequestPath(req: Request) {
  const host = getHostUrl(req);
  return `${host}${req.path}`;
}
