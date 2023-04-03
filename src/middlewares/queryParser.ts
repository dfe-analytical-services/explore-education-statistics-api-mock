import { RequestHandler } from 'express';
import qs from 'qs';

export default function queryParser(): RequestHandler {
  return (req, res, next) => {
    const [, query] = req.url.split('?', 2);

    req.query = qs.parse(query, {
      allowDots: true,
      comma: true,
    });

    next();
  };
}
