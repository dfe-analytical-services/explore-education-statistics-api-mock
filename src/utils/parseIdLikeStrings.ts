import Hashids from 'hashids';
import { compact } from 'lodash';

export function parseIdLikeStrings(ids: string[], idHasher: Hashids): string[] {
  return compact(
    ids.map((id) => {
      try {
        return idHasher.decode(id)[0].toString();
      } catch (err) {
        // If the id is NaN, then allow this as it could be a
        // code or other identifier that can be used instead.
        // Plain numbers shouldn't be accepted to avoid
        return Number.isNaN(Number(id)) ? id : '';
      }
    })
  );
}
