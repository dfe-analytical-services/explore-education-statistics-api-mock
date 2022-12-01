import Hashids from 'hashids';
import { compact } from 'lodash';

export default function parseIdHashes(
  ids: string[],
  idHasher: Hashids
): number[] {
  return compact(
    ids.map((id) => {
      try {
        return idHasher.decode(id)[0] as number;
      } catch (err) {
        return Number.NaN;
      }
    })
  );
}
