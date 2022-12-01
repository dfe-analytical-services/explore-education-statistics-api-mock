import Hashids from 'hashids';

export default function parseIdHashes(
  ids: string[],
  idHasher: Hashids
): number[] {
  return ids.map((id) => {
    try {
      return idHasher.decode(id)[0] as number;
    } catch (err) {
      return Number.NaN;
    }
  });
}
