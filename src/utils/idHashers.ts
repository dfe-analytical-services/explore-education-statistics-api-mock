import Hashids from 'hashids';
import { NumberLike } from 'hashids/src/util';
import NodeCache from 'node-cache';
import path from 'path';

const locationEncodedIdCache = new NodeCache();
const locationDecodedIdCache = new NodeCache();

const filterEncodedIdCache = new NodeCache();
const filterDecodedIdCache = new NodeCache();

const indicatorEncodedIdCache = new NodeCache();
const indicatorDecodedIdCache = new NodeCache();

export class IdHasher {
  private encodedIdCache: NodeCache;
  private decodedIdCache: NodeCache;
  private hashids: Hashids;

  constructor(
    salt: string,
    encodedIdCache: NodeCache,
    decodedIdCache: NodeCache
  ) {
    this.encodedIdCache = encodedIdCache;
    this.decodedIdCache = decodedIdCache;
    this.hashids = new Hashids(salt, 8);
  }

  encode(id: NumberLike): string {
    const key = id.toString();
    const cachedId = this.encodedIdCache.get<string>(key);

    if (cachedId !== undefined) {
      return cachedId;
    }

    const encodedId = this.hashids.encode(id);

    this.encodedIdCache.set(key, encodedId);

    return encodedId;
  }

  decode(id: string): NumberLike {
    const key = id.toString();
    const cachedId = this.decodedIdCache.get<NumberLike>(key);

    if (cachedId !== undefined) {
      return cachedId;
    }

    const decodedId = this.hashids.decode(id)[0];

    this.encodedIdCache.set(key, decodedId);

    return decodedId;
  }
}

// NOTE - These hashers use the data set directory name as a salt,
// but they could really use anything e.g. the data set id.

export function createLocationIdHasher(dataSetDir: string): IdHasher {
  return new IdHasher(
    `${path.basename(dataSetDir)}/locations`,
    locationEncodedIdCache,
    locationDecodedIdCache
  );
}

export function createFilterIdHasher(dataSetDir: string): IdHasher {
  return new IdHasher(
    `${path.basename(dataSetDir)}/filters`,
    filterEncodedIdCache,
    filterDecodedIdCache
  );
}

export function createIndicatorIdHasher(dataSetDir: string): IdHasher {
  return new IdHasher(
    `${path.basename(dataSetDir)}/indicators`,
    indicatorEncodedIdCache,
    indicatorDecodedIdCache
  );
}
