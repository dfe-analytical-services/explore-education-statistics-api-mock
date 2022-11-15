import Hashids from 'hashids';
import path from 'path';

// NOTE - These hashers use the data set directory name as a salt,
// but they could really use anything e.g. the data set id.

export function createLocationIdHasher(dataSetDir: string): Hashids {
  return new Hashids(`${path.basename(dataSetDir)}/locations`, 8);
}

export function createFilterIdHasher(dataSetDir: string): Hashids {
  return new Hashids(`${path.basename(dataSetDir)}/filters`, 8);
}

export function createIndicatorIdHasher(dataSetDir: string): Hashids {
  return new Hashids(`${path.basename(dataSetDir)}/indicators`, 8);
}
