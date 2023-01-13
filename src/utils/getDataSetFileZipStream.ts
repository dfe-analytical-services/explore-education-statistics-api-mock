import archiver from 'archiver';
import { createReadStream } from 'fs';
import { Transform } from 'stream';
import * as zlib from 'zlib';
import getDataSetDir from './getDataSetDir';

export default async function getDataSetFileZipStream(
  dataSetId: string
): Promise<Transform> {
  const dataSetDir = getDataSetDir(dataSetId);
  const csvStream = createReadStream(`${dataSetDir}/data.csv.gz`);

  const zip = archiver('zip');

  zip.append(csvStream.pipe(zlib.createGunzip()), {
    name: 'data.csv',
  });

  zip.on('error', (err) => {
    throw err;
  });

  zip.finalize();

  return zip;
}
