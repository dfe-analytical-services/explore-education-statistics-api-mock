import archiver from 'archiver';
import { createReadStream } from 'fs';
import { Transform } from 'stream';
import * as zlib from 'zlib';
import { DataSetMetaViewModel } from '../schema';
import getDataSetDir from './getDataSetDir';

export async function getDataSetCsvFileStream(
  dataSetId: string,
): Promise<Transform> {
  const dataSetDir = getDataSetDir(dataSetId);
  return createReadStream(`${dataSetDir}/data.csv.gz`).pipe(
    zlib.createGunzip(),
  );
}

export async function getDataSetZipFileStream(
  dataSetId: string,
  dataSetMeta: Omit<DataSetMetaViewModel, '_links'>,
): Promise<Transform> {
  const csvStream = await getDataSetCsvFileStream(dataSetId);

  const zip = archiver('zip');

  zip.append(csvStream, {
    name: 'data.csv',
  });
  // TODO: Decide what metadata. Should these be csv e.g. locations.csv, indicators.csv, etc?
  zip.append(JSON.stringify(dataSetMeta, null, 2), {
    name: 'meta.json',
  });

  zip.on('error', (err) => {
    throw err;
  });

  zip.finalize();

  return zip;
}
