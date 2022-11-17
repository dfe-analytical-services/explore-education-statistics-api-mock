import path from 'path';
import {
  benchmarkETDetailedReorderedDataSet,
  benchmarkLtdDmDataSet,
  benchmarkNatDataSet,
  benchmarkQuaDataSet,
  spcEthnicityLanguageDataSet,
  spcYearGroupGenderDataSet,
} from '../mocks/dataSets';

const basePath = path.resolve(__dirname, '../data');

export const dataSetDirs = {
  [spcEthnicityLanguageDataSet.id]: 'spc_pupils_ethnicity_and_language',
  [spcYearGroupGenderDataSet.id]: 'spc_pupils_fsm_ethnicity_yrgp',
  [benchmarkETDetailedReorderedDataSet.id]:
    'e-and-t-geography-detailed_6years_reordered',
  [benchmarkQuaDataSet.id]: 'qua01',
  [benchmarkNatDataSet.id]: 'nat01',
  [benchmarkLtdDmDataSet.id]: 'ltd_dm_201415_inst',
};

export default function getDataSetDir(dataSetId: string) {
  const dataSetDir = dataSetDirs[dataSetId];

  if (!dataSetDir) {
    throw new Error(`No data set for this id: ${dataSetId}`);
  }

  return `${basePath}/${dataSetDir}`;
}
