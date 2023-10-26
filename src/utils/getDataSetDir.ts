import fs from 'node:fs';
import {
  apprenticeshipsProvidersDetailedDataSet,
  benchmarkETDetailedReorderedDataSet,
  benchmarkLtdDmDataSet,
  benchmarkNatDataSet,
  benchmarkQuaDataSet,
  leoIndustryRegionalDataSet,
  pupilAttendanceWeeklyDataSet,
  spcEthnicityLanguageDataSet,
  spcYearGroupGenderDataSet,
} from '../mocks/dataSets';

const mountedDataPath = '/data-files';

const basePath = fs.existsSync(mountedDataPath) ? mountedDataPath : 'data';

export const dataSetDirs = {
  [spcEthnicityLanguageDataSet.id]: 'spc_pupils_ethnicity_and_language',
  [spcYearGroupGenderDataSet.id]: 'spc_pupils_fsm_ethnicity_yrgp',
  [pupilAttendanceWeeklyDataSet.id]: 'ees_weekly_data',
  [leoIndustryRegionalDataSet.id]: 'regional_map_data_ees',
  [apprenticeshipsProvidersDetailedDataSet.id]: 'app-provider-starts-202122-q4',
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
