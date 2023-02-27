import { DataSetViewModel, PublicationSummaryViewModel } from '../schema';
import {
  apprenticeshipsPublication,
  benchmarkPublication,
  leoPublication,
  pupilAbsencePublication,
  spcPublication,
} from './publications';

export const spcEthnicityLanguageDataSet = createDataSet({
  id: '9eee125b-5538-49b8-aa49-4fda877b5e57',
  content:
    'Number of pupils in state-funded nursery, primary, secondary and special schools, non-maintained special schools and pupil referral units by language and ethnicity.',
  geographicLevels: ['National', 'Local authority', 'Regional'],
  name: 'Pupil characteristics - Ethnicity and Language',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: spcPublication,
  timePeriods: {
    start: '2015/16',
    end: '2021/22',
  },
});

export const spcYearGroupGenderDataSet = createDataSet({
  id: 'c5292537-e29a-4dba-a361-8363d2fb08f1',
  content:
    'Number of pupils in state-funded nursery, primary, secondary and special schools, non-maintained special schools, pupil referral units and independent schools by national curriculum year and gender.',
  geographicLevels: ['National', 'Local authority', 'Regional'],
  name: 'Pupil characteristics - Year group and Gender',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: spcPublication,
  timePeriods: {
    start: '2015/16',
    end: '2021/22',
  },
});

export const pupilAttendanceWeeklyDataSet = createDataSet({
  id: '14f4e111-506c-4bb9-86ff-6d4923acdd07',
  content:
    'Weekly local authority, regional and national attendance since 12 September 2022, including reasons for absence. Figures are provided for state-funded primary, secondary and special schools. Totals for all schools are also included that include estimates for non-response.',
  geographicLevels: ['National', 'Local authority', 'Regional'],
  name: 'Pupil attendance since week commencing 12 September - weekly',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: pupilAbsencePublication,
  timePeriods: {
    start: '2022 Week 37',
    end: '2022 Week 49',
  },
});

export const leoIndustryRegionalDataSet = createDataSet({
  id: '81cb8865-a00b-4a35-a2bc-ea8aa1502856',
  content:
    'Graduate populations of UK domiciled graduates of English Higher Education Institutions (HEIs), Alternative Providers (APs) and Further Education Colleges (FECs), one, three, five and ten years after graduation (YAG), 2019/20 tax year',
  geographicLevels: ['Regional'],
  name: 'Industry data - regional',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: leoPublication,
  timePeriods: {
    start: '2019-20',
    end: '2019-20',
  },
});

export const apprenticeshipsProvidersDetailedDataSet = createDataSet({
  id: 'e838e8da-8b1f-4eb5-8e86-0d7c57bc6f7c',
  content:
    'Breakdowns of apprenticeship starts and achievements by individual provider',
  geographicLevels: ['Provider'],
  name: 'Provider - latest detailed series',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: apprenticeshipsPublication,
  timePeriods: {
    start: '2016/17',
    end: '2021/22',
  },
});

export const apprenticeshipsSubjectLevelsHeadlineDataSet = createDataSet({
  id: '17f1f8e9-6167-417f-93f3-0882ef37377f',
  content:
    'Headline summary of apprenticeship starts and achievements by sector subject area',
  geographicLevels: ['National'],
  name: 'Subjects and levels - latest headline summary',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: apprenticeshipsPublication,
  timePeriods: {
    start: '2018/19',
    end: '2021/22',
  },
});

export const benchmarkETDetailedReorderedDataSet = createDataSet({
  id: '91f449b6-0850-45ff-8e09-23d5fdc87fb5',
  content: '',
  geographicLevels: ['National', 'Local authority', 'Regional'],
  name: 'ET Detailed Reordered',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
  timePeriods: {
    start: '2015/16',
    end: '2021/22',
  },
});

export const benchmarkQuaDataSet = createDataSet({
  id: 'a96044e5-2310-4890-a601-8ca0b67d2964',
  content: '',
  geographicLevels: ['National'],
  name: 'QUA01',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
  timePeriods: {
    start: '2013/14',
    end: '2018/19',
  },
});

export const benchmarkNatDataSet = createDataSet({
  id: '942ea929-05da-4e52-b77c-6cead4afb2f0',
  content: '',
  geographicLevels: ['National'],
  name: 'NAT01',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
  timePeriods: {
    start: '2013/14',
    end: '2018/19',
  },
});

export const benchmarkLtdDmDataSet = createDataSet({
  id: '60849ca0-055d-4144-9ec5-30c100ad2245',
  content: '',
  geographicLevels: ['School'],
  name: 'LTD DM',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
  timePeriods: {
    start: '2014/15',
    end: '2015/15',
  },
});

export const allDataSets: DataSet[] = [
  spcEthnicityLanguageDataSet,
  spcYearGroupGenderDataSet,
  pupilAttendanceWeeklyDataSet,
  leoIndustryRegionalDataSet,
  apprenticeshipsProvidersDetailedDataSet,
  apprenticeshipsSubjectLevelsHeadlineDataSet,
  benchmarkETDetailedReorderedDataSet,
  benchmarkLtdDmDataSet,
  benchmarkNatDataSet,
  benchmarkQuaDataSet,
];

export interface DataSet {
  id: string;
  viewModel: DataSetViewModel;
  publication: PublicationSummaryViewModel;
}

function createDataSet({
  publication,
  ...dataSet
}: Omit<DataSetViewModel, '_links'> & {
  publication: PublicationSummaryViewModel;
}): DataSet {
  return {
    id: dataSet.id,
    publication,
    viewModel: {
      ...dataSet,
      _links: {
        self: {
          href: `/api/v1/data-sets/${dataSet.id}`,
        },
        query: {
          href: `/api/v1/data-sets/${dataSet.id}/query`,
          method: 'POST',
        },
        file: {
          href: `/api/v1/data-sets/${dataSet.id}/file`,
        },
        meta: {
          href: `/api/v1/data-sets/${dataSet.id}/meta`,
        },
      },
    },
  };
}
