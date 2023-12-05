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
  title: 'Pupil characteristics - Ethnicity and Language',
  description:
    'Number of pupils in state-funded nursery, primary, secondary and special schools, non-maintained special schools and pupil referral units by language and ethnicity.',
  status: 'Active',
  latestVersion: '2.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: spcPublication,
});

export const spcYearGroupGenderDataSet = createDataSet({
  id: 'c5292537-e29a-4dba-a361-8363d2fb08f1',
  title: 'Pupil characteristics - Year group and Gender',
  description:
    'Number of pupils in state-funded nursery, primary, secondary and special schools, non-maintained special schools, pupil referral units and independent schools by national curriculum year and gender.',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: spcPublication,
});

export const pupilAttendanceWeeklyDataSet = createDataSet({
  id: '14f4e111-506c-4bb9-86ff-6d4923acdd07',
  title: 'Pupil attendance since week commencing 12 September - weekly',
  description:
    'Weekly local authority, regional and national attendance since 12 September 2022, including reasons for absence. Figures are provided for state-funded primary, secondary and special schools. Totals for all schools are also included that include estimates for non-response.',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: pupilAbsencePublication,
});

export const leoIndustryRegionalDataSet = createDataSet({
  id: '81cb8865-a00b-4a35-a2bc-ea8aa1502856',
  title: 'Industry data - regional',
  description:
    'Graduate populations of UK domiciled graduates of English Higher Education Institutions (HEIs), Alternative Providers (APs) and Further Education Colleges (FECs), one, three, five and ten years after graduation (YAG), 2019/20 tax year',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: leoPublication,
});

export const apprenticeshipsProvidersDetailedDataSet = createDataSet({
  id: 'e838e8da-8b1f-4eb5-8e86-0d7c57bc6f7c',
  title: 'Provider - latest detailed series',
  description:
    'Breakdowns of apprenticeship starts and achievements by individual provider',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: apprenticeshipsPublication,
});

export const benchmarkETDetailedReorderedDataSet = createDataSet({
  id: '91f449b6-0850-45ff-8e09-23d5fdc87fb5',
  title: 'ET Detailed Reordered',
  description: '',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
});

export const benchmarkQuaDataSet = createDataSet({
  id: 'a96044e5-2310-4890-a601-8ca0b67d2964',
  title: 'QUA01',
  description: '',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
});

export const benchmarkNatDataSet = createDataSet({
  id: '942ea929-05da-4e52-b77c-6cead4afb2f0',
  title: 'NAT01',
  description: '',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
});

export const benchmarkLtdDmDataSet = createDataSet({
  id: '60849ca0-055d-4144-9ec5-30c100ad2245',
  title: 'LTD DM',
  description: '',
  status: 'Active',
  latestVersion: '1.0',
  lastPublished: '2022-12-01T09:30:00Z',
  publication: benchmarkPublication,
});

export const allDataSets: DataSet[] = [
  spcEthnicityLanguageDataSet,
  spcYearGroupGenderDataSet,
  pupilAttendanceWeeklyDataSet,
  leoIndustryRegionalDataSet,
  apprenticeshipsProvidersDetailedDataSet,
  benchmarkETDetailedReorderedDataSet,
  benchmarkLtdDmDataSet,
  benchmarkNatDataSet,
  benchmarkQuaDataSet,
];

export interface DataSet {
  id: string;
  viewModel: Omit<DataSetViewModel, 'filters' | 'indicators'>;
  publication: PublicationSummaryViewModel;
}

function createDataSet({
  publication,
  ...dataSet
}: Omit<DataSetViewModel, 'filters' | 'indicators' | '_links'> & {
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
        publication: {
          href: `/api/v1/publications/${publication.id}`,
        },
        query: {
          href: `/api/v1/data-sets/${dataSet.id}/query`,
        },
        file: {
          href: `/api/v1/data-sets/${dataSet.id}/file`,
        },
        meta: {
          href: `/api/v1/data-sets/${dataSet.id}/meta`,
        },
        versions: {
          href: `/api/v1/data-sets/${dataSet.id}/versions`,
        },
        latestVersion: {
          href: `/api/v1/data-sets/${dataSet.id}/versions/${dataSet.latestVersion}`,
        },
      },
    },
  };
}
