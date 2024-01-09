import { ChangeViewModel, DataSetVersionViewModel } from '../schema';
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
} from './dataSets';

export type DataSetVersionViewModelMock = Omit<
  DataSetVersionViewModel,
  'totalResults' | 'filters' | 'indicators'
> & {
  changes?: ChangeViewModel[];
};

export const spcEthnicityLanguageDataSetVersions = createVersions({
  dataSetId: spcEthnicityLanguageDataSet.id,
  versions: [
    {
      number: '2.0',
      type: 'Major',
      status: 'Published',
      notes:
        '<p>Addition of 2021/22 data and backwards incompatible changes due to deletion of some filter items and locations.</p>',
      published: '2021-04-01T12:00:00Z',
      geographicLevels: ['National', 'Local authority', 'Regional'],
      timePeriods: {
        start: '2015/16',
        end: '2021/22',
      },
      changes: [
        {
          id: '0a620537-d02f-44de-b3fd-ec8b85f71787',
          type: 'Delete',
          metaType: 'FilterOption',
          previousState: {
            id: 'zZ6RonLA',
            label: 'Primary school',
            filterId: 'phase_type_grouping',
          },
        },
        {
          id: '4e2472f3-e7b1-42e6-96e6-e5164339892b',
          type: 'Delete',
          metaType: 'Location',
          previousState: {
            id: 'pjW8Znwx',
            label: 'Dorset county',
            code: 'E10000009',
            level: 'LocalAuthority',
          },
        },
        {
          id: 'cf63f09a-40fa-4fb1-8b1b-be98cb436b41',
          type: 'Add',
          metaType: 'TimePeriod',
          currentState: {
            code: 'AY',
            year: 202122,
          },
        },
      ],
    },
    {
      number: '1.1',
      type: 'Minor',
      status: 'Published',
      notes: `<p>Addition of 2020/21 data, minor changes to various filters and filter items, and addition of 'percent_of_pupils' indicator.</p>`,
      geographicLevels: ['National', 'Local authority', 'Regional'],
      timePeriods: {
        start: '2015/16',
        end: '2021/22',
      },
      published: '2020-04-01T12:00:00Z',
      changes: [
        {
          id: 'b16780a3-45d5-467f-b63a-114986d758b3',
          type: 'Update',
          metaType: 'FilterOption',
          currentState: {
            id: 'ZjneandK',
            label: 'Asian - Any other Asian background',
            filterId: 'ethnicity',
          },
          previousState: {
            id: 'ZjneandK',
            label: 'Asian - Any other',
            filterId: 'ethnicity',
          },
        },
        {
          id: '42b75774-448a-405c-b58b-3288f1ce17b1',
          type: 'Add',
          metaType: 'FilterOption',
          currentState: {
            id: '8Wn2gB9y',
            label: 'Any other ethnic group',
            filterId: 'ethnicity',
          },
        },
        {
          id: '70231ef3-529e-4b16-ae04-91e13f8188f2',
          type: 'Update',
          metaType: 'Filter',
          currentState: {
            id: 'phase_type_grouping',
            label: 'School type',
          },
          previousState: {
            id: 'phase_type_grouping',
            label: 'Phase type grouping',
          },
        },
        {
          id: 'ee7da221-b7eb-4887-b1be-721edb1a4189',
          type: 'Add',
          metaType: 'TimePeriod',
          currentState: {
            code: 'AY',
            year: 202021,
          },
        },
        {
          id: '48ae31b6-8ec2-45d2-8d6e-5df8c067ab8c',
          type: 'Add',
          metaType: 'Indicator',
          currentState: {
            id: 'percent_of_pupils',
            label: 'Percent',
            unit: '',
          },
        },
      ],
    },
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['National', 'Local authority', 'Regional'],
      timePeriods: {
        start: '2015/16',
        end: '2021/22',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});

export const spcYearGroupGenderDataSetVersions = createVersions({
  dataSetId: spcYearGroupGenderDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['National', 'Local authority', 'Regional'],
      timePeriods: {
        start: '2015/16',
        end: '2021/22',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});
export const pupilAttendanceWeeklyDataSetVersions = createVersions({
  dataSetId: pupilAttendanceWeeklyDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['National', 'Local authority', 'Regional'],
      timePeriods: {
        start: '2022 Week 37',
        end: '2022 Week 49',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});
export const leoIndustryRegionalDataSetVersions = createVersions({
  dataSetId: leoIndustryRegionalDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['Regional'],
      timePeriods: {
        start: '2019-20',
        end: '2019-20',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});
export const apprenticeshipsProvidersDetailedDataSetVersions = createVersions({
  dataSetId: apprenticeshipsProvidersDetailedDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['Provider'],
      timePeriods: {
        start: '2016/17',
        end: '2021/22',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});
export const benchmarkETDetailedReorderedDataSetVersions = createVersions({
  dataSetId: benchmarkETDetailedReorderedDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['National', 'Local authority', 'Regional'],
      timePeriods: {
        start: '2015/16',
        end: '2021/22',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});
export const benchmarkLtdDmDataSetVersions = createVersions({
  dataSetId: benchmarkLtdDmDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['National'],
      timePeriods: {
        start: '2013/14',
        end: '2018/19',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});
export const benchmarkNatDataSetVersions = createVersions({
  dataSetId: benchmarkNatDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['National'],
      timePeriods: {
        start: '2013/14',
        end: '2018/19',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});
export const benchmarkQuaDataSetVersions = createVersions({
  dataSetId: benchmarkQuaDataSet.id,
  versions: [
    {
      number: '1.0',
      type: 'Major',
      status: 'Published',
      notes: '<p>Initial version</p>',
      geographicLevels: ['School'],
      timePeriods: {
        start: '2014/15',
        end: '2015/15',
      },
      published: '2019-04-01T12:00:00Z',
    },
  ],
});

export const allDataSetVersions = {
  [spcEthnicityLanguageDataSet.id]: spcEthnicityLanguageDataSetVersions,
  [spcYearGroupGenderDataSet.id]: spcYearGroupGenderDataSetVersions,
  [pupilAttendanceWeeklyDataSet.id]: pupilAttendanceWeeklyDataSetVersions,
  [leoIndustryRegionalDataSet.id]: leoIndustryRegionalDataSetVersions,
  [apprenticeshipsProvidersDetailedDataSet.id]:
    apprenticeshipsProvidersDetailedDataSetVersions,
  [benchmarkETDetailedReorderedDataSet.id]:
    benchmarkETDetailedReorderedDataSetVersions,
  [benchmarkLtdDmDataSet.id]: benchmarkLtdDmDataSetVersions,
  [benchmarkNatDataSet.id]: benchmarkNatDataSetVersions,
  [benchmarkQuaDataSet.id]: benchmarkQuaDataSetVersions,
};

function createVersions({
  dataSetId,
  versions,
}: {
  dataSetId: string;
  versions: Omit<DataSetVersionViewModelMock, '_links'>[];
}): DataSetVersionViewModelMock[] {
  return versions.map((version) => {
    return {
      _links: {
        self: {
          href: `/api/v1/data-sets/${dataSetId}/versions/${version.number}`,
        },
        changes: {
          href: `/api/v1/data-sets/${dataSetId}/versions/${version.number}/changes`,
        },
        dataSet: {
          href: `/api/v1/data-sets/${dataSetId}`,
        },
        query: {
          href: `/api/v1/data-sets/${dataSetId}/query?dataSetVersion=${version.number}`,
        },
      },
      ...version,
    };
  });
}
