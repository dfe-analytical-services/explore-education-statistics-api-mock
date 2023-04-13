import { DataSetVersionViewModel } from '../schema';
import { allDataSets, spcEthnicityLanguageDataSet } from './dataSets';

export const spcEthnicityLanguageDataSetVersions = createVersions({
  dataSetId: spcEthnicityLanguageDataSet.id,
  versions: [
    {
      number: '2.0',
      type: 'Major',
      notes:
        '<p>Addition of 2021/22 data and backwards incompatible changes due to deletion of some filter items and locations.</p>',
      published: '2021-04-01T12:00:00Z',
      changes: [
        {
          id: '0a620537-d02f-44de-b3fd-ec8b85f71787',
          type: 'Delete',
          targetType: 'FilterItem',
          previous: {
            id: 'zZ6RonLA',
            label: 'Primary school',
            filterName: 'phase_type_grouping',
          },
        },
        {
          id: '4e2472f3-e7b1-42e6-96e6-e5164339892b',
          type: 'Delete',
          targetType: 'Location',
          previous: {
            id: 'pjW8Znwx',
            name: 'Dorset county',
            code: 'E10000009',
            level: 'LocalAuthority',
          },
        },
        {
          id: 'cf63f09a-40fa-4fb1-8b1b-be98cb436b41',
          type: 'Add',
          targetType: 'TimePeriod',
          current: {
            code: 'AY',
            year: 202122,
            label: '2021/22',
          },
        },
      ],
    },
    {
      number: '1.1',
      type: 'Minor',
      notes: `<p>Addition of 2020/21 data, minor changes to various filters and filter items, and addition of 'percent_of_pupils' indicator.</p>`,
      published: '2020-04-01T12:00:00Z',
      changes: [
        {
          id: 'b16780a3-45d5-467f-b63a-114986d758b3',
          type: 'Update',
          targetType: 'FilterItem',
          current: {
            id: 'ZjneandK',
            label: 'Asian - Any other Asian background',
            filterName: 'ethnicity',
          },
          previous: {
            label: 'Asian - Any other',
          },
        },
        {
          id: '42b75774-448a-405c-b58b-3288f1ce17b1',
          type: 'Add',
          targetType: 'FilterItem',
          current: {
            id: '8Wn2gB9y',
            label: 'Any other ethnic group',
            filterName: 'ethnicity',
          },
        },
        {
          id: '70231ef3-529e-4b16-ae04-91e13f8188f2',
          type: 'Update',
          targetType: 'Filter',
          current: {
            label: 'School type',
            name: 'phase_type_grouping',
          },
          previous: {
            label: 'Phase type grouping',
          },
        },
        {
          id: 'ee7da221-b7eb-4887-b1be-721edb1a4189',
          type: 'Add',
          targetType: 'TimePeriod',
          current: {
            code: 'AY',
            year: 202021,
            label: '2020/21',
          },
        },
        {
          id: '48ae31b6-8ec2-45d2-8d6e-5df8c067ab8c',
          type: 'Add',
          targetType: 'Indicator',
          current: {
            id: 'N32Zj2Xv',
            name: 'percent_of_pupils',
            label: 'Percent',
            unit: '',
          },
        },
      ],
    },
    {
      number: '1.0',
      type: 'Major',
      notes: '<p>Initial version</p>',
      published: '2019-04-01T12:00:00Z',
    },
  ],
});

const dataSetVersions = {
  [spcEthnicityLanguageDataSet.id]: spcEthnicityLanguageDataSetVersions,
};

export const allDataSetVersions = allDataSets.reduce<
  Dictionary<DataSetVersionViewModel[]>
>((acc, dataSet) => {
  if (dataSetVersions[dataSet.id]) {
    acc[dataSet.id] = dataSetVersions[dataSet.id];
  } else {
    acc[dataSet.id] = createVersions({
      dataSetId: dataSet.id,
      versions: [
        {
          number: '1.0',
          type: 'Major',
          notes: '<p>Initial version</p>',
          published: '2023-04-01T12:00:00Z',
        },
      ],
    });
  }

  return acc;
}, {});

function createVersions({
  dataSetId,
  versions,
}: {
  dataSetId: string;
  versions: Omit<DataSetVersionViewModel, '_links'>[];
}): DataSetVersionViewModel[] {
  return versions.map((version) => {
    return {
      _links: {
        self: {
          href: `/api/v1/data-sets/${dataSetId}/versions/${version.number}`,
        },
        query: {
          href: `/api/v1/data-sets/${dataSetId}/query?dataSetVersion=${version.number}`,
        },
      },
      ...version,
    };
  });
}
