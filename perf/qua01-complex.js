import http from 'k6/http';

export default function run() {
  http.post(
    'https://ees-api-mock-2.niceocean-106d7a86.uksouth.azurecontainerapps.io/api/v1/data-sets/a96044e5-2310-4890-a601-8ca0b67d2964/query?page=40',
    JSON.stringify({
      facets: {
        and: [
          {
            locations: {
              eq: 'DbvOGvdY',
            },
            geographicLevels: {
              eq: 'Country',
            },
            timePeriods: {
              gte: {
                code: 'AY',
                year: 201617,
              },
            },
          },
          {
            or: [
              {
                filters: {
                  in: ['jAZEyZOq', '8EW5gZY7', 'kXWl1GJd'],
                  notIn: ['yOW2wGoL'],
                },
              },
              {
                filters: {
                  notEq: 'RpGmAG2X',
                  in: ['0j4ejW6E', 'qv4xd49B'],
                },
              },
            ],
          },
        ],
      },
      indicators: [
        'AppAdvancedPlusPercent',
        'ProgressionSustainedPriorPercent',
        'SPDPercent',
        '75thPercentile',
      ],
      sort: [
        {
          name: 'TimePeriod',
          order: 'Asc',
        },
        {
          name: 'SSATier2',
          order: 'Desc',
        },
      ],
    }),
    {
      headers: {
        'Content-Type': 'application/json',
      },
    },
  );
}
