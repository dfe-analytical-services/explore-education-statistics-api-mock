import http from 'k6/http';

export default function run() {
  http.post(
    `${__ENV.BASE_URL}/api/v1/data-sets/a96044e5-2310-4890-a601-8ca0b67d2964/query?page=40`,
    JSON.stringify({
      facets: {
        and: [
          {
            locations: {
              // England
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
                  // Access to HE - Total
                  // Age band - 18 and under
                  // Age band - 19 - 24
                  in: ['jAZEyZOq', '8EW5gZY7', 'kXWl1GJd'],
                  notIn: ['yOW2wGoL'],
                },
              },
              {
                filters: {
                  // Access to HE - Total
                  notIn: ['jAZEyZOq'],
                  // Age band - 25 - 49
                  // Age band - 50 and over
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
