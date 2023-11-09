import http from 'k6/http';

export default function run() {
  http.post(
    `${__ENV.BASE_URL}/api/v1/data-sets/9eee125b-5538-49b8-aa49-4fda877b5e57/query?page=11`,
    JSON.stringify({
      facets: {
        and: [
          {
            filters: {
              // School type - Primary
              // School type - Secondary
              in: ['4W67lOR9', 'X36rmBGR'],
            },
            geographicLevels: {
              eq: 'LocalAuthority',
            },
            locations: {
              // London
              // South East
              // South West
              notIn: ['p9W2g6Jo', 'y26nMWVY', 'pjW8E6xl'],
            },
            timePeriods: {
              gte: {
                code: 'AY',
                year: 201718,
              },
            },
          },
          {
            or: [
              {
                filters: {
                  // First language - Known or believed to be English
                  // First language - Known or believed to be other than English
                  in: ['oE6DL6Yg', 'lROd4Owj'],
                },
              },
              {
                filters: {
                  // Ethnicity - Any other ethnic group
                  // Ethnicity - Total
                  // Ethnicity - Unclassified
                  notIn: ['8Wn2gB9y', 'gAn05BWR', 'EV6oenjk'],
                },
              },
            ],
          },
        ],
      },
      indicators: ['headcount', 'percent_of_pupils'],
      sort: [
        {
          name: 'TimePeriod',
          order: 'Asc',
        },
        {
          name: 'ethnicity',
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
