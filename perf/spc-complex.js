import http from 'k6/http';

export default function run() {
  http.post(
    'https://ees-api-mock-2.niceocean-106d7a86.uksouth.azurecontainerapps.io/api/v1/data-sets/9eee125b-5538-49b8-aa49-4fda877b5e57/query?page=11',
    JSON.stringify({
      facets: {
        and: [
          {},
          {
            or: [],
          },
        ],
      },
      indicators: ['headcount', 'percent_of_pupils'],
    }),
    {
      headers: {
        'Content-Type': 'application/json',
      },
    },
  );
}
