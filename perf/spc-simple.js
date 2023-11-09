import http from 'k6/http';

export default function run() {
  http.post(
    `${__ENV.BASE_URL}/api/v1/data-sets/9eee125b-5538-49b8-aa49-4fda877b5e57/query?page=11`,
    JSON.stringify({
      facets: {},
      indicators: ['headcount', 'percent_of_pupils'],
    }),
    {
      headers: {
        'Content-Type': 'application/json',
      },
    },
  );
}
