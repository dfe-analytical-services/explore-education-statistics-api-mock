import http from 'k6/http';

export default function run() {
  http.post(
    `${__ENV.BASE_URL}/api/v1/data-sets/a96044e5-2310-4890-a601-8ca0b67d2964/query?page=40`,
    JSON.stringify({
      facets: {},
      indicators: [
        'AppAdvancedPlusPercent',
        'ProgressionSustainedPriorPercent',
        'SPDPercent',
        '75thPercentile',
      ],
    }),
    {
      headers: {
        'Content-Type': 'application/json',
      },
    },
  );
}
