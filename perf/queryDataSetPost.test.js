import http from 'k6/http';

export default function run() {
  // http.get(
  //   'https://ees-api-mock-2.niceocean-106d7a86.uksouth.azurecontainerapps.io/api/v1/data-sets/9eee125b-5538-49b8-aa49-4fda877b5e57/query?indicators=headcount,percent_of_pupils&page=6&pageSize=1000',
  // );

  http.get(
    'http://localhost:8080/api/v1/data-sets/9eee125b-5538-49b8-aa49-4fda877b5e57/query?indicators=headcount,percent_of_pupils&page=6&pageSize=200',
  );
}
