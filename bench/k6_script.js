// go-storm benchmark harness — scenario selector via env SCEN
import http from 'k6/http';

const URL = 'http://172.31.33.43:8080/';

const scenarios = {
  s2: {
    executor: 'constant-vus',
    vus: 100,
    duration: '60s',
    gracefulStop: '0s',
  },
  s3: {
    executor: 'constant-arrival-rate',
    rate: 5000,
    timeUnit: '1s',
    duration: '30s',
    preAllocatedVUs: 150,
    maxVUs: 300,
    gracefulStop: '0s',
  },
  s4: {
    executor: 'constant-vus',
    vus: 1000,
    duration: '30s',
    gracefulStop: '0s',
  },
};

export const options = {
  scenarios: {
    main: scenarios[__ENV.SCEN || 's2'],
  },
  thresholds: {},
  summaryTrendStats: ['avg', 'med', 'p(50)', 'p(95)', 'p(99)'],
};

export default function () {
  http.get(URL);
}
