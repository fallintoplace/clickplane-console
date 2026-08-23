CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS commerce;

CREATE TABLE IF NOT EXISTS analytics.http_requests
(
  timestamp DateTime64(3),
  service LowCardinality(String),
  route String,
  status_code UInt16,
  duration_ms Float32
)
ENGINE = MergeTree
ORDER BY (timestamp, service, route);

CREATE TABLE IF NOT EXISTS analytics.errors
(
  timestamp DateTime64(3),
  service LowCardinality(String),
  route String,
  status_code UInt16,
  duration_ms Float32,
  error_type LowCardinality(String),
  message String
)
ENGINE = MergeTree
ORDER BY (timestamp, service, error_type);

CREATE TABLE IF NOT EXISTS commerce.orders
(
  created_at DateTime,
  customer_id UUID,
  region LowCardinality(String),
  revenue Decimal(18, 2),
  status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (created_at, region);

INSERT INTO analytics.http_requests
SELECT
  now64(3) - toIntervalSecond(number),
  if(number % 3 = 0, 'checkout-api', if(number % 3 = 1, 'catalog-api', 'identity-api')),
  if(number % 2 = 0, '/checkout', '/payment'),
  if(number % 5 = 0, 500, 200),
  60 + (number % 11) * 17
FROM numbers(120)
WHERE NOT EXISTS (SELECT 1 FROM analytics.http_requests LIMIT 1);

INSERT INTO analytics.errors
SELECT
  now64(3) - toIntervalSecond(number),
  if(number < 18, 'checkout-api', 'identity-api'),
  if(number < 18, '/payment', '/session'),
  if(number < 18, 500, 401),
  if(number < 18, 220 + (number % 9), 50 + (number % 4)),
  if(number < 18, 'payment_timeout', 'invalid_session'),
  if(number < 18, 'Payment provider did not respond in time', 'Session token was rejected')
FROM numbers(27)
WHERE NOT EXISTS (SELECT 1 FROM analytics.errors LIMIT 1);

INSERT INTO commerce.orders
SELECT
  now() - toIntervalHour(number),
  generateUUIDv4(),
  arrayElement(['DACH', 'Nordics', 'Benelux'], (number % 3) + 1),
  toDecimal64(100 + number * 17.5, 2),
  'paid'
FROM numbers(12)
WHERE NOT EXISTS (SELECT 1 FROM commerce.orders LIMIT 1);
