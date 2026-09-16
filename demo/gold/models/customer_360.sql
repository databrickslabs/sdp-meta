-- Customer activity derived from the shared Silver retail tables.
CREATE TEMPORARY VIEW customer_activity AS
SELECT
  customer_id,
  COUNT(transaction_id) AS transaction_count,
  MAX(transaction_date) AS last_transaction_date
FROM ${silver_catalog}.${silver_schema}.transactions
GROUP BY customer_id;

CREATE OR REFRESH MATERIALIZED VIEW customer_360
COMMENT 'Customer profile enriched with transaction activity'
AS
SELECT
  customers.customer_id,
  customers.email,
  COALESCE(customer_activity.transaction_count, 0) AS transaction_count,
  customer_activity.last_transaction_date
FROM ${silver_catalog}.${silver_schema}.customers AS customers
LEFT JOIN customer_activity
  ON customer_activity.customer_id = customers.customer_id;
