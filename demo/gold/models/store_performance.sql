-- Store metrics combine the shared Silver stores, transactions, and products.
CREATE OR REFRESH MATERIALIZED VIEW store_performance
COMMENT 'Transaction activity and estimated revenue by store'
AS
SELECT
  stores.store_id,
  stores.address,
  COUNT(transactions.transaction_id) AS transaction_count,
  COUNT(DISTINCT transactions.customer_id) AS customer_count,
  COUNT(DISTINCT transactions.product_id) AS product_count,
  COALESCE(SUM(products.price), 0) AS estimated_revenue
FROM ${silver_catalog}.${silver_schema}.stores AS stores
LEFT JOIN ${silver_catalog}.${silver_schema}.transactions AS transactions
  ON transactions.store_id = stores.store_id
LEFT JOIN ${silver_catalog}.${silver_schema}.products AS products
  ON products.product_id = transactions.product_id
GROUP BY stores.store_id, stores.address;
