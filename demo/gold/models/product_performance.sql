-- Product metrics derived from the shared Silver product and transaction tables.
CREATE OR REFRESH MATERIALIZED VIEW product_performance
COMMENT 'Transaction activity and estimated revenue by product'
AS
SELECT
  products.product_id,
  products.name AS product_name,
  products.price,
  COUNT(transactions.transaction_id) AS transaction_count,
  COUNT(DISTINCT transactions.customer_id) AS customer_count,
  COUNT(DISTINCT transactions.store_id) AS store_count,
  COALESCE(SUM(products.price), 0) AS estimated_revenue
FROM ${silver_catalog}.${silver_schema}.products AS products
LEFT JOIN ${silver_catalog}.${silver_schema}.transactions AS transactions
  ON transactions.product_id = products.product_id
GROUP BY products.product_id, products.name, products.price;
