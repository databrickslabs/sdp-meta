-- Unqualified Gold-to-Gold reference: SDP derives this dependency from SQL.
CREATE OR REFRESH MATERIALIZED VIEW high_value_customers
COMMENT 'Customers with at least five recorded transactions'
AS
SELECT *
FROM customer_360
WHERE transaction_count >= 5;
