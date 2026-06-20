-- PLAN_ONLY
SELECT c.customer_id, o.order_id
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id AND c.region = o.region
LIMIT 20
SETTINGS enable_join_runtime_filters = 0
