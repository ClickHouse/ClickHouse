-- PLAN_ONLY
SELECT c.customer_id, c.customer_name, o.order_id
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LIMIT 30
SETTINGS enable_join_runtime_filters = 0
