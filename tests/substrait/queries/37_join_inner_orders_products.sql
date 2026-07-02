-- PLAN_ONLY
SELECT o.order_id, p.name, o.quantity
FROM orders o
INNER JOIN products p ON o.product_idx = p.idx
LIMIT 20
SETTINGS enable_join_runtime_filters = 0
