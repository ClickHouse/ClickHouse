-- PLAN_ONLY
SELECT c.customer_id, o.order_id FROM customers c FULL JOIN orders o ON c.customer_id = o.customer_id LIMIT 30
