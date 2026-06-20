-- Test combined filter and aggregation
SELECT category, max(price) AS max_price FROM products WHERE stock > 0 GROUP BY category
