-- Test conditional filtering (CASE not yet supported)
SELECT name, price FROM products WHERE (price > 200) OR (price > 100 AND price <= 200) OR (price <= 100)
