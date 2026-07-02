-- Test equivalent boolean branches across price ranges
SELECT name, price FROM products WHERE (price > 200) OR (price > 100 AND price <= 200) OR (price <= 100)
