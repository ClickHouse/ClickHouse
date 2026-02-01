-- Test string column selection with filter (LIKE not yet supported)
SELECT name, category FROM products WHERE category = 'Electronics' OR category = 'Home'
