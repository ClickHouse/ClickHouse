-- Test filtering with multiple conditions (coalesce not yet supported)
SELECT name, category, price FROM products WHERE stock > 0 AND price > 0
