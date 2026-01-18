SELECT * FROM products WHERE category IS NOT NULL AND price IS NULL;
SELECT name, coalesce(category, 'Uncategorized') FROM products;
