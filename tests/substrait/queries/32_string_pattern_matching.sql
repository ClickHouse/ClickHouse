SELECT name, length(name) as name_len FROM products WHERE name LIKE 'A%';
SELECT * FROM products WHERE category IN ('Electronics', 'Home', 'Garden');
