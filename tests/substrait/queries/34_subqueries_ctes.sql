SELECT sub.category, max(sub.price) FROM (SELECT * FROM products WHERE stock > 0) AS sub GROUP BY sub.category;
WITH high_stock AS (SELECT * FROM products WHERE stock > 100) SELECT name, price FROM high_stock WHERE price < 50;
