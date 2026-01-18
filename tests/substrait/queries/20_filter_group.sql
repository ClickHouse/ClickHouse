SELECT category, count(*) AS cnt FROM products WHERE stock > 100 GROUP BY category
