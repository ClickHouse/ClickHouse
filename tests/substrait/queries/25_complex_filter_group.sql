SELECT category, sum(stock) AS total_stock FROM products WHERE price > 100 GROUP BY category
