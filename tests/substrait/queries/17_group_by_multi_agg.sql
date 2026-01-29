SELECT category, sum(price) AS total, avg(stock) AS avg_stock FROM products GROUP BY category
