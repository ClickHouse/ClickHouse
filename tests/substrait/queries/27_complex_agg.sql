SELECT category, count(*) AS product_count, sum(stock) AS total_stock, avg(price) AS avg_price FROM products WHERE stock > 100 GROUP BY category
