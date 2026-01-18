SELECT category, sum(price * stock) AS valuation FROM products GROUP BY category
