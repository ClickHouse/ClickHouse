SELECT category, currency, sum(price) AS total FROM products GROUP BY category, currency
