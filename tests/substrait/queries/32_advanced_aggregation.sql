SELECT category, sum(price * stock) AS valuation FROM products GROUP BY category HAVING valuation > 5000;
SELECT count(DISTINCT category) AS unique_categories, count(*) AS total_items FROM products;
