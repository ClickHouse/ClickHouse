SELECT name, price, CASE WHEN price > 200 THEN 'Premium' WHEN price > 100 THEN 'Standard' ELSE 'Budget' END AS price_range FROM products;
