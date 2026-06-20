-- Test pagination with filter (ORDER BY causes JoinLateralStep in some plans)
SELECT idx, name, price, stock FROM products WHERE price > 50 LIMIT 10 OFFSET 5
