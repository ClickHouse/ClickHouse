-- Test string pattern matching with LIKE predicates
SELECT name, category FROM products WHERE category LIKE 'Elec%' OR category LIKE 'Home%'
