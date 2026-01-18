SELECT c.FirstName, c.LastName, l.Source FROM customers AS c JOIN leads AS l ON c.Customer_Id = l.Customer_Id WHERE c.Country = 'United States';
SELECT p.name, c.Name as customer_name FROM products p CROSS JOIN customers c LIMIT 10;
