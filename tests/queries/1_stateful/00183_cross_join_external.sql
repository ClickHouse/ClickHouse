CREATE VIEW unit AS (SELECT 1);

SELECT CounterID, StartURL FROM unit, test.visits ORDER BY (CounterID, StartURL) DESC LIMIT 1000 SETTINGS max_bytes_in_join=1, max_rows_in_join=1;