SELECT a FROM (SELECT 1 AS a, (SELECT count() FROM system.numbers) AS b);
