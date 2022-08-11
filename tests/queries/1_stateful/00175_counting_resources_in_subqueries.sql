-- the work for scalar subquery is properly accounted:
SET max_rows_to_read = 1000000;
SELECT 1 = (SELECT count() FROM test.hits WHERE NOT ignore(AdvEngineID)); -- { serverError 158 }

-- the work for subquery in IN is properly accounted:
SET max_rows_to_read = 1000000;
SELECT 1 IN (SELECT count() FROM hits WHERE NOT ignore(AdvEngineID)); -- { serverError 158 }

-- this query reads from the table twice:
SET max_rows_to_read = 15000000;
SELECT count() IN (SELECT count() FROM hits WHERE NOT ignore(AdvEngineID)) FROM hits WHERE NOT ignore(AdvEngineID); -- { serverError 158 }

-- the resources are properly accounted even if the subquery is evaluated in advance to facilitate the index analysis.
-- this query is using index and filter out the second reading pass.
SET max_rows_to_read = 1000000;
SELECT count() FROM hits WHERE CounterID > (SELECT count() FROM hits WHERE NOT ignore(AdvEngineID)); -- { serverError 158 }

-- this query is using index but have to read all the data twice.
SET max_rows_to_read = 15000000;
SELECT count() FROM hits WHERE CounterID < (SELECT count() FROM hits WHERE NOT ignore(AdvEngineID)); -- { serverError 158 }
