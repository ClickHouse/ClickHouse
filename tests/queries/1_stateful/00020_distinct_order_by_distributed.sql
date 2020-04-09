SET max_rows_to_sort = 10000;
SELECT count() FROM (SELECT DISTINCT PredLastVisit AS x FROM remote('127.0.0.{1,2}', test, visits) ORDER BY VisitID);
