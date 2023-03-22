SELECT count() AS c FROM test.hits WHERE CounterID = 1704509 WITH TOTALS SETTINGS totals_mode = 'before_having',          max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT count() AS c FROM test.hits WHERE CounterID = 1704509 WITH TOTALS SETTINGS totals_mode = 'after_having_inclusive', max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT count() AS c FROM test.hits WHERE CounterID = 1704509 WITH TOTALS SETTINGS totals_mode = 'after_having_exclusive', max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT count() AS c FROM test.hits WHERE CounterID = 1704509 WITH TOTALS SETTINGS totals_mode = 'after_having_auto',      max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';

SELECT 1 AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS SETTINGS totals_mode = 'before_having',          max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT 1 AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS SETTINGS totals_mode = 'after_having_inclusive', max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT 1 AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS SETTINGS totals_mode = 'after_having_exclusive', max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT 1 AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS SETTINGS totals_mode = 'after_having_auto',      max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';

SELECT TraficSourceID AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS ORDER BY k SETTINGS totals_mode = 'before_having',          max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT TraficSourceID AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS ORDER BY k SETTINGS totals_mode = 'after_having_inclusive', max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT TraficSourceID AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS ORDER BY k SETTINGS totals_mode = 'after_having_exclusive', max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';
SELECT TraficSourceID AS k, count() AS c FROM test.hits WHERE CounterID = 1704509 GROUP BY k WITH TOTALS ORDER BY k SETTINGS totals_mode = 'after_having_auto',      max_rows_to_group_by = 100000, group_by_overflow_mode = 'any';

