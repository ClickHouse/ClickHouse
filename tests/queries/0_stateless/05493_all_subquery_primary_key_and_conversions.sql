-- `x op ALL (subquery)` compares `x` with a constant computed from the subquery, so the comparison keeps the
-- conversions of a comparison with a constant, and the primary key is used.

SELECT 'conversions';
SELECT '01' = 1, '01' = ALL (SELECT 1), '01' = ALL (SELECT 1 WHERE 0), '01' = ALL (SELECT 2);
SELECT n, n = ALL (SELECT '1'), n < ALL (SELECT '2'), toDate('2020-01-01') + n = ALL (SELECT '2020-01-02') FROM (SELECT arrayJoin([0, 1, 2]) AS n) ORDER BY n;

DROP TABLE IF EXISTS t_all_pk;
CREATE TABLE t_all_pk (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi';
INSERT INTO t_all_pk SELECT number FROM numbers(65536);

SELECT 'primary key';
SET force_primary_key = 1, max_rows_to_read = 4096;
SELECT count() FROM t_all_pk WHERE id = ALL (SELECT toUInt64(12345));
SELECT count() FROM t_all_pk WHERE id < ALL (SELECT toUInt64(100) UNION ALL SELECT toUInt64(200));
SELECT count() FROM t_all_pk WHERE id <= ALL (SELECT toUInt64(100));
SELECT count() FROM t_all_pk WHERE id > ALL (SELECT toUInt64(65000) UNION ALL SELECT toUInt64(64000));
SELECT count() FROM t_all_pk WHERE id >= ALL (SELECT toUInt64(65000));
SELECT count() FROM t_all_pk WHERE id = ALL (SELECT toUInt64(1) UNION ALL SELECT toUInt64(2));

DROP TABLE t_all_pk;
