-- Conditions sharing a subexpression keep their own PREWHERE steps in the Parquet reader,
-- so a conversion is not evaluated on the rows its guard rejects.
-- https://github.com/ClickHouse/ClickHouse/issues/132525
DROP TABLE IF EXISTS t_05828;
CREATE TABLE t_05828 (x UInt64, s String) ENGINE = File(Parquet);
INSERT INTO t_05828 VALUES (1, '2025-05-16 07:20:20'), (2, 'invaliddatetime');
SET input_format_parquet_use_native_reader_v3 = 1, optimize_move_to_prewhere = 1;

SELECT x, toDateTime64(substring(s, 1, 19), 3) AS t FROM t_05828
WHERE substring(s, 1, 19) != 'invaliddatetime' AND t > '2020-01-01';

SELECT x FROM t_05828
WHERE substring(s, 1, 19) != 'invaliddatetime' AND toDateTime64(substring(s, 1, 19), 3) > '2020-01-01';

-- Three steps: the second passes the shared subexpression on to the third.
SELECT x FROM t_05828
PREWHERE substring(s, 1, 19) != 'invaliddatetime' AND toDateTime64(substring(s, 1, 19), 3) > '2020-01-01'
    AND toDate(substring(s, 1, 19)) < '2030-01-01';

CREATE ROW POLICY OR REPLACE p_05828 ON t_05828
    USING substring(s, 1, 19) != 'invaliddatetime' AND toDateTime64(substring(s, 1, 19), 3) > '2020-01-01' TO ALL;
SELECT x FROM t_05828;
DROP ROW POLICY p_05828 ON t_05828;
DROP TABLE t_05828;
