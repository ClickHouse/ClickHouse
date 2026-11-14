-- Tags: no-fasttest, need-query-parameters

-- CREATE ... AS SELECT with an empty result creates an Iceberg table, and
-- PARTITION BY icebergTruncate without the width argument is rejected as BAD_ARGUMENTS, not LOGICAL_ERROR.

SET allow_insert_into_iceberg = 1;

DROP TABLE IF EXISTS t_empty_ctas;
DROP TABLE IF EXISTS t_bad_truncate;

CREATE TABLE t_empty_ctas (c0 Int) ENGINE = IcebergLocal({CLICKHOUSE_DATABASE:String} || '/t_empty_ctas/', 'Parquet') AS (SELECT 1 OFFSET 1 ROW);
SELECT count() FROM t_empty_ctas;

CREATE TABLE t_bad_truncate (c0 Int) ENGINE = IcebergLocal({CLICKHOUSE_DATABASE:String} || '/t_bad_truncate/', 'Parquet') PARTITION BY (icebergTruncate(c0)); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_empty_ctas;
