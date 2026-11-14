-- A read that needs no column does not use a column that some parts do not store: reading it there
-- computes its default, which reads other columns and can throw.

DROP TABLE IF EXISTS t_materialized;
CREATE TABLE t_materialized (k UInt64, flag UInt8, url String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 100;
SYSTEM STOP MERGES t_materialized;
INSERT INTO t_materialized SELECT number, number % 7, concat('https://', hex(sipHash64(number)), '.com/', repeat(hex(sipHash128(number)), 30)) FROM numbers(1000);
ALTER TABLE t_materialized ADD COLUMN domain String MATERIALIZED domain(url);
INSERT INTO t_materialized (k, flag, url) VALUES (1000, 1, 'https://a.com/');
SELECT count() FROM t_materialized SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_materialized SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
DROP TABLE t_materialized;

DROP TABLE IF EXISTS t_default;
CREATE TABLE t_default (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_default;
INSERT INTO t_default SELECT number, if(number = 500, 'n/a', toString(number)) FROM numbers(1000);
ALTER TABLE t_default ADD COLUMN n UInt64 DEFAULT toUInt64(s);
INSERT INTO t_default (k, s) VALUES (1000, '1000');
SELECT count() FROM t_default SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_default SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_default;
