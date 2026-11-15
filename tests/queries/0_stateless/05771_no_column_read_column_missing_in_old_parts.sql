-- A read that needs no column does not use a column that some parts do not store, or that a pending mutation drops or
-- renames away there: reading it computes its default, which reads other columns and can throw.

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

DROP TABLE IF EXISTS t_narrow;
CREATE TABLE t_narrow (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_narrow;
INSERT INTO t_narrow SELECT number, if(number = 500, 'n/a', toString(number % 100)) FROM numbers(1000);
ALTER TABLE t_narrow ADD COLUMN n UInt8 DEFAULT toUInt8(s);
INSERT INTO t_narrow (k, s) VALUES (1000, '1');
SELECT count() FROM t_narrow SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_narrow SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_narrow;

DROP TABLE IF EXISTS t_readd;
CREATE TABLE t_readd (k UInt64, s String, c UInt8) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_readd;
INSERT INTO t_readd SELECT number, if(number = 500, 'n/a', toString(number)), 0 FROM numbers(1000);
ALTER TABLE t_readd DROP COLUMN c, ADD COLUMN c UInt64 DEFAULT toUInt64(s) SETTINGS alter_sync = 0;
SELECT count() FROM t_readd SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_readd SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_readd GROUP BY _part SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_readd;

DROP TABLE IF EXISTS t_rename;
CREATE TABLE t_rename (k UInt64, s String, a UInt8) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_rename;
INSERT INTO t_rename SELECT number, if(number = 500, 'n/a', toString(number)), 0 FROM numbers(1000);
ALTER TABLE t_rename RENAME COLUMN a TO b, DROP COLUMN b, ADD COLUMN a UInt64 DEFAULT toUInt64(s) SETTINGS alter_sync = 0;
SELECT count() FROM t_rename SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_rename SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_rename GROUP BY _part SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_rename;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (k UInt64, s String, `n.a` Array(UInt8)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1;
SYSTEM STOP MERGES t_nested;
INSERT INTO t_nested SELECT number, if(number = 500, 'n/a', toString(number)), [0] FROM numbers(1000);
ALTER TABLE t_nested DROP COLUMN n, ADD COLUMN `n.a` Array(UInt64) DEFAULT [toUInt64(s)] SETTINGS alter_sync = 0;
SELECT count() FROM t_nested SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_nested SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_nested;

DROP TABLE IF EXISTS t_updated;
CREATE TABLE t_updated (a UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_updated;
INSERT INTO t_updated SELECT number, if(number = 500, 'n/a', toString(number % 100)) FROM numbers(1000);
ALTER TABLE t_updated UPDATE a = a + 0, s = concat(s, '') WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_updated ADD COLUMN n UInt8 DEFAULT toUInt8(s);
INSERT INTO t_updated (a, s) VALUES (1000, '1');
SELECT count() FROM t_updated SETTINGS apply_mutations_on_fly = 1, query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_updated SETTINGS apply_mutations_on_fly = 1, query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_updated;

DROP TABLE IF EXISTS t_independent;
CREATE TABLE t_independent (`n` UInt8, `n.a` String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 0;
SYSTEM STOP MERGES t_independent;
INSERT INTO t_independent SELECT 0, if(number = 500, 'n/a', repeat(hex(sipHash64(number)), 8)) FROM numbers(1000);
ALTER TABLE t_independent DROP COLUMN n, ADD COLUMN n UInt8 DEFAULT toUInt8(`n.a`) SETTINGS alter_sync = 0;
SELECT count() FROM t_independent SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_independent SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_independent;

DROP TABLE IF EXISTS t_renamed;
CREATE TABLE t_renamed (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_renamed;
INSERT INTO t_renamed SELECT if(number = 500, 'n/a', toString(number % 100)) FROM numbers(1000);
ALTER TABLE t_renamed RENAME COLUMN s TO t, ADD COLUMN n UInt8 DEFAULT toUInt8(t) SETTINGS alter_sync = 0;
SELECT count() FROM t_renamed SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_renamed SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_renamed;
