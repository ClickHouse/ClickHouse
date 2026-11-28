-- A read that needs no column does not use a column whose default reads stored columns (also through an alias,
-- t_alias_reads) where a part does not store it or a pending mutation drops it: computing it reads them and can throw.
-- A column whose default reads no stored column is still used (t_clear, t_literal, t_constant, t_lambda, t_alias,
-- t_added), and so is a column that old parts store under its name before a pending rename (t_rename_insert, t_rename_readd).

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

DROP TABLE IF EXISTS t_recalculated;
CREATE TABLE t_recalculated (k UInt8, s String, m UInt64 MATERIALIZED toUInt64(s)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_recalculated;
INSERT INTO t_recalculated (k, s) SELECT 0, toString(number) FROM numbers(1000);
ALTER TABLE t_recalculated UPDATE k = k, s = concat(s, 'x') WHERE 1 SETTINGS mutations_sync = 0;
SELECT count() FROM t_recalculated SETTINGS apply_mutations_on_fly = 1, query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_recalculated SETTINGS apply_mutations_on_fly = 1, query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_recalculated;

DROP TABLE IF EXISTS t_update_target;
CREATE TABLE t_update_target (k UInt8, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_update_target;
INSERT INTO t_update_target SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_update_target UPDATE k = k WHERE 1 SETTINGS mutations_sync = 0;
SELECT count() FROM t_update_target SETTINGS apply_mutations_on_fly = 1, query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_update_target SETTINGS apply_mutations_on_fly = 1, query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
DROP TABLE t_update_target;

DROP TABLE IF EXISTS t_patch_target;
CREATE TABLE t_patch_target (k UInt8, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_patch_target;
INSERT INTO t_patch_target SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
UPDATE t_patch_target SET k = k + 1 WHERE 1 SETTINGS enable_lightweight_update = 1;
SELECT count() FROM t_patch_target SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_patch_target SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
DROP TABLE t_patch_target;

DROP TABLE IF EXISTS t_clear;
CREATE TABLE t_clear (k UInt8, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_clear;
INSERT INTO t_clear SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_clear CLEAR COLUMN k IN PARTITION ID 'all' SETTINGS alter_sync = 0;
SELECT count() FROM t_clear SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_clear SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_clear WHERE _part_offset % 2 = 0 SETTINGS max_bytes_to_read = 100000;
DROP TABLE t_clear;

DROP TABLE IF EXISTS t_literal;
CREATE TABLE t_literal (k UInt8 DEFAULT 0, s String, m String MATERIALIZED s) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_literal;
INSERT INTO t_literal SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_literal CLEAR COLUMN k IN PARTITION ID 'all' SETTINGS alter_sync = 0;
SELECT count() FROM t_literal SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_literal SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
DROP TABLE t_literal;

DROP TABLE IF EXISTS t_rename_insert;
CREATE TABLE t_rename_insert (k UInt8, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_rename_insert;
INSERT INTO t_rename_insert SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_rename_insert RENAME COLUMN k TO renamed SETTINGS alter_sync = 0;
INSERT INTO t_rename_insert VALUES (0, 'new');
SELECT count() FROM t_rename_insert SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_rename_insert SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
DROP TABLE t_rename_insert;

DROP TABLE IF EXISTS t_constant;
CREATE TABLE t_constant (k UInt8 DEFAULT toUInt8(0), s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_constant;
INSERT INTO t_constant SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_constant CLEAR COLUMN k IN PARTITION ID 'all' SETTINGS alter_sync = 0;
SELECT count() FROM t_constant SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_constant SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_constant WHERE _part_offset % 2 = 0 SETTINGS max_bytes_to_read = 100000;
DROP TABLE t_constant;

DROP TABLE IF EXISTS t_lambda;
CREATE TABLE t_lambda (k UInt8 DEFAULT arraySum(arrayMap(x -> x, [toUInt8(0)])), s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_lambda;
INSERT INTO t_lambda SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_lambda CLEAR COLUMN k IN PARTITION ID 'all' SETTINGS alter_sync = 0;
SELECT count() FROM t_lambda SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_lambda SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_lambda WHERE _part_offset % 2 = 0 SETTINGS max_bytes_to_read = 100000;
DROP TABLE t_lambda;

DROP TABLE IF EXISTS t_rename_readd;
CREATE TABLE t_rename_readd (k UInt8, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_rename_readd;
INSERT INTO t_rename_readd SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_rename_readd RENAME COLUMN k TO renamed, ADD COLUMN k UInt8 DEFAULT 0 SETTINGS alter_sync = 0;
SELECT count() FROM t_rename_readd WHERE _part_offset % 2 = 0 SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM t_rename_readd GROUP BY _part SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
DROP TABLE t_rename_readd;

DROP TABLE IF EXISTS t_alias;
CREATE TABLE t_alias (k UInt8 DEFAULT c, s String, c UInt8 ALIAS d, d UInt8 ALIAS 0) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_alias;
INSERT INTO t_alias (k, s) SELECT 0, repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
ALTER TABLE t_alias CLEAR COLUMN k IN PARTITION ID 'all' SETTINGS alter_sync = 0;
SELECT count() FROM t_alias SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_alias SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_alias WHERE _part_offset % 2 = 0 SETTINGS max_bytes_to_read = 100000;
DROP TABLE t_alias;

DROP TABLE IF EXISTS t_alias_reads;
CREATE TABLE t_alias_reads (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_alias_reads;
INSERT INTO t_alias_reads SELECT number, if(number = 500, 'n/a', toString(number % 100)) FROM numbers(1000);
ALTER TABLE t_alias_reads ADD COLUMN a UInt8 ALIAS toUInt8(s), ADD COLUMN n UInt8 DEFAULT a;
INSERT INTO t_alias_reads (k, s) VALUES (1000, '1');
SELECT count() FROM t_alias_reads SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_alias_reads SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_alias_reads;

DROP TABLE IF EXISTS t_added;
CREATE TABLE t_added (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP MERGES t_added;
INSERT INTO t_added SELECT toString(number) FROM numbers(10);
ALTER TABLE t_added ADD COLUMN k UInt8 DEFAULT 0;
INSERT INTO t_added (s) SELECT repeat(hex(sipHash128(number)), 30) FROM numbers(1000);
SELECT count() FROM t_added SETTINGS query_plan_remove_unused_columns = 1, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_added SETTINGS query_plan_remove_unused_columns = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, max_bytes_to_read = 100000;
DROP TABLE t_added;
