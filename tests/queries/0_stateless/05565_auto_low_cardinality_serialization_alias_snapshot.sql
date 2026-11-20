-- An `Alias` table reads its target with a fresh snapshot, not with the one used during the analysis,
-- so a part that encodes the column with automatic `LowCardinality` serialization can appear in between.
-- The subcolumn rewrite of a `String` column of a `MergeTree` target is therefore not applied through
-- the alias, even when the threshold is zero and no part encodes the column.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET enable_shared_storage_snapshot_in_query = 0;

DROP TABLE IF EXISTS t_auto_lc_alias;
DROP TABLE IF EXISTS t_auto_lc_alias_target;

CREATE TABLE t_auto_lc_alias_target (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS max_uniq_number_for_low_cardinality = 0;

INSERT INTO t_auto_lc_alias_target SELECT number, 'v_' || toString(number % 10) FROM numbers(100);

CREATE TABLE t_auto_lc_alias ENGINE = Alias('t_auto_lc_alias_target');

SELECT 'alias: rewrite is skipped';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_alias) WHERE explain LIKE '%s.size%';
SELECT sum(length(s)) FROM t_auto_lc_alias;

SELECT 'direct read of the target: rewrite fires';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_alias_target) WHERE explain LIKE '%s.size%';
SELECT sum(length(s)) FROM t_auto_lc_alias_target;

DROP TABLE t_auto_lc_alias;
DROP TABLE t_auto_lc_alias_target;
