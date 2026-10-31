-- Tags: zookeeper, no-replicated-database

-- The implicit min-max index of an expression alias is built over its expanded expression. Redefining
-- another alias that it reaches (`b` for `c ALIAS b + 1`) is a metadata-only change, so the index files
-- of the existing parts go stale and must not be used to prune granules.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_dependent_alias;
CREATE TABLE t_dependent_alias (a UInt64, b UInt64 ALIAS a + 1, c UInt64 ALIAS b + 1, d UInt64 ALIAS b, e UInt64 ALIAS d * 2)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 8192;

INSERT INTO t_dependent_alias SELECT number FROM numbers(100000);

ALTER TABLE t_dependent_alias MODIFY COLUMN b UInt64 ALIAS a + 1000000;

SELECT count() FROM t_dependent_alias WHERE c = 1000001;
SELECT count() FROM t_dependent_alias WHERE c = 1000001 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_dependent_alias WHERE e = 2000000;
SELECT count() FROM t_dependent_alias WHERE e = 2000000 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_dependent_alias' ORDER BY name;

DROP TABLE t_dependent_alias;

DROP TABLE IF EXISTS t_dependent_alias_replicated;
CREATE TABLE t_dependent_alias_replicated (a UInt64, b UInt64 ALIAS a + 1, c UInt64 ALIAS b + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dependent_alias_replicated', 'r1') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 8192;

INSERT INTO t_dependent_alias_replicated SELECT number FROM numbers(100000);

ALTER TABLE t_dependent_alias_replicated MODIFY COLUMN b UInt64 ALIAS a + 1000000 SETTINGS alter_sync = 2;

SELECT count() FROM t_dependent_alias_replicated WHERE c = 1000001;
SELECT count() FROM t_dependent_alias_replicated WHERE c = 1000001 SETTINGS use_skip_indexes = 0;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_dependent_alias_replicated' ORDER BY name;

DROP TABLE t_dependent_alias_replicated;
