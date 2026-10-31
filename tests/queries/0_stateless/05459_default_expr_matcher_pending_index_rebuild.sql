-- A skip index whose expression changes is rebuilt by a mutation. Until the mutation reaches a part,
-- the old files of the index in the part must not be used for the new expression.

SELECT '-- a stale index is not used while its rebuild is pending';
DROP TABLE IF EXISTS t_pending_rebuild;
CREATE TABLE t_pending_rebuild
(
    src1 UInt64,
    al UInt64 ALIAS greatest(COLUMNS('^src')),
    INDEX idx al TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS alter_column_secondary_index_mode = 'rebuild';
INSERT INTO t_pending_rebuild (src1) VALUES (1);
SYSTEM STOP MERGES t_pending_rebuild;
ALTER TABLE t_pending_rebuild MODIFY COLUMN al UInt64 ALIAS greatest(COLUMNS('^src')) + 999 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_pending_rebuild' AND NOT is_done;
SELECT count() FROM t_pending_rebuild WHERE al = 1000;
SELECT count() FROM t_pending_rebuild WHERE al = 1000 SETTINGS use_skip_indexes = 0;
SYSTEM START MERGES t_pending_rebuild;
DROP TABLE t_pending_rebuild;

SELECT '-- the EXCEPT of an explicit list of a lambda parameter is renamed together with the parameter';
SET enable_named_columns_in_function_tuple = 0;
DROP TABLE IF EXISTS t_list_except;
CREATE TABLE t_list_except
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(COLUMNS('^a$') APPLY (x -> arrayMap(a -> tuple(COLUMNS(a) EXCEPT a, x), [1]))))
)
ENGINE = Memory;
INSERT INTO t_list_except (a) VALUES (7);
SELECT b FROM t_list_except;
DROP TABLE t_list_except;
