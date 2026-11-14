-- Tags: no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database, no-shared-merge-tree: the table must stay a plain `MergeTree`.

-- A metadata ALTER whose mutation has no part to rewrite finishes it right away, as an UPDATE does,
-- so `finish_time` is set instead of staying zero.

DROP TABLE IF EXISTS t_alter_no_parts;
CREATE TABLE t_alter_no_parts (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

ALTER TABLE t_alter_no_parts MODIFY COLUMN v UInt32;
ALTER TABLE t_alter_no_parts RENAME COLUMN v TO w;
ALTER TABLE t_alter_no_parts UPDATE w = 1 WHERE 1;

SELECT command, is_done, finish_time != 0, progress FROM system.mutations
WHERE database = currentDatabase() AND table = 't_alter_no_parts' ORDER BY mutation_id;

DROP TABLE t_alter_no_parts;
