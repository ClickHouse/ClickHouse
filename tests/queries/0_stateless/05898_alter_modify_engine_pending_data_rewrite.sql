-- Tags: no-shared-merge-tree
-- ^ no-shared-merge-tree: MODIFY ENGINE is not supported for Replicated/Shared MergeTree yet.

-- The engine set by ALTER TABLE ... MODIFY ENGINE applies on the next table load. Until then the table
-- works as its old engine, so the guards of the new engine on its special columns are not in effect.
-- Commands that rewrite data are refused meanwhile, otherwise they could rewrite the sign or version
-- column and the new engine would then collapse the wrong rows.

SET allow_experimental_alter_modify_engine = 1;
SET async_insert = 0;
SET mutations_sync = 2;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_pending_collapsing;
CREATE TABLE t_pending_collapsing (k UInt32, v UInt32, sign Int8, extra UInt32)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_pending_collapsing SELECT number, number, 1, 0 FROM numbers(5);

-- Without a pending engine these are ordinary commands of a MergeTree table.
ALTER TABLE t_pending_collapsing UPDATE extra = 1 WHERE 1;
ALTER TABLE t_pending_collapsing CLEAR COLUMN extra;

-- A data-rewriting command in the same ALTER as MODIFY ENGINE is refused, and the engine is not changed.
ALTER TABLE t_pending_collapsing CLEAR COLUMN sign, MODIFY ENGINE = CollapsingMergeTree(sign); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_pending_collapsing MODIFY ENGINE = CollapsingMergeTree(sign), MODIFY COLUMN extra UInt64; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending_collapsing';

ALTER TABLE t_pending_collapsing MODIFY ENGINE = CollapsingMergeTree(sign);

-- While the engine is pending.
ALTER TABLE t_pending_collapsing CLEAR COLUMN sign; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_pending_collapsing CLEAR COLUMN extra; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_pending_collapsing DROP COLUMN extra; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_pending_collapsing MODIFY COLUMN extra UInt64; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_pending_collapsing UPDATE sign = 0 WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_pending_collapsing DELETE WHERE k = 0; -- { serverError SUPPORT_IS_DISABLED }
UPDATE t_pending_collapsing SET sign = 0 WHERE 1; -- { serverError SUPPORT_IS_DISABLED }
DELETE FROM t_pending_collapsing WHERE k = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Metadata-only commands are still allowed.
ALTER TABLE t_pending_collapsing ADD COLUMN z UInt8;
ALTER TABLE t_pending_collapsing MODIFY SETTING merge_max_block_size = 4096;

SELECT 'pending', count(), sum(sign) FROM t_pending_collapsing;

DETACH TABLE t_pending_collapsing;
ATTACH TABLE t_pending_collapsing;

-- Nothing was rewritten, so every row is still there under the new engine.
SELECT 'reloaded', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending_collapsing';
SELECT 'final', count(), sum(sign) FROM t_pending_collapsing FINAL;

-- After the reload nothing is pending: the guards of the new engine answer for its special columns,
-- and the other commands work again.
ALTER TABLE t_pending_collapsing CLEAR COLUMN sign; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_pending_collapsing UPDATE extra = 2 WHERE 1;
ALTER TABLE t_pending_collapsing CLEAR COLUMN z;
SELECT 'after', count(), sum(extra) FROM t_pending_collapsing FINAL;

DROP TABLE t_pending_collapsing;

-- The same for the version column of ReplacingMergeTree.
DROP TABLE IF EXISTS t_pending_replacing;
CREATE TABLE t_pending_replacing (k UInt32, v String, ver UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pending_replacing VALUES (1, 'old', 1);
INSERT INTO t_pending_replacing VALUES (1, 'new', 2);

ALTER TABLE t_pending_replacing MODIFY ENGINE = ReplacingMergeTree(ver);
ALTER TABLE t_pending_replacing CLEAR COLUMN ver; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_pending_replacing UPDATE ver = 0 WHERE 1; -- { serverError SUPPORT_IS_DISABLED }

DETACH TABLE t_pending_replacing;
ATTACH TABLE t_pending_replacing;
SELECT 'replacing', v FROM t_pending_replacing FINAL;

DROP TABLE t_pending_replacing;
