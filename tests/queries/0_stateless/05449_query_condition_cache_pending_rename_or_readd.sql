-- While a metadata mutation that changes column identity is still pending, reads already apply it,
-- but the part name and the condition hash stay the same. An entry of the query condition cache
-- written before the `ALTER` describes other data, so it must not be consulted for such a part.

SET use_query_condition_cache = 1;
SET use_skip_indexes = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE IF EXISTS t_qcc_pending_rename;
CREATE TABLE t_qcc_pending_rename (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple();
-- Only to hold the mutation pending deterministically.
SYSTEM STOP MERGES t_qcc_pending_rename;
INSERT INTO t_qcc_pending_rename VALUES (1, 100), (2, 200);

-- Warm the cache.
SELECT count() FROM t_qcc_pending_rename WHERE x = 100;
SELECT count() FROM t_qcc_pending_rename PREWHERE x = 100;

ALTER TABLE t_qcc_pending_rename DROP COLUMN x, RENAME COLUMN y TO x SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT count() FROM t_qcc_pending_rename WHERE x = 100;
SELECT count() FROM t_qcc_pending_rename PREWHERE x = 100;
SELECT count() FROM t_qcc_pending_rename WHERE x = 100 SETTINGS use_query_condition_cache = 0;

SYSTEM START MERGES t_qcc_pending_rename;
DROP TABLE t_qcc_pending_rename;

-- The same for a pending `DROP COLUMN` followed by re-adding a column with the same name: reads
-- treat the on-disk data as missing and fill the new default.
DROP TABLE IF EXISTS t_qcc_pending_readd;
CREATE TABLE t_qcc_pending_readd (id Int64, x Int64) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_qcc_pending_readd;
INSERT INTO t_qcc_pending_readd VALUES (1, 1), (2, 2);

SELECT count() FROM t_qcc_pending_readd WHERE x = 100;
SELECT count() FROM t_qcc_pending_readd PREWHERE x = 100;

ALTER TABLE t_qcc_pending_readd DROP COLUMN x, ADD COLUMN x Int64 DEFAULT 100 SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT count() FROM t_qcc_pending_readd WHERE x = 100;
SELECT count() FROM t_qcc_pending_readd PREWHERE x = 100;
SELECT count() FROM t_qcc_pending_readd WHERE x = 100 SETTINGS use_query_condition_cache = 0;

SYSTEM START MERGES t_qcc_pending_readd;
DROP TABLE t_qcc_pending_readd;
