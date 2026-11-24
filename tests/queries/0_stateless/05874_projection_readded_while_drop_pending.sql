-- A projection part is looked up by the projection's name. After `DROP PROJECTION p` and `ADD PROJECTION p`
-- with another definition, a part the `DROP PROJECTION` mutation has not rewritten yet still holds the old
-- projection part, and must not be read as the new `p`.

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;

DROP TABLE IF EXISTS t_order_by;
CREATE TABLE t_order_by (id UInt64, k UInt64, a UInt64, PROJECTION p (SELECT id, k, a ORDER BY k))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
INSERT INTO t_order_by SELECT number, number % 100, number % 2 FROM numbers(2000);
SYSTEM STOP MERGES t_order_by;
ALTER TABLE t_order_by DROP PROJECTION p SETTINGS alter_sync = 0;
ALTER TABLE t_order_by ADD PROJECTION p (SELECT id, k, a ORDER BY a) SETTINGS alter_sync = 0;

SELECT 'another ORDER BY, two ALTERs';
SELECT count() FROM t_order_by WHERE a = 1 AND k = 5;
SELECT count() FROM t_order_by WHERE a = 1 AND k = 5 SETTINGS optimize_use_projections = 0;

SELECT 'a part inserted after the ALTER uses the new projection';
INSERT INTO t_order_by SELECT number, number % 100, number % 2 FROM numbers(2000, 2000);
SELECT count() FROM t_order_by WHERE a = 1 AND k = 5 SETTINGS force_optimize_projection_name = 'p';

DROP TABLE IF EXISTS t_one_alter;
CREATE TABLE t_one_alter (id UInt64, k UInt64, a UInt64, PROJECTION p (SELECT id, k, a ORDER BY k))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
INSERT INTO t_one_alter SELECT number, number % 100, number % 2 FROM numbers(2000);
SYSTEM STOP MERGES t_one_alter;
ALTER TABLE t_one_alter DROP PROJECTION p, ADD PROJECTION p (SELECT id, k, a ORDER BY a) SETTINGS alter_sync = 0;

SELECT 'another ORDER BY, one ALTER';
SELECT count() FROM t_one_alter WHERE a = 1 AND k = 5;
SELECT count() FROM t_one_alter WHERE a = 1 AND k = 5 SETTINGS optimize_use_projections = 0;

DROP TABLE IF EXISTS t_index;
CREATE TABLE t_index (id UInt64, k UInt64, a UInt64, PROJECTION p (SELECT _part_offset WHERE a = 0 ORDER BY k))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
INSERT INTO t_index SELECT number, number % 100, number % 2 FROM numbers(2000);
SYSTEM STOP MERGES t_index;
ALTER TABLE t_index DROP PROJECTION p SETTINGS alter_sync = 0;
ALTER TABLE t_index ADD PROJECTION p (SELECT _part_offset WHERE a = 1 ORDER BY k) SETTINGS alter_sync = 0;

SELECT 'projection index with another WHERE';
SELECT count() FROM t_index WHERE k = 5 AND a = 1 SETTINGS min_table_rows_to_use_projection_index = 0;
SELECT count() FROM t_index WHERE k = 5 AND a = 1 SETTINGS optimize_use_projections = 0;

DROP TABLE IF EXISTS t_aggregate;
CREATE TABLE t_aggregate (id UInt64, k UInt64, a UInt64, PROJECTION p (SELECT k, count() WHERE a = 0 GROUP BY k))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
INSERT INTO t_aggregate SELECT number, number % 100, number % 2 FROM numbers(2000);
SYSTEM STOP MERGES t_aggregate;
ALTER TABLE t_aggregate DROP PROJECTION p SETTINGS alter_sync = 0;
ALTER TABLE t_aggregate ADD PROJECTION p (SELECT k, count() WHERE a = 1 GROUP BY k) SETTINGS alter_sync = 0;

SELECT 'aggregate projection with another WHERE';
SELECT k, count() FROM t_aggregate WHERE a = 1 GROUP BY k ORDER BY k LIMIT 2;
SELECT k, count() FROM t_aggregate WHERE a = 1 GROUP BY k ORDER BY k LIMIT 2 SETTINGS optimize_use_projections = 0;

-- A mutation that fails keeps the `DROP PROJECTION` behind it pending, also across a reload of the table and merges.
DROP TABLE IF EXISTS t_queued;
CREATE TABLE t_queued (id UInt64, k UInt64, a UInt64, PROJECTION p (SELECT id, k, a ORDER BY k))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
INSERT INTO t_queued SELECT number, number % 100, number % 2 FROM numbers(1000);
INSERT INTO t_queued SELECT number, number % 100, number % 2 FROM numbers(1000, 1000);
ALTER TABLE t_queued UPDATE k = k + throwIf(id >= 0) WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_queued DROP PROJECTION p, ADD PROJECTION p (SELECT id, k, a ORDER BY a) SETTINGS alter_sync = 0;

SELECT 'drop queued behind a failing mutation';
SELECT count() FROM t_queued WHERE a = 1 AND k = 5;
DETACH TABLE t_queued;
ATTACH TABLE t_queued;
SELECT count() FROM t_queued WHERE a = 1 AND k = 5;
OPTIMIZE TABLE t_queued FINAL;
SELECT count() FROM t_queued WHERE a = 1 AND k = 5;
SELECT count() FROM t_queued WHERE a = 1 AND k = 5 SETTINGS optimize_use_projections = 0;

-- `mergeTreeProjection` reads the projection parts as they are; a pending drop of another projection does not make
-- it refuse the read under a row policy.
DROP TABLE IF EXISTS t_row_policy;
DROP ROW POLICY IF EXISTS rp_t_row_policy ON t_row_policy;
CREATE TABLE t_row_policy (id UInt64, v UInt8, PROJECTION p (SELECT id, v ORDER BY v), PROJECTION q (SELECT id ORDER BY id))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_row_policy VALUES (1, 1), (2, 1), (3, 0);
CREATE ROW POLICY rp_t_row_policy ON t_row_policy FOR SELECT USING v = 1 TO ALL;
SYSTEM STOP MERGES t_row_policy;
ALTER TABLE t_row_policy DROP PROJECTION q SETTINGS alter_sync = 0;

SELECT 'mergeTreeProjection under a row policy while another projection is dropped';
SELECT id FROM mergeTreeProjection(currentDatabase(), t_row_policy, p) ORDER BY id;

-- The part's `p.proj` belongs to the dropped definition of `p`, so under a row policy the read is refused.
ALTER TABLE t_row_policy DROP PROJECTION p, ADD PROJECTION p (SELECT id, v ORDER BY id) SETTINGS alter_sync = 0;
SELECT 'mergeTreeProjection under a row policy while the projection itself is dropped and added again';
SELECT id FROM mergeTreeProjection(currentDatabase(), t_row_policy, p) ORDER BY id; -- { serverError ACCESS_DENIED }
DROP ROW POLICY rp_t_row_policy ON t_row_policy;

DROP TABLE t_order_by;
DROP TABLE t_one_alter;
DROP TABLE t_index;
DROP TABLE t_aggregate;
DROP TABLE t_queued;
DROP TABLE t_row_policy;
