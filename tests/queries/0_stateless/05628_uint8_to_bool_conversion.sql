-- Data reaching a Bool from a UInt8 is converted as CAST converts it: every nonzero value becomes 1.
-- Random settings limits: ratio_of_defaults_for_sparse_serialization=(0.3, None)
-- groupArray of a sparse column puts its default values last, which would break the id order of the arrays below.

CREATE TABLE legacy (id UInt32, active UInt8, n Nullable(UInt8), a Array(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO legacy VALUES (1, 0, 0, [0]), (2, 1, 1, [1]), (3, 2, 2, [2]), (4, 255, NULL, [255, 0]);

SELECT 'INSERT SELECT';
CREATE TABLE users (id UInt32, active Bool, n Nullable(Bool), a Array(Bool)) ENGINE = MergeTree ORDER BY id;
INSERT INTO users SELECT * FROM legacy;
INSERT INTO users SELECT 5, 2, 2, [2]::Array(UInt8);
SELECT id, toUInt8(active), toUInt8(n), arrayMap(x -> toUInt8(x), a) FROM users ORDER BY id;
SELECT countIf(active = true), countIf(active), count() FROM users;
SELECT active, count() FROM users GROUP BY active ORDER BY active;
CREATE TABLE users_nested (t Tuple(x Bool), m Map(String, Bool)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO users_nested SELECT tuple(255)::Tuple(x UInt8), map('a', 2)::Map(String, UInt8);
SELECT toUInt8(t.x), toUInt8(m['a']) FROM users_nested;

SELECT 'MATERIALIZED VIEW';
CREATE TABLE mv_src (active UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE mv_dst (active Bool) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv TO mv_dst AS SELECT active FROM mv_src;
CREATE TABLE mvr_dst (active UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mvr TO mvr_dst (active Bool) AS SELECT active FROM mv_src;
INSERT INTO mv_src VALUES (0), (2), (255);
SELECT arraySort(groupArray(toUInt8(active))) FROM mv_dst;
SELECT arraySort(groupArray(toUInt8(active))) FROM mvr;
SELECT count() FROM mvr WHERE active = true SETTINGS optimize_move_to_prewhere = 1;
SELECT count() FROM mvr PREWHERE active = true; -- { serverError ILLEGAL_PREWHERE }

SELECT 'REFRESHABLE MATERIALIZED VIEW';
CREATE TABLE rmv_dst (active Bool) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW rmv REFRESH AFTER 1 YEAR SETTINGS all_replicas = 1 APPEND TO rmv_dst EMPTY AS SELECT active FROM mv_src;
CREATE MATERIALIZED VIEW rmv_push REFRESH AFTER 1 YEAR SETTINGS all_replicas = 1 APPEND TO mv_src EMPTY AS SELECT toUInt8(2) AS active;
SYSTEM REFRESH VIEW rmv_push;
SYSTEM WAIT VIEW rmv_push;
SYSTEM REFRESH VIEW rmv;
SYSTEM WAIT VIEW rmv;
SELECT arraySort(groupArray(toUInt8(active))) FROM mv_dst;
SELECT arraySort(groupArray(toUInt8(active))) FROM rmv_dst;

SELECT 'VIEW';
CREATE VIEW v (id UInt32, active Bool) AS SELECT id, active FROM legacy;
SELECT countIf(active = true) FROM v;
SELECT countIf(active = true) FROM v SETTINGS analyzer_inline_views = 1;
SELECT id FROM v ORDER BY active DESC, id LIMIT 1;
CREATE TABLE dist AS legacy ENGINE = Distributed(test_shard_localhost, currentDatabase(), legacy);
CREATE VIEW vd (id UInt32, active Bool) AS SELECT id, active FROM dist;
SELECT countIf(active = true) FROM vd SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT countIf(active = true) FROM vd SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;

SELECT 'Distributed INSERT';
CREATE TABLE loc (id UInt32, active Bool) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_w (id UInt32, active UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), loc);
INSERT INTO dist_w SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 1 VALUES (1, 2);
INSERT INTO dist_w SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (2, 255);
INSERT INTO dist_w SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0 VALUES (3, 2);
SYSTEM FLUSH DISTRIBUTED dist_w;
SELECT groupArray(toUInt8(active)) FROM (SELECT active FROM loc ORDER BY id);

SELECT 'Merge';
CREATE TABLE m (id UInt32, active Bool) ENGINE = Merge(currentDatabase(), '^legacy$');
SELECT count() FROM m WHERE active = true SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM m WHERE active = true SETTINGS optimize_move_to_prewhere = 1;
SELECT count() FROM m PREWHERE active = true; -- { serverError ILLEGAL_PREWHERE }

SELECT 'MODIFY COLUMN, pending mutation';
CREATE TABLE pending (id UInt32, active UInt8, INDEX ix active TYPE set(0) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO pending SELECT id, active FROM legacy;
SYSTEM STOP MERGES pending;
ALTER TABLE pending MODIFY COLUMN active Bool SETTINGS alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'pending' AND NOT is_done;
SELECT groupArray(toUInt8(active)) FROM (SELECT active FROM pending ORDER BY id);
SELECT count() FROM pending WHERE active = true;
SELECT count() FROM pending WHERE active = true SETTINGS use_skip_indexes = 0;

SELECT 'MODIFY COLUMN';
CREATE TABLE flags (id UInt32, active UInt8, a Array(UInt8), w UInt8, INDEX ix active TYPE set(0) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO flags SELECT id, active, a, active FROM legacy;
ALTER TABLE flags MODIFY COLUMN active Bool, MODIFY COLUMN a Array(Bool), MODIFY COLUMN w Nullable(Bool) SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'flags';
SELECT groupArray(toUInt8(active)), groupArray(arrayMap(x -> toUInt8(x), a)), groupArray(toUInt8(w)) FROM (SELECT * FROM flags ORDER BY id);
SELECT count() FROM flags WHERE active = true;
DELETE FROM flags WHERE active = true;
SELECT id FROM flags ORDER BY id;
CREATE TABLE keyed (k UInt8) ENGINE = MergeTree ORDER BY k;
ALTER TABLE keyed MODIFY COLUMN k Bool; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE pkeyed (k UInt8) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
ALTER TABLE pkeyed MODIFY COLUMN k Bool; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE ver (k UInt32, v UInt8) ENGINE = ReplacingMergeTree(v) ORDER BY k;
ALTER TABLE ver MODIFY COLUMN v Bool; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE ver_sub (k UInt32, t Tuple(v UInt8)) ENGINE = ReplacingMergeTree(t.v) ORDER BY k;
ALTER TABLE ver_sub MODIFY COLUMN t Tuple(v Bool); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE ver_vc (k UInt32, s Int8, v UInt8) ENGINE = VersionedCollapsingMergeTree(s, v) ORDER BY k;
ALTER TABLE ver_vc MODIFY COLUMN v Bool; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE back (id UInt32, active Bool) ENGINE = MergeTree ORDER BY id;
ALTER TABLE back MODIFY COLUMN active UInt8 SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'back';

SELECT 'ATTACH PARTITION FROM';
CREATE TABLE src (p UInt8, active UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
CREATE TABLE dst (p UInt8, active Bool) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO src VALUES (1, 2), (1, 0);
ALTER TABLE dst ATTACH PARTITION 1 FROM src; -- { serverError INCOMPATIBLE_COLUMNS }
ALTER TABLE dst REPLACE PARTITION 1 FROM src; -- { serverError INCOMPATIBLE_COLUMNS }
ALTER TABLE src MOVE PARTITION 1 TO TABLE dst; -- { serverError INCOMPATIBLE_COLUMNS }
INSERT INTO dst VALUES (2, true);
ALTER TABLE src ATTACH PARTITION 2 FROM dst;
SELECT p, toUInt8(active) FROM src WHERE p = 2;

SELECT 'Variant';
-- CAST matches Variant alternatives by name, so a Bool inside one is left as is rather than failing the conversion.
CREATE TABLE vsrc (v Variant(Array(UInt8), String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE vdst (v Variant(Array(Bool), String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO vsrc SELECT [2]::Array(UInt8);
INSERT INTO vdst SELECT v FROM vsrc;
ALTER TABLE vsrc MODIFY COLUMN v Variant(Array(Bool), String) SETTINGS mutations_sync = 2;
SELECT (SELECT count() FROM vdst), (SELECT count() FROM vsrc), (SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'vsrc');

SELECT 'Projection';
CREATE TABLE proj (id UInt32, active UInt8, PROJECTION p (SELECT sum(active))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE proj MODIFY COLUMN active Bool; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE proj DROP PROJECTION p, MODIFY COLUMN active Bool SETTINGS mutations_sync = 2;
CREATE TABLE proj_sub (id UInt32, t Tuple(x UInt8), m Map(String, UInt8), PROJECTION pt (SELECT sum(t.x)), PROJECTION pm (SELECT sum(arraySum(m.values)))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE proj_sub MODIFY COLUMN t Tuple(x Bool); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE proj_sub MODIFY COLUMN m Map(String, Bool); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE proj_outer (id UInt32, m Map(String, Array(UInt8)), PROJECTION p (SELECT sum(arraySum(arrayFlatten(mapValues(m)))))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE proj_outer MODIFY COLUMN m Map(FixedString(1), Array(Bool)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE proj_sub MODIFY COLUMN t Tuple(x Bool, y String) SETTINGS allow_metadata_only_named_tuple_alter = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE idx_sub (id UInt32, p Tuple(x UInt8), INDEX ix toString(p.x) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id;
ALTER TABLE idx_sub MODIFY COLUMN p Tuple(x Bool); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE proj_key (id UInt32, active UInt8, PROJECTION p (SELECT active, count() GROUP BY active)) ENGINE = MergeTree ORDER BY id;
INSERT INTO proj_key VALUES (1, 2), (2, 255), (3, 1);
ALTER TABLE proj_key MODIFY COLUMN active Bool SETTINGS mutations_sync = 2;
SELECT active, count() FROM proj_key GROUP BY active ORDER BY active SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
CREATE TABLE proj_where (id UInt32, active UInt8, PROJECTION p (SELECT id, active WHERE active = 1 ORDER BY id)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE proj_where MODIFY COLUMN active Bool; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE proj_order (id UInt32, active UInt8, PROJECTION p (SELECT id, active ORDER BY toString(active))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE proj_order MODIFY COLUMN active Bool; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
CREATE TABLE proj_plain (id UInt32, active UInt8, PROJECTION p (SELECT id, active ORDER BY id)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO proj_plain VALUES (1, 0), (2, 255), (3, 2);
ALTER TABLE proj_plain MODIFY COLUMN active Bool SETTINGS mutations_sync = 2;
SELECT toUInt8(active) FROM proj_plain WHERE id = 2 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
