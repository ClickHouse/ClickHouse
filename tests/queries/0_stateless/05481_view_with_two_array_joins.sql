-- A view whose query has more than one ARRAY JOIN must be readable (issue #128410).
CREATE TABLE t (id UInt32, a Array(Array(UInt32))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, [[1, 2], [3]]);
CREATE TABLE t_dist AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);

CREATE VIEW v AS SELECT id, y FROM t ARRAY JOIN a AS x ARRAY JOIN x AS y;
CREATE VIEW v_definer SQL SECURITY DEFINER AS SELECT id, y FROM t ARRAY JOIN a AS x ARRAY JOIN x AS y;
CREATE VIEW v_dist AS SELECT id, y FROM t_dist ARRAY JOIN a AS x ARRAY JOIN x AS y;

SELECT 'v', * FROM v ORDER BY y SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT 'v_definer', * FROM v_definer ORDER BY y SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;
SELECT 'v_definer, no pushdown', * FROM v_definer ORDER BY y SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT 'v_dist', * FROM v_dist ORDER BY y SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;

-- A refreshable materialized view reading the view.
CREATE TABLE dst (id UInt32, y UInt32) ENGINE = MergeTree ORDER BY (id, y);
CREATE MATERIALIZED VIEW mv REFRESH AFTER 1 YEAR SETTINGS all_replicas = 1 APPEND TO dst EMPTY AS SELECT id, y FROM v;
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT 'mv', * FROM dst ORDER BY y;
