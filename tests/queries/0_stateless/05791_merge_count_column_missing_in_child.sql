-- SELECT count() from Merge tables over tables that do not store a column added by ALTER TABLE ADD COLUMN.

DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS mj;
DROP TABLE IF EXISTS al;
DROP TABLE IF EXISTS vj;
DROP TABLE IF EXISTS vn;
DROP TABLE IF EXISTS mm;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS ma;
DROP TABLE IF EXISTS mp;
DROP TABLE IF EXISTS mu;
DROP TABLE IF EXISTS t_old;
DROP TABLE IF EXISTS t_new;
DROP TABLE IF EXISTS t_ali;
DROP TABLE IF EXISTS t_ali2;
DROP TABLE IF EXISTS u_old;
DROP TABLE IF EXISTS u_new;
DROP TABLE IF EXISTS mk;
DROP TABLE IF EXISTS t_dep;
DROP TABLE IF EXISTS t_ua;
DROP TABLE IF EXISTS t_ub;

CREATE TABLE t_old (k UInt64, s String, f UInt16, kk UInt64 ALIAS k + 1) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_old SELECT number, if(number = 500, 'n/a', toString(number)), 0 FROM numbers(1000);
ALTER TABLE t_old ADD COLUMN n UInt8 DEFAULT toUInt8(s);

CREATE TABLE t_new (k UInt64, s String, f UInt16, kk UInt64 ALIAS k + 1, n UInt8 DEFAULT toUInt8(s)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_new (k, s, f) VALUES (1000, '1', 0);

CREATE TABLE m (k UInt64, s String, n UInt8) ENGINE = Merge(currentDatabase(), '^t_(old|new)$');
CREATE TABLE mm (k UInt64, s String, n UInt8) ENGINE = Merge(currentDatabase(), '^m$');

CREATE TABLE u_old (k UInt64, url String) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO u_old SELECT number, concat('https://', hex(sipHash64(number)), '.com/', repeat(hex(sipHash128(number)), 30)) FROM numbers(1000);
ALTER TABLE u_old ADD COLUMN domain String MATERIALIZED domain(url);
CREATE TABLE u_new (k UInt64, url String, domain String MATERIALIZED domain(url)) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO u_new (k, url) VALUES (1000, 'https://a.com/');
CREATE TABLE mu (k UInt64, url String, domain String) ENGINE = Merge(currentDatabase(), '^u_(old|new)$');

CREATE TABLE t_ali2 (k UInt64, x String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ali2 SELECT number, if(number = 500, 'n/a', toString(number)) FROM numbers(1000);
ALTER TABLE t_ali2 ADD COLUMN s UInt8 DEFAULT toUInt8(x);
ALTER TABLE t_ali2 ADD COLUMN n UInt8 ALIAS s;
CREATE TABLE ma (k UInt64, n UInt8) ENGINE = Merge(currentDatabase(), '^t_(new|ali2)$');

CREATE TABLE t_ali (k UInt64, s String, n UInt8 ALIAS toUInt8(_table = 't_ali')) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ali VALUES (2000, 'x');
CREATE TABLE mp (k UInt64, s String, n UInt8) ENGINE = Merge(currentDatabase(), '^t_(new|ali)$');

SELECT count() FROM m SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1;
SELECT count() FROM m SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 0;
SELECT count() FROM merge(currentDatabase(), '^t_(old|new)$') SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1;
SELECT count() FROM m CROSS JOIN numbers(1) AS one SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1;
SELECT count() FROM mm SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1;
SELECT count() FROM mm SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 0;
SELECT count() FROM mu SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1, max_bytes_to_read = 100000;
SELECT count() FROM mu SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 0, max_bytes_to_read = 100000;
SELECT count() FROM ma SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1;
SELECT count() FROM ma SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 0;

CREATE ROW POLICY rp ON t_ali USING n = 1 TO ALL;
SELECT count() FROM mp SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1;
DROP ROW POLICY rp ON t_ali;
CREATE TABLE t_dep (k UInt64, x UInt8 ALIAS toUInt8(k < 10 AND _table = 't_dep')) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dep SELECT number FROM numbers(100);
CREATE TABLE mk (k UInt64) ENGINE = Merge(currentDatabase(), '^t_dep$');
CREATE ROW POLICY rp_dep ON t_dep USING x = 1 TO ALL;
SELECT count() FROM mk SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, query_plan_remove_unused_columns = 1;
DROP ROW POLICY rp_dep ON t_dep;

SELECT count() FROM mu SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, additional_table_filters = {'mu': 'domain != \'\''};

-- Through a view and a subquery, which read the Merge table only to count the rows too.
CREATE VIEW v AS SELECT * FROM m;
CREATE VIEW vn AS SELECT n, k FROM m;
SELECT count() FROM v SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM vn SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM vn SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, analyzer_inline_views = 1;
SELECT count() FROM (SELECT n, k FROM m) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM (SELECT n FROM m LIMIT 10) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
-- The projection still decides the number of rows here.
SELECT count() FROM (SELECT k % 7 FROM m GROUP BY k % 7) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM (SELECT arrayJoin([k, k]) FROM m) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM (SELECT sum(k) FROM m) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM (SELECT DISTINCT k % 3 FROM m) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
CREATE TABLE t_nested (`n.a` Array(UInt8), `n.c` Array(UInt8)) ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_nested VALUES ([1, 2], [3]), ([4], [5, 6, 7]);
SELECT count() FROM (SELECT n.c FROM t_nested ARRAY JOIN n) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM (SELECT c FROM (SELECT n.a AS a, n.c AS c FROM t_nested ARRAY JOIN n)) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
CREATE VIEW vj AS SELECT n.a AS a, n.c AS c FROM t_nested ARRAY JOIN n;
SELECT count() FROM (SELECT c FROM vj) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
CREATE TABLE al ENGINE = Alias(vj);
SELECT count() FROM (SELECT c FROM al) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM (SELECT c FROM merge(currentDatabase(), '^vj$')) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
CREATE TABLE mj (c UInt8) ENGINE = Merge(currentDatabase(), '^(vj|al)$');
SELECT count() FROM mj SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
-- `system.parts` returns more part states when `_state` is read, so the subquery keeps it.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT _state FROM system.parts WHERE database = currentDatabase())) WHERE explain ILIKE '%column_name: _state%';
-- The `USING` key is the projection alias `k`, which a replica resolves again from the query text.
CREATE TABLE t_ua (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_ub (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ua VALUES (1), (2);
INSERT INTO t_ub VALUES (2), (3);
SELECT count() FROM (SELECT a.k + 1 AS k FROM t_ua AS a JOIN t_ub USING (k))
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0, automatic_parallel_replicas_mode = 0;
DROP TABLE t_ua;
DROP TABLE t_ub;
DROP TABLE mj;
DROP TABLE al;
DROP TABLE vj;
DROP TABLE t_nested;
DROP TABLE v;
DROP TABLE vn;

DROP TABLE mm;
DROP TABLE m;
DROP TABLE ma;
DROP TABLE mp;
DROP TABLE mu;
DROP TABLE t_old;
DROP TABLE t_new;
DROP TABLE t_ali;
DROP TABLE t_ali2;
DROP TABLE u_old;
DROP TABLE u_new;
DROP TABLE mk;
DROP TABLE t_dep;
