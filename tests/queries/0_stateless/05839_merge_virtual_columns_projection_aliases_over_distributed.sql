-- The child query of a `Merge` gets a projection of its own (the substitute read is appended to it, or it
-- is reduced to its reads), so the column list of a CTE or of a subquery must not be applied to it.

DROP TABLE IF EXISTS merge_vc_cte;
DROP TABLE IF EXISTS merge_vc_cte_dist;
DROP TABLE IF EXISTS merge_vc_cte_child;

CREATE TABLE merge_vc_cte_child (p UInt8) ENGINE = MergeTree ORDER BY p;
INSERT INTO merge_vc_cte_child VALUES (7), (8);
CREATE TABLE merge_vc_cte_dist AS merge_vc_cte_child ENGINE = Distributed(test_shard_localhost, currentDatabase(), merge_vc_cte_child);
CREATE TABLE merge_vc_cte (p String) ENGINE = Merge(currentDatabase(), '^merge_vc_cte_dist$');

SET prefer_localhost_replica = 0;

WITH cte(v) AS (SELECT _table FROM merge_vc_cte) SELECT v FROM cte;
WITH cte(v, c) AS (SELECT _table, count() FROM merge_vc_cte GROUP BY _table) SELECT * FROM cte;
SELECT v FROM (SELECT _table FROM merge_vc_cte) AS t(v);
WITH cte(v, w) AS (SELECT _table, p FROM merge_vc_cte) SELECT * FROM cte ORDER BY w;
WITH cte(v, w) AS (SELECT _table, one FROM merge_vc_cte ARRAY JOIN [1] AS one) SELECT * FROM cte;
WITH cte(p) AS (SELECT _table FROM merge_vc_cte) SELECT p FROM cte;

SET prefer_localhost_replica = 1;

WITH cte(v) AS (SELECT _table FROM merge_vc_cte) SELECT v FROM cte;
WITH cte(v, c) AS (SELECT _table, count() FROM merge_vc_cte GROUP BY _table) SELECT * FROM cte;

DROP TABLE merge_vc_cte;
DROP TABLE merge_vc_cte_dist;
DROP TABLE merge_vc_cte_child;
