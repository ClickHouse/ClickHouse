-- Tags: shard
-- Tag shard: the test reads a `Distributed` table, so it needs a server that has the test clusters.

-- `analyzer_compatibility_join_using_top_level_identifier` resolves a `JOIN USING` key from a
-- `SELECT`-list alias. The description of the setting in `src/Core/Settings.cpp` says that such a
-- query, when it is sent to a remote server, "is rejected with an exception only when the identifier
-- cannot be resolved on the remote server at all; if the alias shadows a real column of the left
-- table, the remote server joins by that column instead, so the results may differ from local
-- execution".
-- Whether the shard can resolve the key is decided by walking the `JOIN`'s left table expression, so
-- the decision must not depend on the kind of that expression. Each case below puts the real `id`
-- behind a different kind - a subquery's projection, the projection of a `UNION ALL`, the other
-- branch of a `JOIN`, another member of a `CROSS JOIN`, the columns of a table function - and the key
-- is shipped every time; the case with no `id` anywhere on the left is the one that is rejected.
-- The shipped `USING (id)` carries no nested alias, so the shard joins by the real `id`: the last
-- case of the file runs the same query without a `Distributed` table and gets a different answer.

DROP TABLE IF EXISTS shard_kinds;
DROP TABLE IF EXISTS dist_kinds;
DROP TABLE IF EXISTS other_kinds;
DROP TABLE IF EXISTS no_key_kinds;

-- The shipped table itself has no `id`, so the key can only come from the rest of the left side.
CREATE TABLE shard_kinds (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_kinds VALUES (1);
CREATE TABLE dist_kinds AS shard_kinds
    ENGINE = Distributed('test_shard_localhost', currentDatabase(), shard_kinds, rand());
CREATE TABLE other_kinds (k UInt64, id UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO other_kinds VALUES (1, 5);
CREATE TABLE no_key_kinds (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO no_key_kinds VALUES (1);

SELECT 'the key comes from a subquery in the left JOIN';
SELECT sum(d.x + 10 AS id), min(s.id)
FROM dist_kinds AS d
JOIN (SELECT 1 AS k, 5 AS id) AS s ON d.x = s.k
JOIN (SELECT 5 AS id) AS t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT 'the key comes from a UNION ALL in the left JOIN';
SELECT sum(d.x + 10 AS id), min(u.id)
FROM dist_kinds AS d
JOIN (SELECT 1 AS k, 5 AS id UNION ALL SELECT 2 AS k, 6 AS id) AS u ON d.x = u.k
JOIN (SELECT 5 AS id) AS t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT 'the key comes from a table function in the left JOIN';
SELECT sum(d.x + 10 AS id)
FROM dist_kinds AS d
JOIN values('k UInt64, id UInt64', (1, 5)) AS v ON d.x = v.k
JOIN (SELECT 5 AS id) AS t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT 'the key comes from the other member of a CROSS JOIN';
SELECT sum(d.x + 10 AS id)
FROM dist_kinds AS d
CROSS JOIN other_kinds AS s
JOIN (SELECT 5 AS id) AS t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

-- Nothing on the left side has an `id`, so the shard would receive a `USING (id)` it cannot resolve
-- at all. That is the one case the initiator rejects, before anything is sent.
SELECT 'no key anywhere on the left side';
SELECT sum(d.x + 10 AS id)
FROM dist_kinds AS d
CROSS JOIN no_key_kinds AS s
JOIN (SELECT 5 AS id) AS t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError UNSUPPORTED_METHOD }

-- The same shape without the `Distributed` table: nothing is shipped, the nested alias stays the key,
-- and `11 = 5` does not hold, so no row joins. This is the divergence the setting description names.
SELECT 'the same subquery case with no Distributed table';
SELECT sum(d.x + 10 AS id), min(s.id)
FROM shard_kinds AS d
JOIN (SELECT 1 AS k, 5 AS id) AS s ON d.x = s.k
JOIN (SELECT 5 AS id) AS t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

DROP TABLE no_key_kinds;
DROP TABLE other_kinds;
DROP TABLE dist_kinds;
DROP TABLE shard_kinds;
