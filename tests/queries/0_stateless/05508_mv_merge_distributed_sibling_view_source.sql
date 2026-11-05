-- A materialized view and the `RETURNING` query of an `INSERT` read their source table again through `merge`.
-- With a `Distributed` sibling, `merge` analyzes the query for each child table again. This analysis has
-- the inserted block in the context, but it reads the real table. So it must use the real table's metadata,
-- with its primary key. Before the fix, the server crashed in `KeyCondition`.
-- The counts use only the rows inserted before (`id >= 100`), so they do not depend on whether the view sees the inserted rows.
-- `IN (SELECT ... FROM src)` and scalar subqueries over `src` read the inserted block, also through `merge`.

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS other;
DROP TABLE IF EXISTS src;

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE other (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist (id UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), other);

INSERT INTO src VALUES (100), (101), (102);
INSERT INTO other VALUES (200);

SELECT 'RETURNING';
INSERT INTO src RETURNING (SELECT id, (SELECT count() FROM merge(currentDatabase(), '^src$|^dist$') WHERE id >= 100)) VALUES (3);
INSERT INTO src RETURNING (SELECT id, (SELECT countIf(id >= 100) FROM merge(currentDatabase(), '^src$|^dist$'))) VALUES (4);
INSERT INTO src RETURNING (SELECT count() FROM merge(currentDatabase(), '^src$|^dist$') WHERE id >= 100) VALUES (5);

CREATE TABLE dst
(
    id UInt64,
    in_src UInt8,
    count_src UInt64,
    merge_src_where UInt64,
    merge_where UInt64,
    merge_if UInt64,
    merge_src_in UInt64,
    merge_in UInt64
)
ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv TO dst AS
SELECT
    id,
    id IN (SELECT id FROM src) AS in_src,
    (SELECT count() FROM src) AS count_src,
    (SELECT count() FROM merge(currentDatabase(), '^src$') WHERE id >= 100) AS merge_src_where,
    (SELECT count() FROM merge(currentDatabase(), '^src$|^dist$') WHERE id >= 100) AS merge_where,
    (SELECT countIf(id >= 100) FROM merge(currentDatabase(), '^src$|^dist$')) AS merge_if,
    (SELECT count() FROM merge(currentDatabase(), '^src$') WHERE id IN (SELECT if(id < 100, id + 100, id) FROM src)) AS merge_src_in,
    (SELECT count() FROM merge(currentDatabase(), '^src$|^dist$') WHERE id IN (SELECT if(id < 100, id + 100, id) FROM src)) AS merge_in
FROM src;

SELECT 'materialized view';
INSERT INTO src VALUES (1), (2);
SELECT * FROM dst ORDER BY id;

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE dist;
DROP TABLE other;
DROP TABLE src;
