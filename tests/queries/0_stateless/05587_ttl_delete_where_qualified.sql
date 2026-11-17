-- Tags: no-replicated-database
-- A TTL `DELETE WHERE v IN cf_src` with a bare table name used to be stored unqualified, and TTL
-- merges analyze the stored predicate in a context with no current database, so every merge of a
-- part with expired rows failed with UNKNOWN_DATABASE and the expired rows were kept forever.
-- The predicate is now stored with the database the CREATE session resolved the name against.

DROP TABLE IF EXISTS ttl_src_05587;
DROP TABLE IF EXISTS ttl_tw_05587;

CREATE TABLE ttl_src_05587 (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO ttl_src_05587 VALUES (100), (200);

CREATE TABLE ttl_tw_05587 (d DateTime, v UInt64) ENGINE = MergeTree ORDER BY d
    TTL d + INTERVAL 1 SECOND DELETE WHERE v IN ttl_src_05587;

INSERT INTO ttl_tw_05587 SELECT now() - INTERVAL 1 HOUR, number + 100 FROM numbers(10);

-- The stored definition carries the qualified name now.
SELECT countSubstrings(create_table_query, 'ttl_src_05587') > 0 AND countSubstrings(create_table_query, 'v IN ttl_src_05587') = 0 AS stored_qualified
FROM system.tables WHERE database = currentDatabase() AND name = 'ttl_tw_05587';

-- The merge can build the set and delete the expired row.
OPTIMIZE TABLE ttl_tw_05587 FINAL;
SELECT count() AS rows_after_ttl_delete FROM ttl_tw_05587;

-- The qualified spelling keeps working end to end as before.
ALTER TABLE ttl_tw_05587 MODIFY TTL d + INTERVAL 1 SECOND DELETE WHERE v IN ttl_src_05587;
INSERT INTO ttl_tw_05587 SELECT now() - INTERVAL 1 HOUR, 300;
OPTIMIZE TABLE ttl_tw_05587 FINAL;
SELECT count() AS rows_after_second_ttl_delete FROM ttl_tw_05587;

DROP TABLE ttl_tw_05587;
DROP TABLE ttl_src_05587;
