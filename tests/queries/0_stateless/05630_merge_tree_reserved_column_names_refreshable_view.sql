-- Tags: no-ordinary-database, no-replicated-database
-- no-replicated-database: a refreshable view with a non-replicated target is refused in a Replicated database.

-- A refreshable materialized view re-creates its target from the stored definition on every refresh.
DROP TABLE IF EXISTS rmv;
DROP TABLE IF EXISTS src;
CREATE TABLE src (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO src VALUES (1);

SET allow_reserved_column_names = 1;
CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 YEAR ENGINE = MergeTree ORDER BY x AS SELECT x, _part FROM src;
SET allow_reserved_column_names = 0;

SYSTEM WAIT VIEW rmv;
SYSTEM REFRESH VIEW rmv;
SYSTEM WAIT VIEW rmv;
SELECT x FROM rmv;

DROP TABLE rmv;
DROP TABLE src;
