-- Tags: no-fasttest, no-shared-merge-tree
-- no-fasttest: UNIQUE KEY needs RocksDB.
-- no-shared-merge-tree: UNIQUE KEY is supported only by plain MergeTree.

-- With insert deduplication, the database `max_rows` check is deferred to the commit of the part, which a table
-- with `UNIQUE KEY` does through its own path: a replay is accepted, and a new key is rejected.

SET enable_unique_key = 1;
SET async_insert = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_rows = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt32) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (2); -- { serverError TOO_MANY_ROWS }
-- The rejected block is not left in the deduplication log, so its retry is rejected again.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (2); -- { serverError TOO_MANY_ROWS }
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
