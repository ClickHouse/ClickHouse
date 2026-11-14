-- Tags: zookeeper, no-replicated-database, no-ordinary-database
-- `ATTACH TABLE ... AS REPLICATED` must not convert a table with a `LOOKUP INDEX`, which is not supported for
-- `ReplicatedMergeTree`, and the failed statement must leave the table definition unchanged.

SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS lookup_to_replicated;

CREATE TABLE lookup_to_replicated (id UInt64, LOOKUP INDEX s (id) TYPE table_set) ENGINE = MergeTree ORDER BY id;
INSERT INTO lookup_to_replicated VALUES (1);

DETACH TABLE lookup_to_replicated;
ATTACH TABLE lookup_to_replicated AS REPLICATED; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE lookup_to_replicated;

SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lookup_to_replicated';
SELECT * FROM lookup_to_replicated;

DROP TABLE lookup_to_replicated;
