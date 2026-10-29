-- Tags: zookeeper
-- Tag zookeeper: the test creates a Replicated database.

-- A refresh without APPEND re-creates the inner table from its stored definition, subcolumn codecs included.
-- The refresh does not run with the settings of the session that created the view.

SET enable_subcolumn_codecs = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier} SYNC;

SELECT 'Atomic';
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR
    (id UInt64, p Tuple(a UInt64, b String), CODEC p.a (Delta, ZSTD(1)))
    ENGINE = MergeTree ORDER BY id EMPTY
    AS SELECT number AS id, (number, toString(number)) AS p FROM numbers(10);
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT count(), sum(p.a) FROM mv;
SELECT subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner%' AND name = 'p';

SELECT 'Replicated';
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Replicated('/clickhouse/05452_refreshable_mv_subcolumn_codecs/{database}', 'shard1', 'replica1') FORMAT Null;
USE {CLICKHOUSE_DATABASE_2:Identifier};
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR
    (id UInt64, p Tuple(a UInt64, b String), CODEC p.a (Delta, ZSTD(1)))
    ENGINE = ReplicatedMergeTree ORDER BY id EMPTY
    AS SELECT number AS id, (number, toString(number)) AS p FROM numbers(10) FORMAT Null;
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT count(), sum(p.a) FROM mv;
SELECT subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner%' AND name = 'p';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} SYNC;
