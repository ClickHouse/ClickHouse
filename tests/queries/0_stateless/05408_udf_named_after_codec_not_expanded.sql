-- Tags: no-parallel
-- The UDF is a server-global object whose name has to be a codec name.

-- Codec elements are names, not function calls, so a UDF of the same name must not be expanded into them.
DROP FUNCTION IF EXISTS LZ4HC;
CREATE FUNCTION LZ4HC AS x -> x + 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (d DateTime, x UInt64 CODEC(LZ4HC))
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(1) RECOMPRESS CODEC(LZ4HC(1));

ALTER TABLE t MODIFY COLUMN x UInt64 CODEC(LZ4HC(1));
-- The last argument is an expression, so the UDF does apply there.
ALTER TABLE t MODIFY TTL d + toIntervalDay(LZ4HC(1)) RECOMPRESS CODEC(LZ4HC);

SELECT
    countSubstrings(create_table_query, 'CODEC(LZ4HC(1))'),
    countSubstrings(create_table_query, 'RECOMPRESS CODEC(LZ4HC)'),
    countSubstrings(create_table_query, 'toIntervalDay(1 + 1)')
FROM system.tables WHERE database = currentDatabase() AND name = 't';

DROP TABLE t;
DROP FUNCTION LZ4HC;
