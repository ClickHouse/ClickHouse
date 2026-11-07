-- A `Nullable` column without NULLs may have no null map, and a reading query may create it lazily
-- in the shared buffered column. The accounted bytes of the `Buffer` must stay consistent,
-- so that flushing the buffer does not make `total_bytes` underflow.

DROP TABLE IF EXISTS b;
CREATE TABLE b (x Nullable(UInt8), a Array(Nullable(UInt8))) ENGINE = Buffer('', '', 1, 1000000, 1000000, 1000000000, 1000000000, 1000000000, 1000000000);
INSERT INTO b SELECT toUInt8(number % 10), [toUInt8(number % 10)] FROM numbers(65536);
SELECT count() FROM b WHERE NOT ignore(if(x, 1, 0), arrayMap(e -> if(e, 1, 0), a)) SETTINGS compile_expressions = 0;
OPTIMIZE TABLE b;
SELECT total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 'b';
DROP TABLE b;
