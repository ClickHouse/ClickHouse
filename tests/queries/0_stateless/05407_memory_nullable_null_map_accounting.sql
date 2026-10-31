-- A `Nullable` column without NULLs may have no null map, and a reading query may create it lazily
-- in the shared stored column. The accounted bytes of the stored `Memory` blocks must stay consistent,
-- so that the FIFO eviction does not make `total_bytes` underflow.

DROP TABLE IF EXISTS m;
CREATE TABLE m (x Nullable(UInt8), a Array(Nullable(UInt8))) ENGINE = Memory SETTINGS max_rows_to_keep = 10000;
INSERT INTO m SELECT toUInt8(number % 10), [toUInt8(number % 10)] FROM numbers(10000) SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000;
SELECT count() FROM m WHERE NOT ignore(if(x, 1, 0), arrayMap(e -> if(e, 1, 0), a)) SETTINGS compile_expressions = 0;
INSERT INTO m SELECT toUInt8(1), [toUInt8(1)];
SELECT total_rows, total_bytes < 1000000 FROM system.tables WHERE database = currentDatabase() AND name = 'm';
DROP TABLE m;
