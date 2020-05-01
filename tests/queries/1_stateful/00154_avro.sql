DROP TABLE IF EXISTS test.avro;

SET max_threads = 1, max_block_size = 8192, min_insert_block_size_rows = 8192, min_insert_block_size_bytes = 1048576; -- lower memory usage

CREATE TABLE test.avro AS test.hits ENGINE = File(Avro);
INSERT INTO test.avro SELECT * FROM test.hits WHERE intHash64(WatchID) % 100 = 0;

SELECT sum(cityHash64(*)) FROM test.hits WHERE intHash64(WatchID) % 100 = 0;
SELECT sum(cityHash64(*)) FROM test.avro;

DROP TABLE test.avro;
