DROP TABLE IF EXISTS test.arrow;

SET max_threads = 1, max_block_size = 8192, min_insert_block_size_rows = 8192, min_insert_block_size_bytes = 1048576; -- lower memory usage

CREATE TABLE test.arrow AS test.hits ENGINE = File(Arrow);
INSERT INTO test.arrow SELECT * FROM test.hits LIMIT 10000;

SELECT sum(cityHash64(*)) FROM (SELECT * FROM test.hits LIMIT 10000);
SELECT sum(cityHash64(*)) FROM test.arrow;

DROP TABLE test.arrow;
