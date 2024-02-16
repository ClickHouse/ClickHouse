DROP TABLE IF EXISTS test.hits_dst;
DROP TABLE IF EXISTS test.hits_buffer;

CREATE TABLE test.hits_dst AS test.hits
ENGINE = MergeTree
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy = 'default';

CREATE TABLE test.hits_buffer AS test.hits_dst ENGINE = Buffer(test, hits_dst, 8, 600, 600, 1000000, 1000000, 100000000, 1000000000);

INSERT INTO test.hits_buffer SELECT * FROM test.hits WHERE CounterID = 800784;
SELECT count() FROM test.hits_buffer;
SELECT count() FROM test.hits_dst;

OPTIMIZE TABLE test.hits_buffer;
SELECT count() FROM test.hits_buffer;
SELECT count() FROM test.hits_dst;

DROP TABLE test.hits_dst;
DROP TABLE test.hits_buffer;
