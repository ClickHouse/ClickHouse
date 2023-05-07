DROP TABLE IF EXISTS test09876_1;
CREATE TABLE test09876_1 (x String) ENGINE=Memory;
INSERT INTO test09876_1 VALUES ('a'), ('b'), ('c');
SELECT trainEntropyLearnedHash(x, 'id1') FROM test09876_1;
SELECT entropyLearnedHash(x, 'id1') FROM test09876_1;
SELECT cityHash64(x) FROM test09876_1;

DROP TABLE IF EXISTS test09876_2;
CREATE TABLE test09876_2 (x String) ENGINE=Memory;
INSERT INTO test09876_2 VALUES ('aa'), ('ba'), ('ca');
SELECT trainEntropyLearnedHash(x, 'id1') FROM test09876_2;
SELECT entropyLearnedHash(x, 'id1') FROM test09876_2;
SELECT cityHash64(x) FROM test09876_1;
