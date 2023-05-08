DROP TABLE IF EXISTS test09876_ELH;
DROP TABLE IF EXISTS test09876_CH;
CREATE TABLE test09876_ELH (simple String, one_excess String, two_important String) ENGINE=Memory;
CREATE TABLE test09876_CH (simple String, one_excess String, two_important String) ENGINE=Memory;
INSERT INTO test09876_ELH VALUES ('a', 'aw', 'aaw'), ('b', 'bw', 'abw'), ('c', 'cw', 'baw');
INSERT INTO test09876_CH VALUES ('a', 'a', 'aa'), ('b', 'b', 'ab'), ('c', 'c', 'ba');

SELECT trainEntropyLearnedHash(simple, 'id1') FROM test09876_ELH;
SELECT entropyLearnedHash(simple, 'id1') FROM test09876_ELH;
SELECT cityHash64(simple) FROM test09876_CH;

SELECT trainEntropyLearnedHash(one_excess, 'id2') FROM test09876_ELH;
SELECT entropyLearnedHash(one_excess, 'id2') FROM test09876_ELH;
SELECT cityHash64(one_excess) FROM test09876_CH;

SELECT trainEntropyLearnedHash(two_important, 'id3') FROM test09876_ELH;
SELECT entropyLearnedHash(two_important, 'id3') FROM test09876_ELH;
SELECT cityHash64(two_important) FROM test09876_CH;
