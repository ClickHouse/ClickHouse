-- Test memory scaling with multiple tables
CREATE TABLE test_mt_2 AS test_mt;
CREATE TABLE test_mt_3 AS test_mt;

-- Prevent background merge selection from crossing the heap-dump checkpoint.
SYSTEM STOP MERGES test_mt_2;
SYSTEM STOP MERGES test_mt_3;

INSERT INTO test_mt_2 SELECT * FROM test_mt;
INSERT INTO test_mt_3 SELECT * FROM test_mt;
