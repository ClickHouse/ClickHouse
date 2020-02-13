DROP TABLE IF EXISTS test.merge_hits;
CREATE TABLE IF NOT EXISTS test.merge_hits AS test.hits ENGINE = Merge(test, '^hits$');
SELECT count() FROM test.merge_hits WHERE AdvEngineID = 2;
SELECT count() FROM test.merge_hits PREWHERE AdvEngineID = 2;
DROP TABLE test.merge_hits;
