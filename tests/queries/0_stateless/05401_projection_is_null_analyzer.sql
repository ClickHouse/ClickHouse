-- https://github.com/ClickHouse/ClickHouse/issues/68949

DROP TABLE IF EXISTS test_05401;

CREATE TABLE test_05401
(
    type String
)
ENGINE = MergeTree()
ORDER BY type;

ALTER TABLE test_05401 ADD PROJECTION p_05401_1 (SELECT countIf(type IS NULL));
ALTER TABLE test_05401 ADD PROJECTION p_05401_2 (SELECT countIf(type IS NOT NULL));

INSERT INTO test_05401 VALUES ('apple'), ('pear');

SELECT countIf(type IS NULL) FROM test_05401 SETTINGS force_optimize_projection = 1;
SELECT countIf(type IS NOT NULL) FROM test_05401 SETTINGS force_optimize_projection = 1;

DROP TABLE test_05401;
