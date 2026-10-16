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

CREATE TABLE test_05401
(
    type UInt8,
    PROJECTION p_constants
    (
        SELECT sum(toUInt64(getSetting('max_block_size'))),
               any(toFloat64('0'))),
    PROJECTION p_containers
    (
        SELECT any([toFloat64('0')]),
               any(tuple(toFloat64('0'))),
               any(map('key', toFloat64('0')))
    )
)
ENGINE = MergeTree()
ORDER BY type;

INSERT INTO test_05401 VALUES (1), (2) SETTINGS max_block_size = 100;

SELECT sum(toUInt64(getSetting('max_block_size'))) FROM test_05401
SETTINGS max_block_size = 200, optimize_use_projections = 0;
SELECT sum(toUInt64(getSetting('max_block_size'))) FROM test_05401
SETTINGS max_block_size = 200, prefer_optimize_projection = 1;

SELECT 1 / any(toFloat64('-0')) FROM test_05401 SETTINGS optimize_use_projections = 0;
SELECT 1 / any(toFloat64('-0')) FROM test_05401 SETTINGS prefer_optimize_projection = 1;

SELECT 1 / any([toFloat64('-0')])[1],
       1 / tupleElement(any(tuple(toFloat64('-0'))), 1),
       1 / any(map('key', toFloat64('-0')))['key']
FROM test_05401 SETTINGS optimize_use_projections = 0;
SELECT 1 / any([toFloat64('-0')])[1],
       1 / tupleElement(any(tuple(toFloat64('-0'))), 1),
       1 / any(map('key', toFloat64('-0')))['key']
FROM test_05401 SETTINGS prefer_optimize_projection = 1;

DROP TABLE test_05401;
