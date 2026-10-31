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

INSERT INTO test_05401 SETTINGS max_block_size = 100 VALUES (1), (2);

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

SET session_timezone = 'UTC';

CREATE TABLE test_05401
(
    type UInt8,
    PROJECTION p_timezone (SELECT any(toHour(toDateTime(0)))),
    PROJECTION p_dynamic (SELECT any(CAST(toUInt8(1), 'Dynamic'))),
    PROJECTION p_variant (SELECT any(CAST(toDate(1), 'Variant(Date, UInt64)')))
)
ENGINE = MergeTree()
ORDER BY type;

INSERT INTO test_05401 VALUES (1), (2);

SELECT any(toHour(toDateTime(0))) FROM test_05401
SETTINGS session_timezone = 'Europe/Berlin', optimize_use_projections = 0;
SELECT any(toHour(toDateTime(0))) FROM test_05401
SETTINGS session_timezone = 'Europe/Berlin', prefer_optimize_projection = 1;

SELECT dynamicType(any(CAST(toUInt64(1), 'Dynamic'))) FROM test_05401
SETTINGS optimize_use_projections = 0;
SELECT dynamicType(any(CAST(toUInt64(1), 'Dynamic'))) FROM test_05401
SETTINGS prefer_optimize_projection = 1;

SELECT variantType(any(CAST(toUInt64(1), 'Variant(Date, UInt64)'))) FROM test_05401
SETTINGS optimize_use_projections = 0;
SELECT variantType(any(CAST(toUInt64(1), 'Variant(Date, UInt64)'))) FROM test_05401
SETTINGS prefer_optimize_projection = 1;

DROP TABLE test_05401;

SET cast_keep_nullable = 1;

CREATE TABLE test_05401
(
    type Nullable(UInt8),
    PROJECTION p_cast
    (
        SELECT countIf(isNull(CAST(type, 'UInt8'))),
               countIf(isNotNull(CAST(type, 'UInt8')))
    )
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO test_05401 VALUES (NULL), (1);

SELECT countIf(isNull(CAST(type, 'UInt8'))), countIf(isNotNull(CAST(type, 'UInt8')))
FROM test_05401 SETTINGS cast_keep_nullable = 0, optimize_use_projections = 0;
SELECT countIf(isNull(CAST(type, 'UInt8'))), countIf(isNotNull(CAST(type, 'UInt8')))
FROM test_05401 SETTINGS cast_keep_nullable = 0, prefer_optimize_projection = 1;

DROP TABLE test_05401;
