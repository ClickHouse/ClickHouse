-- Reads of Variant and JSON columns with runs of NULLs of different lengths, in blocks that start
-- inside a run. Every read is compared with the values computed at insert time. The checks are
-- separate countIf() terms: a short-circuit OR would filter the column and hide wrong offsets.

DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_basic;

CREATE TABLE t_compact (id UInt64, v Variant(UInt64, String), ref Nullable(String), rtype String, j JSON, jref String)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 37, use_compact_variant_discriminators_serialization = 1;

CREATE TABLE t_basic (id UInt64, v Variant(UInt64, String), ref Nullable(String), rtype String)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 37, use_compact_variant_discriminators_serialization = 0;

-- A row is non-NULL iff id % p = 0, so runs of NULLs are 0, 13, 26, 39, 52, 65 or 78 rows long.
INSERT INTO t_compact
SELECT id,
    if(id % p = 0, if(id % 2 = 0, id::Variant(UInt64, String), toString(id)::Variant(String)::Variant(UInt64, String)), NULL),
    if(id % p = 0, toString(id), NULL),
    if(id % p = 0, if(id % 2 = 0, 'UInt64', 'String'), 'None'),
    toJSONString(map('k' || toString(id % 50), id)),
    toString(toJSONString(map('k' || toString(id % 50), id))::JSON)
FROM (SELECT number AS id, [1, 14, 27, 40, 53, 66, 79][intDiv(number, 1000) % 7 + 1] AS p FROM numbers(7000));

INSERT INTO t_basic
SELECT id,
    if(id % p = 0, if(id % 2 = 0, id::Variant(UInt64, String), toString(id)::Variant(String)::Variant(UInt64, String)), NULL),
    if(id % p = 0, toString(id), NULL),
    if(id % p = 0, if(id % 2 = 0, 'UInt64', 'String'), 'None')
FROM (SELECT number AS id, [1, 14, 27, 40, 53, 66, 79][intDiv(number, 1000) % 7 + 1] AS p FROM numbers(7000));

SELECT count(), countIf(v IS NOT NULL),
    countIf(isNull(v) != isNull(ref)), countIf(toString(v) != ref), countIf(toString(variantType(v)) != rtype),
    countIf(toString(j) != jref)
FROM t_compact;
SELECT count(), countIf(v IS NOT NULL),
    countIf(isNull(v) != isNull(ref)), countIf(toString(v) != ref), countIf(toString(variantType(v)) != rtype)
FROM t_compact SETTINGS max_block_size = 13;
SELECT count(), countIf(v IS NOT NULL),
    countIf(isNull(v) != isNull(ref)), countIf(toString(v) != ref), countIf(toString(variantType(v)) != rtype),
    countIf(toString(j) != jref)
FROM t_compact WHERE (id BETWEEN 100 AND 1100) OR (id BETWEEN 2011 AND 3500) OR id > 4400 SETTINGS max_block_size = 65505;

SELECT count(), countIf(v IS NOT NULL),
    countIf(isNull(v) != isNull(ref)), countIf(toString(v) != ref), countIf(toString(variantType(v)) != rtype)
FROM t_basic;
SELECT count(), countIf(v IS NOT NULL),
    countIf(isNull(v) != isNull(ref)), countIf(toString(v) != ref), countIf(toString(variantType(v)) != rtype)
FROM t_basic SETTINGS max_block_size = 13;
SELECT count(), countIf(v IS NOT NULL),
    countIf(isNull(v) != isNull(ref)), countIf(toString(v) != ref), countIf(toString(variantType(v)) != rtype)
FROM t_basic WHERE (id BETWEEN 100 AND 1100) OR (id BETWEEN 2011 AND 3500) OR id > 4400 SETTINGS max_block_size = 65505;

DROP TABLE t_compact;
DROP TABLE t_basic;
