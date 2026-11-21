-- Inserts and merges of Variant, Dynamic and JSON columns with runs of NULLs shorter and longer than 32 rows,
-- in ranges shorter and longer than 32 rows. Every read is compared with the values computed from id. The checks
-- are separate countIf() terms: a short-circuit AND/OR would filter the column and hide wrong offsets.

DROP TABLE IF EXISTS t_vw;
DROP TABLE IF EXISTS t_vm;

CREATE TABLE t_vw (id UInt64, v Variant(UInt64, String), d Dynamic, d1 Dynamic(max_types = 1), j JSON,
    ref Nullable(String), rtype String, dtype String, jref String)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 37, min_bytes_for_wide_part = 0;

-- A row is non-NULL iff id % p = 0, so runs of NULLs are 0, 13, 26, 32, 39, 65 or 78 rows long.
-- d holds only UInt64 for id < 1000, so the squashed block has a Dynamic whose local and global variant orders differ.
INSERT INTO t_vw
SELECT id,
    if(nn, if(u, id::Variant(UInt64, String), toString(id)::Variant(String)::Variant(UInt64, String)), NULL),
    if(nn, if(du, id::Dynamic, toString(id)::Dynamic), NULL),
    if(nn, if(du, id::Dynamic, toString(id)::Dynamic), NULL),
    js,
    if(nn, toString(id), NULL),
    if(nn, if(u, 'UInt64', 'String'), 'None'),
    if(nn, if(du, 'UInt64', 'String'), 'None'),
    toString(js::JSON)
FROM
(
    SELECT number AS id, id % [1, 14, 27, 33, 40, 66, 79][intDiv(id, 500) % 7 + 1] = 0 AS nn, id % 2 = 0 AS u,
        id < 1000 OR id % 2 = 0 AS du,
        if(id % 3 != 0, toJSONString(map('k' || toString(id % 64), id)), toJSONString(map('k' || toString(id % 64), toString(id)))) AS js
    FROM numbers(0, 3500)
)
SETTINGS max_block_size = 1000, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000,
    min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;

-- Squashing ranges shorter than 32 rows.
INSERT INTO t_vw
SELECT id,
    if(nn, if(u, id::Variant(UInt64, String), toString(id)::Variant(String)::Variant(UInt64, String)), NULL),
    if(nn, if(du, id::Dynamic, toString(id)::Dynamic), NULL),
    if(nn, if(du, id::Dynamic, toString(id)::Dynamic), NULL),
    js,
    if(nn, toString(id), NULL),
    if(nn, if(u, 'UInt64', 'String'), 'None'),
    if(nn, if(du, 'UInt64', 'String'), 'None'),
    toString(js::JSON)
FROM
(
    SELECT number AS id, id % [1, 14, 27, 33, 40, 66, 79][intDiv(id, 500) % 7 + 1] = 0 AS nn, id % 2 = 0 AS u,
        id < 1000 OR id % 2 = 0 AS du,
        if(id % 3 != 0, toJSONString(map('k' || toString(id % 64), id)), toJSONString(map('k' || toString(id % 64), toString(id)))) AS js
    FROM numbers(4000, 400)
)
SETTINGS max_block_size = 13, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000,
    min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;

-- One variant and no NULLs.
INSERT INTO t_vw
SELECT id, id::Variant(UInt64, String), id::Dynamic, id::Dynamic, toJSONString(map('k' || toString(id % 64), id)),
    toString(id), 'UInt64', 'UInt64', toString(toJSONString(map('k' || toString(id % 64), id))::JSON)
FROM (SELECT number AS id FROM numbers(5000, 500))
SETTINGS max_block_size = 250, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000,
    min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;

SELECT count(), countIf(isNotNull(v)),
    countIf(isNull(v) != isNull(ref)), countIf(ifNull(toString(v), '\N') != ifNull(ref, '\N')), countIf(toString(variantType(v)) != rtype),
    countIf(isNull(d) != isNull(ref)), countIf(ifNull(toString(d), '\N') != ifNull(ref, '\N')), countIf(toString(dynamicType(d)) != dtype),
    countIf(isNull(d1) != isNull(ref)), countIf(ifNull(toString(d1), '\N') != ifNull(ref, '\N')), countIf(toString(dynamicType(d1)) != dtype),
    countIf(toString(j) != jref), countIf(ifNull(toString(firstNonDefault(v, 'fb'::Variant(UInt64, String))), '') != ifNull(ref, 'fb'))
FROM t_vw SETTINGS short_circuit_function_evaluation = 'disable';

OPTIMIZE TABLE t_vw FINAL;

SELECT count(), countIf(isNotNull(v)),
    countIf(isNull(v) != isNull(ref)), countIf(ifNull(toString(v), '\N') != ifNull(ref, '\N')), countIf(toString(variantType(v)) != rtype),
    countIf(isNull(d) != isNull(ref)), countIf(ifNull(toString(d), '\N') != ifNull(ref, '\N')), countIf(toString(dynamicType(d)) != dtype),
    countIf(isNull(d1) != isNull(ref)), countIf(ifNull(toString(d1), '\N') != ifNull(ref, '\N')), countIf(toString(dynamicType(d1)) != dtype),
    countIf(toString(j) != jref), countIf(ifNull(toString(firstNonDefault(v, 'fb'::Variant(UInt64, String))), '') != ifNull(ref, 'fb'))
FROM t_vw SETTINGS short_circuit_function_evaluation = 'disable';

-- Inserted parts are smaller than one granule and written as whole columns. The vertical merge gathers runs of
-- 40 and 5 rows into a part of two granules.
CREATE TABLE t_vm (id UInt64, v Variant(UInt64, String), d Dynamic, ref Nullable(String), rtype String, dtype String)
ENGINE = MergeTree ORDER BY id
SETTINGS use_compact_variant_discriminators_serialization = 0, index_granularity = 1000, min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vm
SELECT id,
    if(nn, if(u, id::Variant(UInt64, String), toString(id)::Variant(String)::Variant(UInt64, String)), NULL),
    if(nn, if(du, id::Dynamic, toString(id)::Dynamic), NULL),
    if(nn, toString(id), NULL),
    if(nn, if(u, 'UInt64', 'String'), 'None'),
    if(nn, if(du, 'UInt64', 'String'), 'None')
FROM
(
    SELECT number AS id, id % [1, 14, 27][intDiv(id, 500) + 1] = 0 AS nn, id % 2 = 0 AS u, id < 100 OR id % 2 = 0 AS du,
        if(id < 1200, intDiv(id, 40), intDiv(id, 5)) % 3 AS part
    FROM numbers(1500)
)
WHERE part = 0
SETTINGS max_block_size = 100, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000,
    min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;

INSERT INTO t_vm
SELECT id,
    if(nn, if(u, id::Variant(UInt64, String), toString(id)::Variant(String)::Variant(UInt64, String)), NULL),
    if(nn, if(du, id::Dynamic, toString(id)::Dynamic), NULL),
    if(nn, toString(id), NULL),
    if(nn, if(u, 'UInt64', 'String'), 'None'),
    if(nn, if(du, 'UInt64', 'String'), 'None')
FROM
(
    SELECT number AS id, id % [1, 14, 27][intDiv(id, 500) + 1] = 0 AS nn, id % 2 = 0 AS u, id < 100 OR id % 2 = 0 AS du,
        if(id < 1200, intDiv(id, 40), intDiv(id, 5)) % 3 AS part
    FROM numbers(1500)
)
WHERE part = 1
SETTINGS max_block_size = 100, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000,
    min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;

INSERT INTO t_vm
SELECT id,
    if(nn, if(u, id::Variant(UInt64, String), toString(id)::Variant(String)::Variant(UInt64, String)), NULL),
    if(nn, if(du, id::Dynamic, toString(id)::Dynamic), NULL),
    if(nn, toString(id), NULL),
    if(nn, if(u, 'UInt64', 'String'), 'None'),
    if(nn, if(du, 'UInt64', 'String'), 'None')
FROM
(
    SELECT number AS id, id % [1, 14, 27][intDiv(id, 500) + 1] = 0 AS nn, id % 2 = 0 AS u, id < 100 OR id % 2 = 0 AS du,
        if(id < 1200, intDiv(id, 40), intDiv(id, 5)) % 3 AS part
    FROM numbers(1500)
)
WHERE part = 2
SETTINGS max_block_size = 100, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000,
    min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;

SELECT count(), countIf(isNotNull(v)),
    countIf(isNull(v) != isNull(ref)), countIf(ifNull(toString(v), '\N') != ifNull(ref, '\N')), countIf(toString(variantType(v)) != rtype),
    countIf(isNull(d) != isNull(ref)), countIf(ifNull(toString(d), '\N') != ifNull(ref, '\N')), countIf(toString(dynamicType(d)) != dtype)
FROM t_vm SETTINGS short_circuit_function_evaluation = 'disable';

OPTIMIZE TABLE t_vm FINAL;

SELECT count(), countIf(isNotNull(v)),
    countIf(isNull(v) != isNull(ref)), countIf(ifNull(toString(v), '\N') != ifNull(ref, '\N')), countIf(toString(variantType(v)) != rtype),
    countIf(isNull(d) != isNull(ref)), countIf(ifNull(toString(d), '\N') != ifNull(ref, '\N')), countIf(toString(dynamicType(d)) != dtype)
FROM t_vm SETTINGS short_circuit_function_evaluation = 'disable';

DROP TABLE t_vw;
DROP TABLE t_vm;
