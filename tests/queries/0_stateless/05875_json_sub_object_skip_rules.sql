SET enable_json_type = 1;

DROP TABLE IF EXISTS t_sub_object_skip_mt;
DROP TABLE IF EXISTS t_sub_object_skip_mem;

CREATE TABLE t_sub_object_skip_mt
(
    id UInt8,
    j0 JSON(a.b UInt32, SKIP REGEXP '^x$'),
    j1 JSON(a.b UInt32, SKIP b),
    j2 JSON(a.b UInt32, SKIP REGEXP '^b$')
)
ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_sub_object_skip_mem AS t_sub_object_skip_mt ENGINE = Memory;

INSERT INTO t_sub_object_skip_mt VALUES (1, '{"a":{"b":1,"c":"x"},"b":"y"}', '{"a":{"b":1,"c":"x"},"b":"y"}', '{"a":{"b":1,"c":"x"},"b":"y"}');
INSERT INTO t_sub_object_skip_mem SELECT * FROM t_sub_object_skip_mt;

SELECT 'MergeTree';
SELECT j0.^a, j0.@a, j1.^a, j1.@a, j2.^a, j2.@a FROM t_sub_object_skip_mt;
SELECT 'Memory';
SELECT j0.^a, j0.@a, j1.^a, j1.@a, j2.^a, j2.@a FROM t_sub_object_skip_mem;
SELECT 'types';
SELECT toTypeName(j0.^a), toTypeName(j1.^a), toTypeName(j2.^a), dynamicType(j1.@a) FROM t_sub_object_skip_mt;
SELECT 'functions';
SELECT JSONHas(j1, 'a'), JSONHas(j2, 'a'), JSONExtractRaw(j2, 'a') FROM t_sub_object_skip_mt;
SELECT 'nested';
SELECT j.arr[].^a FROM (SELECT '{"arr":[{"a":{"b":1},"b":2}]}'::JSON(arr Array(JSON(a.b UInt32, SKIP b))) AS j);

-- A typed path that a SKIP rule of the same type matches is still rejected.
SELECT '{}'::JSON(b UInt32, SKIP b); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_sub_object_skip_mt;
DROP TABLE t_sub_object_skip_mem;
