DROP TABLE IF EXISTS squality_explicit_null_in_remote_plan;
CREATE TABLE squality_explicit_null_in_remote_plan (k Nullable(UInt8)) ENGINE = Memory;
INSERT INTO squality_explicit_null_in_remote_plan VALUES (NULL), (1), (2);

SELECT count()
FROM remote('127.0.0.1', currentDatabase(), squality_explicit_null_in_remote_plan)
WHERE nullIn(k, (SELECT CAST(NULL AS Nullable(UInt8))))
SETTINGS in_null_as_unknown = 1, transform_null_in = 0,
    serialize_query_plan = 0, prefer_localhost_replica = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT count()
FROM remote('127.0.0.1', currentDatabase(), squality_explicit_null_in_remote_plan)
WHERE nullIn(k, (SELECT CAST(NULL AS Nullable(UInt8))))
SETTINGS in_null_as_unknown = 1, transform_null_in = 0,
    serialize_query_plan = 1, prefer_localhost_replica = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE squality_explicit_null_in_remote_plan;
