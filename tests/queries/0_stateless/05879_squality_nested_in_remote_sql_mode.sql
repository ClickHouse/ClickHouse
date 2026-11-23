DROP TABLE IF EXISTS squality_nested_in_remote_sql_mode;
CREATE TABLE squality_nested_in_remote_sql_mode (k Nullable(UInt8)) ENGINE = Memory;
INSERT INTO squality_nested_in_remote_sql_mode VALUES (NULL), (1), (2);

SELECT count() FROM
(
    SELECT * FROM squality_nested_in_remote_sql_mode SETTINGS in_null_as_unknown = 0
)
WHERE isNull(k IN (1, NULL))
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM
(
    SELECT * FROM remote('127.0.0.1', currentDatabase(), squality_nested_in_remote_sql_mode)
    SETTINGS in_null_as_unknown = 0
)
WHERE isNull(k IN (1, NULL))
SETTINGS in_null_as_unknown = 1, serialize_query_plan = 0,
    prefer_localhost_replica = 0, enable_parallel_replicas = 0,
    automatic_parallel_replicas_mode = 0;

SELECT count() FROM
(
    SELECT * FROM remote('127.0.0.1', currentDatabase(), squality_nested_in_remote_sql_mode)
    SETTINGS in_null_as_unknown = 0
)
WHERE isNull(k IN (1, NULL))
SETTINGS in_null_as_unknown = 1, serialize_query_plan = 1,
    prefer_localhost_replica = 0, enable_parallel_replicas = 0,
    automatic_parallel_replicas_mode = 0;

DROP TABLE squality_nested_in_remote_sql_mode;
