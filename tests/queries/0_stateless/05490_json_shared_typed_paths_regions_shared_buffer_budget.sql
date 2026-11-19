-- The JSON columns of a part write share the memory budget of the buffered shared data: one JSON column of about 5 MB
-- fits into a budget of 7 MiB, two such columns of a merge do not and spill. The granules are much smaller than the
-- part, so that the buffers of both columns grow together during the merge.

DROP TABLE IF EXISTS t_budget_one;
DROP TABLE IF EXISTS t_budget_two;

CREATE TABLE t_budget_one (x UInt64, j1 JSON(SHARED a UInt64, SHARED b String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192,
         index_granularity = 1024, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 7340032;

CREATE TABLE t_budget_two (x UInt64, j1 JSON(SHARED a UInt64, SHARED b String), j2 JSON(SHARED a UInt64, SHARED b String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192,
         index_granularity = 1024, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 7340032;

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{"a":', toString(number), ',"b":"', repeat('b', 100), toString(number % 10), '"}') AS json
FROM numbers(40000);

INSERT INTO t_budget_one SELECT x, json FROM src WHERE x < 20000;
INSERT INTO t_budget_one SELECT x, json FROM src WHERE x >= 20000;
INSERT INTO t_budget_two SELECT x, json, json FROM src WHERE x < 20000;
INSERT INTO t_budget_two SELECT x, json, json FROM src WHERE x >= 20000;
OPTIMIZE TABLE t_budget_one FINAL;
OPTIMIZE TABLE t_budget_two FINAL;

SELECT 'data', (SELECT sum(cityHash64(x, toString(j1))) FROM t_budget_two) = (SELECT sum(cityHash64(x, toString(json::JSON(SHARED a UInt64, SHARED b String)))) FROM src),
    (SELECT sum(cityHash64(x, toString(j2))) FROM t_budget_two) = (SELECT sum(cityHash64(x, toString(json::JSON(SHARED a UInt64, SHARED b String)))) FROM src),
    (SELECT sum(cityHash64(x, toString(j1))) FROM t_budget_one) = (SELECT sum(cityHash64(x, toString(json::JSON(SHARED a UInt64, SHARED b String)))) FROM src);

SYSTEM FLUSH LOGS part_log;

WITH (
    SELECT groupArray((table, ProfileEvents['SerializationSpilledUncompressedBytes']))
    FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_budget_one', 't_budget_two') AND event_type = 'MergeParts'
        AND (table, part_name) IN (SELECT table, name FROM system.parts WHERE database = currentDatabase() AND active)
) AS spilled
SELECT 'spilled', arrayFirst(e -> e.1 = 't_budget_one', spilled).2 = 0, arrayFirst(e -> e.1 = 't_budget_two', spilled).2 > 0;

DROP TABLE t_budget_one;
DROP TABLE t_budget_two;
