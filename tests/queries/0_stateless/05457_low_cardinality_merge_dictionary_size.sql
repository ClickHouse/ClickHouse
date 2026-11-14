-- A merge writes one dictionary for a LowCardinality column whose values change from granule to granule and whose
-- uniq statistics estimate a few times more values than low_cardinality_max_dictionary_size (8192).
-- k, kn: raised. r: eligible by statistics, but its first 8 granules use few values, so it is written as before.
-- s: not eligible. m: raised, then its values come in runs and it continues with the configured size.

DROP TABLE IF EXISTS t_on;
DROP TABLE IF EXISTS t_off;
DROP TABLE IF EXISTS t_split_on;
DROP TABLE IF EXISTS t_split_off;

CREATE TABLE t_on (n UInt64, k LowCardinality(String), kn LowCardinality(Nullable(String)), r LowCardinality(String), s LowCardinality(String), m LowCardinality(String))
ENGINE = MergeTree ORDER BY n
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    auto_statistics_types = 'uniq_v2', merge_max_block_size = 8192, vertical_merge_algorithm_min_rows_to_activate = 1000000000;

CREATE TABLE t_off AS t_on;
ALTER TABLE t_off MODIFY SETTING merge_max_low_cardinality_dictionary_size = 0;

-- Granules split across the blocks of a vertical merge.
-- t_split_on also uses a bound far above any dictionary.
CREATE TABLE t_split_on AS t_on;
ALTER TABLE t_split_on MODIFY SETTING merge_max_block_size = 1000, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
    merge_max_low_cardinality_dictionary_size = 1000000000000;

CREATE TABLE t_split_off AS t_split_on;
ALTER TABLE t_split_off MODIFY SETTING merge_max_low_cardinality_dictionary_size = 0;

-- One part per table, so that the only merge is the OPTIMIZE below.
INSERT INTO t_on SELECT
    number,
    'v' || toString(cityHash64(number, 1) % 20000),
    if(number % 10 = 0, NULL, 'v' || toString(cityHash64(number, 2) % 20000)),
    if(intDiv(number, 8192) < 8, 'v' || toString(cityHash64(intDiv(number, 20)) % 20000), 'v' || toString(cityHash64(number, 6) % 20000)),
    'v' || toString(cityHash64(number, 3) % 1000),
    if(intDiv(number, 8192) BETWEEN 2 AND 10, 'v' || toString(cityHash64(intDiv(number, 20), 5) % 20000), 'v' || toString(cityHash64(number, 4) % 20000))
FROM numbers(122880)
SETTINGS materialize_statistics_on_insert = 1, materialize_statistics_on_insert_max_table_size = 0, max_insert_threads = 1, max_threads = 1,
    min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 1000000000, max_insert_block_size = 1000000;

ALTER TABLE t_off ATTACH PARTITION tuple() FROM t_on;
ALTER TABLE t_split_on ATTACH PARTITION tuple() FROM t_on;
ALTER TABLE t_split_off ATTACH PARTITION tuple() FROM t_on;

SELECT table, count(), max(level) FROM system.parts WHERE active AND database = currentDatabase() GROUP BY table ORDER BY table;

OPTIMIZE TABLE t_on FINAL;
OPTIMIZE TABLE t_off FINAL;
OPTIMIZE TABLE t_split_on FINAL;
OPTIMIZE TABLE t_split_off FINAL;

SELECT table, count(), max(level) FROM system.parts WHERE active AND database = currentDatabase() GROUP BY table ORDER BY table;

SELECT 'eligible by statistics';
SELECT column, estimates.cardinality BETWEEN 8193 AND 43690
FROM system.parts_columns
WHERE active AND database = currentDatabase() AND table = 't_on' AND column IN ('k', 'kn', 'r', 's', 'm')
ORDER BY column;

SELECT 'content';
WITH (
    SELECT (sum(cityHash64(n, k, kn, r, s, m)), countIf(kn IS NULL))
    FROM (SELECT
        number AS n,
        'v' || toString(cityHash64(number, 1) % 20000) AS k,
        if(number % 10 = 0, NULL, 'v' || toString(cityHash64(number, 2) % 20000)) AS kn,
        if(intDiv(number, 8192) < 8, 'v' || toString(cityHash64(intDiv(number, 20)) % 20000), 'v' || toString(cityHash64(number, 6) % 20000)) AS r,
        'v' || toString(cityHash64(number, 3) % 1000) AS s,
        if(intDiv(number, 8192) BETWEEN 2 AND 10, 'v' || toString(cityHash64(intDiv(number, 20), 5) % 20000), 'v' || toString(cityHash64(number, 4) % 20000)) AS m
        FROM numbers(122880))
) AS expected
SELECT * FROM
(
    SELECT 't_on', count(), (sum(cityHash64(n, k, kn, r, s, m)), countIf(kn IS NULL)) = expected FROM t_on
    UNION ALL SELECT 't_off', count(), (sum(cityHash64(n, k, kn, r, s, m)), countIf(kn IS NULL)) = expected FROM t_off
    UNION ALL SELECT 't_split_on', count(), (sum(cityHash64(n, k, kn, r, s, m)), countIf(kn IS NULL)) = expected FROM t_split_on
    UNION ALL SELECT 't_split_off', count(), (sum(cityHash64(n, k, kn, r, s, m)), countIf(kn IS NULL)) = expected FROM t_split_off
)
ORDER BY 1;

-- One dictionary for k and kn (data marks + the final mark). m: a raised first dictionary over more than 8 granules, then at least one of the configured size.
SELECT 'dictionaries';
SELECT
    table,
    countDistinct(k_dict) <= 2,
    countDistinct(kn_dict) <= 2,
    countDistinct(m_dict) > 3,
    arrayCount(x -> x = argMin(m_dict, mark_number), groupArray(m_dict)) > 8
FROM
(
    SELECT 't_off' AS table, mark_number, `k.dict.mark` AS k_dict, `kn.dict.mark` AS kn_dict, `m.dict.mark` AS m_dict
    FROM mergeTreeIndex(currentDatabase(), t_off, with_marks = true)
    WHERE part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_off' AND active)
    UNION ALL
    SELECT 't_on', mark_number, `k.dict.mark`, `kn.dict.mark`, `m.dict.mark`
    FROM mergeTreeIndex(currentDatabase(), t_on, with_marks = true)
    WHERE part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_on' AND active)
    UNION ALL
    SELECT 't_split_off', mark_number, `k.dict.mark`, `kn.dict.mark`, `m.dict.mark`
    FROM mergeTreeIndex(currentDatabase(), t_split_off, with_marks = true)
    WHERE part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_split_off' AND active)
    UNION ALL
    SELECT 't_split_on', mark_number, `k.dict.mark`, `kn.dict.mark`, `m.dict.mark`
    FROM mergeTreeIndex(currentDatabase(), t_split_on, with_marks = true)
    WHERE part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_split_on' AND active)
)
GROUP BY table
ORDER BY table;

SELECT 'layout';
SELECT
    pair,
    column,
    compressed_on = compressed_off AND uncompressed_on = uncompressed_off
FROM
(
    SELECT
        if(table LIKE 't_split_%', 'split', 'whole') AS pair,
        column,
        sumIf(column_data_compressed_bytes, table LIKE '%_on') AS compressed_on,
        sumIf(column_data_compressed_bytes, table LIKE '%_off') AS compressed_off,
        sumIf(column_data_uncompressed_bytes, table LIKE '%_on') AS uncompressed_on,
        sumIf(column_data_uncompressed_bytes, table LIKE '%_off') AS uncompressed_off
    FROM system.parts_columns
    WHERE active AND database = currentDatabase() AND column IN ('r', 's')
    GROUP BY pair, column
)
ORDER BY pair, column;

DROP TABLE t_on;
DROP TABLE t_off;
DROP TABLE t_split_on;
DROP TABLE t_split_off;
