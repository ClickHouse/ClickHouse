DROP TABLE IF EXISTS str_prewhere SYNC;

SET optimize_move_to_prewhere = 1;
SET allow_experimental_stats_for_prewhere_optimization = 1;
SET calculate_stats_during_insert = 1;

CREATE TABLE str_prewhere
(
    a Int,
    b String CODEC(NONE),
    c String CODEC(NONE),
    d String CODEC(NONE),
    STATISTIC st (b, c, d) TYPE granule_string_hash
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 100000, index_granularity = 10;

SELECT 'empty';
EXPLAIN SYNTAX SELECT a, b, c, d FROM str_prewhere WHERE a == 10 AND b == 'test 10 clickhouse 10' AND c == 'test 10 clickhouse 10' AND d == 'test 42 clickhouse 42';

INSERT INTO str_prewhere SELECT
    number AS a,
    format('test {}0 clickhouse {}0', toString(number % 100), toString(number % 100)) AS b,   -- 10% of granules include 'test 1 clickhouse 1'
    format('test {}0 clickhouse {}0', toString(number % 11), toString(number % 11)) AS c,     -- lots of granules include 'test 1 clickhouse 1'
    format('test {} clickhouse {}', toString(number % 500), toString(number % 500)) AS d -- smallest selectivity
FROM system.numbers
LIMIT 1000000;

SYSTEM RELOAD STATISTICS str_prewhere;

EXPLAIN SYNTAX SELECT a, b, c, d FROM str_prewhere WHERE a == 10 AND b == 'test 10 clickhouse 10' AND c == 'unknown' AND d == 'test 42 clickhouse 42';

EXPLAIN SYNTAX SELECT a, b, c, d FROM str_prewhere WHERE a == 10 AND b == 'test 10 clickhouse 10' AND c == 'test 10 clickhouse 10';

EXPLAIN SYNTAX SELECT a, b, c, d FROM str_prewhere WHERE a == 10 AND b == 'test 10 clickhouse 10' AND c == 'test 10 clickhouse 10' AND d == 'test 42 clickhouse 42';


DROP TABLE str_prewhere SYNC;
