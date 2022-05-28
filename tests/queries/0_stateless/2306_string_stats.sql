DROP TABLE IF EXISTS str_prewhere SYNC;

SET optimize_move_to_prewhere = 1;
SET allow_experimental_stats_for_prewhere_optimization = 1;

CREATE TABLE str_prewhere
(
    a Int,
    b String,
    c String,
    d String,
    STATISTIC st (a, b, c, d)
)
ENGINE=MergeTree() ORDER BY a
SETTINGS experimantal_stats_update_period = 100000, index_granularity = 10;

SELECT 'empty';
EXPLAIN SYNTAX SELECT a, b, c, d FROM str_prewhere WHERE a == 10 AND b == 'test 1 clickhouse 1' AND c == 'test 1 clickhouse 1' AND d == 'unknown';

INSERT INTO str_prewhere SELECT
    number AS a,
    format('test {} clickhouse {}', toString(number % 100), toString(number % 100)) AS b,   -- 10% of granules include 'test 1 clickhouse 1'
    format('test {} clickhouse {}', toString(number % 15), toString(number % 15)) AS c,     -- 66% of granules include 'test 1 clickhouse 1'
    format('test {} clickhouse {}', toString(number % 1000), toString(number % 1000)) AS d
FROM system.numbers
LIMIT 1000000;

SYSTEM RELOAD STATISTICS str_prewhere;

EXPLAIN SYNTAX SELECT a, b, c, d FROM str_prewhere WHERE a == 10 AND b == 'test 1 clickhouse 1' AND c == 'test 1 clickhouse 1' AND d == 'unknown';
EXPLAIN SYNTAX SELECT a, b, c, d FROM str_prewhere WHERE a == 10 AND b == 'test 1 clickhouse 1' AND c == 'test 1 clickhouse 1' AND d == 'unknown';

DROP TABLE str_prewhere SYNC;
