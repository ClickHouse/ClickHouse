SET max_threads = 1
SET max_block_size = 1000
SET group_by_each_block_no_merge = true;
SELECT
    SearchEngineID AS k1,
    count() AS c
FROM test.hits
LIMIT 20
GROUP BY k1
