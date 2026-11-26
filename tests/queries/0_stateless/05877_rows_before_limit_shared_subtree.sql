-- `rows_before_limit_at_least` when several inputs of a LIMIT share the subtree that counts the rows.

SET exact_rows_before_limit = 1, output_format_write_statistics = 0;
SET max_threads = 4, max_block_size = 10, join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 0, query_plan_join_swap_table = 0;
SET format_template_row_format = '${0:Raw}';
SET format_template_resultset_format = '${data}{"rows_before_limit_at_least":${rows_before_limit:Raw}}\n';

-- The four join streams share the LIMIT BY below the join, the UNION ALL stream is counted by the LIMIT: 1000 + 500.
SELECT '' FROM
(
    SELECT l.number FROM (SELECT number FROM numbers(1000) LIMIT 1 BY number) AS l
    INNER JOIN numbers(1000) AS r ON l.number = r.number
    UNION ALL
    SELECT number FROM numbers(500)
)
LIMIT 1 FORMAT Template;

-- The same with the remote sources below the shared resize of the left side: 2 x 100 + 500.
SELECT '' FROM
(
    SELECT l.number FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(100))) AS l
    INNER JOIN numbers(100) AS r ON l.number = r.number
    UNION ALL
    SELECT number FROM numbers(500)
)
LIMIT 1 FORMAT Template SETTINGS prefer_localhost_replica = 0;

-- The LIMIT has five inputs, four of them through the shared join pipeline.
SELECT countIf(explain ILIKE '%Limit 5 → 5%') FROM
(
    EXPLAIN PIPELINE SELECT '' FROM
    (
        SELECT l.number FROM (SELECT number FROM numbers(1000) LIMIT 1 BY number) AS l
        INNER JOIN numbers(1000) AS r ON l.number = r.number
        UNION ALL
        SELECT number FROM numbers(500)
    )
    LIMIT 1
);
