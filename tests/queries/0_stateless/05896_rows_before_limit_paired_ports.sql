-- `rows_before_limit_at_least` of a LIMIT over UNION ALL branches counted by a LIMIT BY (10 rows) and by the LIMIT (5 rows),
-- read through a processor that keeps its port pairs apart.

SET exact_rows_before_limit = 1, output_format_write_statistics = 0;
SET format_template_row_format = '${0:Raw}';
SET format_template_resultset_format = '${data}{"rows_before_limit_at_least":${rows_before_limit:Raw}}\n';

-- `Offset` below `NegativeLimit`.
SELECT '' FROM (SELECT number FROM numbers(10) LIMIT 1 BY number UNION ALL SELECT number FROM numbers(5)) LIMIT -1 FORMAT Template;

-- `DelayedPorts` of the `IN` set below `Limit`.
SELECT '' FROM (SELECT number FROM (SELECT number FROM numbers(10) LIMIT 1 BY number UNION ALL SELECT number FROM numbers(5)) WHERE number IN (SELECT number FROM numbers(100))) LIMIT 1 FORMAT Template;

SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT '' FROM (SELECT number FROM numbers(10) LIMIT 1 BY number UNION ALL SELECT number FROM numbers(5)) LIMIT -1) WHERE explain ILIKE '%Offset % → %';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT '' FROM (SELECT number FROM (SELECT number FROM numbers(10) LIMIT 1 BY number UNION ALL SELECT number FROM numbers(5)) WHERE number IN (SELECT number FROM numbers(100))) LIMIT 1) WHERE explain ILIKE '%DelayedPorts % → %';
