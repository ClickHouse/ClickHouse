-- Tags: no-fasttest
-- Totals and Extremes print below the main output, so they get their own row budget: the main
-- output exhausting `output_format_pretty_max_rows` used to hide their rows for good.

SET output_format_pretty_color = 0;
SET output_format_pretty_squash_consecutive_ms = 0;
SET output_format_pretty_max_rows = 2;

SELECT number, count() FROM numbers(5) GROUP BY number WITH TOTALS ORDER BY number FORMAT PrettyCompactMonoBlock;

SET extremes = 1;
SELECT number FROM numbers(5) ORDER BY number FORMAT PrettyCompactMonoBlock;
