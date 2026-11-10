-- The totals and extremes of a result without rows must follow the format prefix.

SET output_format_write_statistics = 0;

SELECT number % 2 AS k, count() AS c FROM numbers(3) GROUP BY k WITH TOTALS HAVING k > 10 FORMAT JSONCompact;
SELECT number % 2 AS k, count() AS c FROM numbers(3) GROUP BY k WITH TOTALS HAVING k > 10 FORMAT JSONColumnsWithMetadata;
SELECT number % 2 AS k, count() AS c FROM numbers(3) GROUP BY k WITH TOTALS HAVING k > 10 FORMAT TSVWithNames;

SELECT number FROM numbers(3) SETTINGS extremes = 1, offset = 10 FORMAT JSONCompact;
SELECT number FROM numbers(3) SETTINGS extremes = 1, offset = 10 FORMAT JSONColumnsWithMetadata;
SELECT number FROM numbers(3) SETTINGS extremes = 1, offset = 10 FORMAT TSVWithNames;
