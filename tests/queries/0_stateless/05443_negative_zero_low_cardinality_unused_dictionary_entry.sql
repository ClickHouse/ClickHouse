-- The dictionary of a `LowCardinality` column can keep a negative zero that no remaining row references
-- after filtering. Canonicalizing negative zeros must handle it as a column without negative zeros.

SET allow_suspicious_low_cardinality_types = 1;

SELECT DISTINCT k, x
FROM
(
    SELECT *
    FROM format(TSV, 'k UInt64, x LowCardinality(Float64)', '0\t-0\n1\t1\n2\t2')
    WHERE k > 0
    ORDER BY k
)
SETTINGS optimize_distinct_in_order = 1;

SELECT DISTINCT k, x
FROM
(
    SELECT *
    FROM format(TSV, 'k UInt64, x LowCardinality(Float64)', '0\t-0\n1\t1\n2\t2')
    WHERE k > 0
    ORDER BY k
)
SETTINGS optimize_distinct_in_order = 0;

SELECT arrayDistinct(groupArray(x))
FROM
(
    SELECT *
    FROM format(TSV, 'k UInt64, x LowCardinality(Float64)', '0\t-0\n1\t1\n2\t1')
    WHERE k > 0
);
