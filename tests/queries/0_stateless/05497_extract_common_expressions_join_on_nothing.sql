-- Common-expression extraction in JOIN ON must keep the query valid when it absorbs the only
-- `Nothing`-typed operand (a comparison with the alias of `ARRAY JOIN []`).
SET join_algorithm = 'hash';

SELECT count() FROM values('c0 Int32', 1, 2) AS tx ARRAY JOIN [] AS a0 RIGHT JOIN values('c0 Int32', 1, 2) AS t2
ON (t2.c0 = tx.c0) AND (((t2.c0 = 1) AND (t2.c0 = a0)) OR (t2.c0 = 1))
SETTINGS optimize_extract_common_expressions = 1;

SELECT count() FROM values('c0 Int32', 1, 2) AS tx ARRAY JOIN [] AS a0 RIGHT JOIN values('c0 Int32', 1, 2) AS t2
ON ((t2.c0 = tx.c0) AND (t2.c0 = a0)) OR (t2.c0 = tx.c0)
SETTINGS optimize_extract_common_expressions = 1;

-- Extraction still applies when the `Nothing` operand survives it: `partial_merge` rejects this OR otherwise.
SELECT count() FROM values('c0 Int32', 1, 2) AS tx ARRAY JOIN [] AS a0 INNER JOIN values('c0 Int32', 1, 2) AS t2
ON ((t2.c0 = tx.c0) AND (t2.c0 = a0)) OR ((t2.c0 = tx.c0) AND (t2.c0 = 1))
SETTINGS optimize_extract_common_expressions = 1, join_algorithm = 'partial_merge';
