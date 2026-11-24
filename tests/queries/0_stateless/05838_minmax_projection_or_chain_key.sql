-- A MergeTree PARTITION BY / ORDER BY expression may itself be an OR chain of equalities.
-- Creating the table builds the implicit minmax_count projection over those expressions, and
-- the AST optimizer rewrites a chain of at least `optimize_min_equality_disjunction_chain_length`
-- equalities (default 3, see the setting description in src/Core/Settings.cpp) into a single
-- `expr IN (x1, ..., xN)`. A chain that collapses to one operand must be spliced into its parent
-- correctly, otherwise the key expression of the table is silently wrong. This test asserts that
-- such a table stores and reads back every row, and that filtering on the same OR chains agrees
-- with the data.

DROP TABLE IF EXISTS t_or_key;

-- (b = 1 OR b = 2) is a 2-element chain and stays an OR; (a = 1 OR a = 2 OR a = 3) reaches the
-- default threshold of 3 and becomes a IN (1, 2, 3), a single operand.
CREATE TABLE t_or_key (a UInt32, b UInt32) ENGINE = MergeTree
PARTITION BY (b = 1 OR b = 2)
ORDER BY (a = 1 OR a = 2 OR a = 3);

INSERT INTO t_or_key SELECT number, number % 4 FROM numbers(12);

-- Every row survived the rewritten key expression.
SELECT count(), min(a), max(a), sum(b) FROM t_or_key;

-- The rewritten sorting key still answers a filter on the same chain: a in {1, 2, 3}.
SELECT count() FROM t_or_key WHERE a = 1 OR a = 2 OR a = 3;

-- The partition expression still answers a filter on its own chain: b in {1, 2}, i.e.
-- number % 4 in {1, 2} over numbers(12).
SELECT count() FROM t_or_key WHERE b = 1 OR b = 2;

-- Both values of the partition key expression are present and hold the rows they should.
SELECT (b = 1 OR b = 2) AS part_key, count() FROM t_or_key GROUP BY part_key ORDER BY part_key;

DROP TABLE t_or_key;
