-- A positional argument is a column number used instead of a column name in
-- GROUP BY / ORDER BY / LIMIT BY. Documented in
-- docs/reference/statements/select/order-by.mdx ("To disable sorting by column
-- numbers ...") and docs/reference/statements/select/group-by.mdx ("group data
-- in the table by column numbers instead of column names"), controlled by the
-- setting enable_positional_arguments (described in src/Core/Settings.cpp).
--
-- A subquery inside a CHECK constraint is analysed by the AST rewriter when
-- CREATE TABLE validates the constraint expression, so this is where the
-- documented limits of a column number are enforced for a plain SELECT.

SET enable_positional_arguments = 1;

DROP TABLE IF EXISTS pos_arg_zero;
DROP TABLE IF EXISTS pos_arg_high;
DROP TABLE IF EXISTS pos_arg_neg_oob;
DROP TABLE IF EXISTS pos_arg_agg;
DROP TABLE IF EXISTS pos_arg_agg_nested;
DROP TABLE IF EXISTS pos_arg_subquery;
DROP TABLE IF EXISTS pos_arg_float;
DROP TABLE IF EXISTS pos_arg_negative;
DROP TABLE IF EXISTS pos_arg_function;

-- Column numbers start at 1, so 0 is rejected.
CREATE TABLE pos_arg_zero (a UInt64, CONSTRAINT c CHECK a IN (SELECT 1 ORDER BY 0))
ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- A number past the last selected column is rejected.
CREATE TABLE pos_arg_high (a UInt64, CONSTRAINT c CHECK a IN (SELECT 1 ORDER BY 9))
ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- A negative number counts back from the last selected column, so it must not
-- be smaller than minus the number of selected columns.
CREATE TABLE pos_arg_neg_oob (a UInt64, CONSTRAINT c CHECK a IN (SELECT 1 ORDER BY -9))
ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- A rejected CREATE leaves no table behind.
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name IN ('pos_arg_zero', 'pos_arg_high', 'pos_arg_neg_oob');

-- GROUP BY must not point at an aggregate function.
CREATE TABLE pos_arg_agg (a UInt64, CONSTRAINT c CHECK a IN (SELECT count() FROM numbers(2) GROUP BY 1))
ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The aggregate function is also found when it is nested in an argument.
CREATE TABLE pos_arg_agg_nested (a UInt64, CONSTRAINT c CHECK a IN (SELECT 1 + count() FROM numbers(2) GROUP BY 1))
ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Only an identifier, a literal or a function can stand at that column number;
-- a subquery expression cannot.
CREATE TABLE pos_arg_subquery (a UInt64, CONSTRAINT c CHECK a IN (SELECT (SELECT 1) ORDER BY 1))
ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A literal that is not an integer is an ordinary constant expression, not a
-- column number, so it is accepted and nothing is replaced.
CREATE TABLE pos_arg_float (a UInt64, CONSTRAINT c CHECK a IN (SELECT 1 ORDER BY 1.5))
ENGINE = MergeTree ORDER BY a;

-- -1 is the last of the two selected columns.
CREATE TABLE pos_arg_negative (a UInt64, CONSTRAINT c CHECK a IN (SELECT 1, 2 ORDER BY -1))
ENGINE = MergeTree ORDER BY a;

-- A non-aggregate function at that column number is accepted in GROUP BY.
CREATE TABLE pos_arg_function (a UInt64, CONSTRAINT c CHECK a IN (SELECT number + 1 FROM numbers(2) GROUP BY 1))
ENGINE = MergeTree ORDER BY a;

-- The three accepted constraints produced three tables.
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name IN ('pos_arg_float', 'pos_arg_negative', 'pos_arg_function');

DROP TABLE pos_arg_float;
DROP TABLE pos_arg_negative;
DROP TABLE pos_arg_function;
