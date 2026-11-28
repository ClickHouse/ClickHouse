-- Tags: no-fasttest, no-flaky-check
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-flaky-check: ANTLR locks a mutex several times per token, and the thread fuzzer of the flaky check migrates
-- the thread to another CPU on every lock and unlock, so parsing these long queries takes minutes there.

-- A deeply nested PromQL query must fail with `TOO_DEEP_RECURSION`. It used to overflow the stack of the thread,
-- and the server was terminated by a segmentation fault.

SET allow_experimental_time_series_table = 1;

CREATE TABLE ts ENGINE = TimeSeries;

-- These are nested in the grammar, so the parser recurses on them.
SELECT * FROM prometheusQuery(ts, repeat('-', 100000) || '1', 1000); -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM prometheusQuery(ts, repeat('(', 100000) || '1' || repeat(')', 100000), 1000); -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM prometheusQuery(ts, repeat('2^', 100000) || '2', 1000); -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM prometheusQuery(ts, repeat('abs(', 100000) || 'up' || repeat(')', 100000), 1000); -- { serverError TOO_DEEP_RECURSION }

-- These are parsed in a loop, but the result is a tree as deep as the chain is long.
SELECT * FROM prometheusQuery(ts, repeat('1+', 100000) || '1', 1000); -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM prometheusQuery(ts, repeat('up or ', 100000) || 'up', 1000); -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM prometheusQuery(ts, 'up' || repeat('[1m:]', 100000), 1000); -- { serverError TOO_DEEP_RECURSION }

-- `bool` and `group_left` used to be metric names as well, so the parser looked ahead through the whole chain
-- to choose whether the keyword is a modifier. The lookahead overflowed the stack when it recursed on the chain,
-- and when it freed its state at the end.
SELECT * FROM prometheusQuery(ts, '1 == bool ' || repeat('-', 100000) || '1', 1000); -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM prometheusQuery(ts, '1 == bool ' || repeat('-(', 100000) || '1' || repeat(')', 100000), 1000); -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM prometheusQuery(ts, 'up * on(a) group_left ' || repeat('-(', 100000) || '1' || repeat(')', 100000), 1000); -- { serverError TOO_DEEP_RECURSION }

-- A syntax error deep inside a nested query. The parser used to overflow the stack while it was guessing whether
-- the closing parenthesis is just missing. In a build with larger stack frames this chain is already too deep to parse.
SELECT * FROM prometheusQuery(ts, repeat('-', 10000) || '(1', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY, TOO_DEEP_RECURSION }

DROP TABLE ts;
