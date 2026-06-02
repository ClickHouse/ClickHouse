#!/usr/bin/env python3
"""
Tests for the SQLStorm query rewriter.

Validates that the PostgreSQL -> ClickHouse rewrites do not produce invalid SQL
for known edge cases (FETCH/OFFSET combinations).
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from rewrite_queries import rewrite_query


class TestFetchOffsetRewrite(unittest.TestCase):
    def test_offset_rows_fetch_first_rows_only(self):
        # SQL standard: rewriting only FETCH FIRST while leaving OFFSET ... ROWS
        # behind would produce invalid ClickHouse SQL like
        # `OFFSET 5 ROWS LIMIT 10`. Both clauses must be rewritten together.
        self.assertEqual(
            rewrite_query("SELECT * FROM t ORDER BY x OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY"),
            "SELECT * FROM t ORDER BY x LIMIT 10 OFFSET 5",
        )

    def test_offset_row_fetch_first_row_only(self):
        # Singular ROW form must be handled too.
        self.assertEqual(
            rewrite_query("SELECT * FROM t ORDER BY x OFFSET 5 ROW FETCH FIRST 1 ROW ONLY"),
            "SELECT * FROM t ORDER BY x LIMIT 1 OFFSET 5",
        )

    def test_standalone_fetch_first_rows_only(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t FETCH FIRST 10 ROWS ONLY"),
            "SELECT * FROM t LIMIT 10",
        )


class TestArrayJoinOnClause(unittest.TestCase):
    def test_on_predicate_does_not_consume_trailing_clauses(self):
        # The `ON` regex must stop before clause keywords; otherwise it
        # greedily swallows `WHERE`/`ORDER BY` etc. and drops them from the
        # rewritten query.
        self.assertEqual(
            rewrite_query(
                "SELECT * FROM t JOIN arrayJoin(arr) AS a ON a > 0 WHERE id = 1 ORDER BY id"
            ),
            "SELECT * FROM t \nARRAY JOIN arr AS a WHERE id = 1 ORDER BY id",
        )

    def test_on_true_followed_by_where(self):
        self.assertEqual(
            rewrite_query(
                "SELECT * FROM t LEFT JOIN arrayJoin(arr) AS a ON TRUE WHERE id = 1"
            ),
            "SELECT * FROM t \nLEFT ARRAY JOIN arr AS a WHERE id = 1",
        )

    def test_alias_with_whitespace_before_column_list(self):
        # `AS tag (TagName) ON TRUE` has whitespace before the column list; the
        # column name must still be parsed and the `ON` clause stripped, instead
        # of leaking ` (TagName) ON TRUE` into the rewritten query.
        self.assertEqual(
            rewrite_query(
                "SELECT * FROM t LEFT JOIN arrayJoin(arr) AS tag (TagName) ON TRUE WHERE id = 1"
            ),
            "SELECT * FROM t \nLEFT ARRAY JOIN arr AS TagName WHERE id = 1",
        )


class TestAnyArrayRewrite(unittest.TestCase):
    def test_simple_identifier(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t WHERE x = ANY(arr)"),
            "SELECT * FROM t WHERE has(arr, x)",
        )

    def test_qualified_identifier_with_function_operand(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM tt WHERE tt.TagName = ANY(splitByString(',', s))"),
            "SELECT * FROM tt WHERE has(splitByString(',', s), tt.TagName)",
        )

    def test_subquery_operand_left_untouched(self):
        # ClickHouse handles `ANY(subquery)` natively; do not rewrite it.
        sql = "SELECT * FROM t WHERE x = ANY(SELECT id FROM u)"
        self.assertEqual(rewrite_query(sql), sql)

    def test_with_subquery_operand_left_untouched(self):
        sql = "SELECT * FROM t WHERE x = ANY(WITH c AS (SELECT 1) SELECT * FROM c)"
        self.assertEqual(rewrite_query(sql), sql)


def _sort_key(f):
    """Mirror of `runner._sort_key` — kept inline to avoid importing the runner
    (which has heavy side imports). If runner's sort key changes, update here."""
    stem = os.path.splitext(f)[0]
    return (0, int(stem), "") if stem.isdigit() else (1, 0, stem)


class TestRunnerSortKey(unittest.TestCase):
    def test_mixed_numeric_and_alpha_filenames(self):
        # Python 3 raises TypeError when sorting heterogeneous int/str keys.
        # This test guards against regressing the sort key to that form.
        files = ["10.sql", "1.sql", "abc.sql", "2.sql", "zzz.sql"]
        self.assertEqual(
            sorted(files, key=_sort_key),
            ["1.sql", "2.sql", "10.sql", "abc.sql", "zzz.sql"],
        )

    def test_all_numeric(self):
        files = ["3.sql", "1.sql", "2.sql"]
        self.assertEqual(
            sorted(files, key=_sort_key),
            ["1.sql", "2.sql", "3.sql"],
        )

    def test_all_alpha(self):
        files = ["zzz.sql", "abc.sql", "mno.sql"]
        self.assertEqual(
            sorted(files, key=_sort_key),
            ["abc.sql", "mno.sql", "zzz.sql"],
        )


if __name__ == "__main__":
    unittest.main()
