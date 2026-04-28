#!/usr/bin/env python3
"""
Tests for the SQLStorm query rewriter.

Validates that the PostgreSQL -> ClickHouse rewrites do not produce invalid SQL
for known edge cases (e.g. casted or compound LHS expressions in `= ANY(...)`).
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from rewrite_queries import rewrite_any_comparison


class TestRewriteAnyComparison(unittest.TestCase):
    def test_simple_identifier(self):
        self.assertEqual(
            rewrite_any_comparison("SELECT * FROM t WHERE a = ANY(arr)"),
            "SELECT * FROM t WHERE has(arr, a)",
        )

    def test_qualified_identifier(self):
        self.assertEqual(
            rewrite_any_comparison("SELECT * FROM t WHERE x.col = ANY(arr)"),
            "SELECT * FROM t WHERE has(arr, x.col)",
        )

    def test_not_equal(self):
        self.assertEqual(
            rewrite_any_comparison("SELECT * FROM t WHERE col != ANY(arr)"),
            "SELECT * FROM t WHERE NOT has(arr, col)",
        )

    def test_angle_not_equal(self):
        self.assertEqual(
            rewrite_any_comparison("SELECT * FROM t WHERE col <> ANY(arr)"),
            "SELECT * FROM t WHERE NOT has(arr, col)",
        )

    def test_postgres_cast_lhs_not_rewritten(self):
        # `a::integer = ANY(arr)` must not become `a::has(arr, integer)`
        sql = "SELECT * FROM t WHERE a::integer = ANY(arr)"
        self.assertEqual(rewrite_any_comparison(sql), sql)

    def test_parenthesized_cast_lhs_not_rewritten(self):
        sql = "SELECT * FROM t WHERE (a)::integer = ANY(arr)"
        self.assertEqual(rewrite_any_comparison(sql), sql)

    def test_qualified_cast_lhs_not_rewritten(self):
        sql = "SELECT * FROM t WHERE x.a::integer = ANY(arr)"
        self.assertEqual(rewrite_any_comparison(sql), sql)

    def test_arithmetic_lhs_not_rewritten(self):
        # `a + b = ANY(arr)` must not become `a + has(arr, b)`
        sql = "SELECT * FROM t WHERE a + b = ANY(arr)"
        self.assertEqual(rewrite_any_comparison(sql), sql)


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
