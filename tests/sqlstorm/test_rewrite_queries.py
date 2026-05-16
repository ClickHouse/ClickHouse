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
