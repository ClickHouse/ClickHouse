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
    def test_real_on_predicate_left_unchanged(self):
        # A real `ON` predicate cannot be expressed as an `ARRAY JOIN`
        # condition. Dropping it would turn a filtered join into an unfiltered
        # cross product, so the construct must be left unchanged instead.
        sql = "SELECT * FROM t JOIN arrayJoin(arr) AS a ON a > 0 WHERE id = 1 ORDER BY id"
        self.assertEqual(rewrite_query(sql), sql)

    def test_real_on_equality_predicate_left_unchanged(self):
        # Mirrors corpus queries such as `stackoverflow/335.sql` and
        # `1279.sql`, where the equality join predicate must be preserved.
        sql = "SELECT * FROM t LEFT JOIN arrayJoin(arr) AS tag ON t.TagName = tag"
        self.assertEqual(rewrite_query(sql), sql)

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

    def test_using_clause_left_unchanged(self):
        # `USING (...)` is a real join predicate, like a non-trivial `ON`.
        # `ARRAY JOIN` cannot carry it, so the construct must be left unchanged
        # rather than emitting invalid `ARRAY JOIN arr AS id USING (id)` (which
        # would also drop the table alias `u`).
        sql = "SELECT * FROM t JOIN UNNEST(arr) AS u(id) USING (id)"
        self.assertEqual(rewrite_query(sql), sql)


class TestArrayJoinKeyword(unittest.TestCase):
    def test_inner_join_strips_inner_keyword(self):
        # `INNER JOIN` must be rewritten as `ARRAY JOIN`; the bare-`JOIN`
        # handling alone would leave a stray `INNER` before `ARRAY JOIN`.
        self.assertEqual(
            rewrite_query("SELECT * FROM t INNER JOIN arrayJoin(arr) AS a ON TRUE"),
            "SELECT * FROM t \nARRAY JOIN arr AS a",
        )

    def test_left_outer_join_becomes_left_array_join(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t LEFT OUTER JOIN arrayJoin(arr) AS a ON TRUE"),
            "SELECT * FROM t \nLEFT ARRAY JOIN arr AS a",
        )

    def test_cross_join_becomes_array_join(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t CROSS JOIN arrayJoin(arr) AS a"),
            "SELECT * FROM t \nARRAY JOIN arr AS a",
        )

    def test_right_join_left_unchanged(self):
        # RIGHT/FULL ARRAY JOIN has no ClickHouse equivalent.
        sql = "SELECT * FROM t RIGHT JOIN arrayJoin(arr) AS a ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)

    def test_full_outer_join_left_unchanged(self):
        sql = "SELECT * FROM t FULL OUTER JOIN arrayJoin(arr) AS a ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)


class TestArrayJoinFromPosition(unittest.TestCase):
    # `arrayJoin`/`UNNEST` directly in `FROM` position is wrapped in a subquery,
    # since neither is a table function. The `FROM` keyword must be re-emitted in
    # the replacement — dropping it produces invalid `SELECT ... (SELECT ...)` —
    # and the original table alias must be preserved as the subquery alias so a
    # qualified projection such as `u.x` still resolves.
    def test_unnest_in_from_position_wrapped_in_subquery(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM UNNEST(arr) AS u(x)"),
            "SELECT * FROM (SELECT arrayJoin(arr) AS x) AS u",
        )

    def test_unnest_in_from_position_preserves_qualified_reference(self):
        # The table alias `u` must survive so `u.x` in the projection resolves;
        # replacing it with a synthetic alias would break the reference.
        self.assertEqual(
            rewrite_query("SELECT u.x FROM UNNEST(arr) AS u(x)"),
            "SELECT u.x FROM (SELECT arrayJoin(arr) AS x) AS u",
        )

    def test_array_join_in_from_position_wrapped_in_subquery(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM arrayJoin(arr) AS a"),
            "SELECT * FROM (SELECT arrayJoin(arr) AS a) AS a",
        )

    def test_unnest_in_from_position_with_function_operand(self):
        # The operand keeps its nested parentheses after the function rewrites.
        self.assertEqual(
            rewrite_query("SELECT x FROM UNNEST(string_to_array(tags, ',')) AS u(tag)"),
            "SELECT x FROM (SELECT arrayJoin(splitByString(',', assumeNotNull(tags))) AS tag) AS u",
        )


class TestUnnestWithOrdinality(unittest.TestCase):
    # `UNNEST(...) WITH ORDINALITY` is valid PostgreSQL table-function syntax
    # that adds a 1-based ordinality column. `ARRAY JOIN` cannot express it, and
    # the omitted-`AS` alias parser would otherwise misread the bare `WITH`
    # token as the table alias and emit invalid
    # `ARRAY JOIN arr AS WITH ORDINALITY AS u(x, n)`. The construct must be left
    # unchanged rather than turned into invalid ClickHouse SQL (which would make
    # the benchmark count a rewriter artifact as a ClickHouse query failure).
    def test_join_unnest_with_ordinality_left_unchanged(self):
        sql = "SELECT * FROM t CROSS JOIN UNNEST(arr) WITH ORDINALITY AS u(x, n)"
        self.assertEqual(rewrite_query(sql), sql)

    def test_left_join_unnest_with_ordinality_left_unchanged(self):
        sql = "SELECT * FROM t LEFT JOIN UNNEST(arr) WITH ORDINALITY AS u(x, n) ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)

    def test_from_unnest_with_ordinality_left_unchanged(self):
        sql = "SELECT * FROM UNNEST(arr) WITH ORDINALITY AS u(x, n)"
        self.assertEqual(rewrite_query(sql), sql)

    def test_unnest_with_ordinality_case_insensitive_left_unchanged(self):
        # The guard is case-insensitive, like the rest of the rewriter.
        sql = "SELECT * FROM t JOIN unnest(arr) with ordinality AS u(x, n)"
        self.assertEqual(rewrite_query(sql), sql)

    def test_plain_unnest_alias_still_rewritten(self):
        # Sanity check that the `WITH ORDINALITY` guard does not suppress the
        # ordinary `UNNEST(arr) AS u(x)` rewrite.
        self.assertEqual(
            rewrite_query("SELECT * FROM t CROSS JOIN UNNEST(arr) AS u(x)"),
            "SELECT * FROM t \nARRAY JOIN arr AS x",
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

    def test_parenthesized_subquery_operand_left_untouched(self):
        # A parenthesized subquery `ANY((SELECT ...))` is also a subquery and
        # must not be rewritten to `has((SELECT ...), x)` (invalid: `has`
        # requires an array operand).
        sql = "SELECT * FROM t WHERE x = ANY((SELECT id FROM u))"
        self.assertEqual(rewrite_query(sql), sql)

    def test_parenthesized_with_subquery_operand_left_untouched(self):
        sql = "SELECT * FROM t WHERE x = ANY((WITH c AS (SELECT 1) SELECT * FROM c))"
        self.assertEqual(rewrite_query(sql), sql)

    def test_arithmetic_left_hand_side_left_untouched(self):
        # The captured identifier (`b`) is only the tail of `a + b`, not the
        # whole left-hand side; rewriting would wrongly produce
        # `a + has(arr, b)`. Leave such complex expressions untouched.
        sql = "SELECT * FROM t WHERE a + b = ANY(arr)"
        self.assertEqual(rewrite_query(sql), sql)

    def test_cast_left_hand_side_left_untouched(self):
        # A PostgreSQL cast `a::integer` must not be split into
        # `a::has(arr, integer)`. The `::` before the identifier marks it as
        # part of a larger expression.
        sql = "SELECT * FROM t WHERE a::integer = ANY(arr)"
        self.assertEqual(rewrite_query(sql), sql)


class TestProtectedSpans(unittest.TestCase):
    def test_offset_fetch_inside_string_literal_left_untouched(self):
        # The rewrites must not fire inside string literals: only dialect
        # syntax outside literals should change.
        sql = "SELECT 'OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY' AS s"
        self.assertEqual(rewrite_query(sql), sql)

    def test_function_name_inside_string_literal_left_untouched(self):
        sql = "SELECT 'STRING_TO_ARRAY(x, y)' AS s"
        self.assertEqual(rewrite_query(sql), sql)

    def test_rewrite_outside_literal_still_applies(self):
        # A literal earlier in the query must not shield the real `OFFSET ...
        # LIMIT` that follows it.
        self.assertEqual(
            rewrite_query("SELECT 'note: OFFSET 1 LIMIT 2' AS s, x OFFSET 5 LIMIT 10"),
            "SELECT 'note: OFFSET 1 LIMIT 2' AS s, x LIMIT 10 OFFSET 5",
        )

    def test_string_literal_left_hand_side_of_any_still_rewritten(self):
        # A literal operand is masked, but it is still a single token, so
        # `'tag' = ANY(tags)` is rewritten and the literal restored.
        self.assertEqual(
            rewrite_query("SELECT * FROM t WHERE 'tag' = ANY(tags)"),
            "SELECT * FROM t WHERE has(tags, 'tag')",
        )

    def test_at_time_zone_literal_preserved(self):
        # The timezone literal is masked, matched as a placeholder, and restored.
        self.assertEqual(
            rewrite_query("SELECT ts AT TIME ZONE 'UTC' FROM t"),
            "SELECT toTimezone(ts, 'UTC') FROM t",
        )

    def test_dollar_quoted_literal_left_untouched(self):
        # PostgreSQL dollar-quoted literals are protected spans too; the syntax
        # rewrite must only fire on the real FETCH clause outside the literal.
        self.assertEqual(
            rewrite_query("SELECT $$FETCH FIRST 5 ROWS ONLY$$ AS s FETCH FIRST 1 ROW ONLY"),
            "SELECT $$FETCH FIRST 5 ROWS ONLY$$ AS s LIMIT 1",
        )

    def test_tagged_dollar_quoted_literal_left_untouched(self):
        sql = "SELECT $tag$STRING_TO_ARRAY(x, y)$tag$ AS s"
        self.assertEqual(rewrite_query(sql), sql)

    def test_dollar_quoted_literal_containing_single_quote(self):
        # A dollar-quoted literal may contain an unpaired single quote; it must
        # be masked as one span, not break the quote scanner.
        sql = "SELECT $$it's OFFSET 1 LIMIT 2$$ AS s"
        self.assertEqual(rewrite_query(sql), sql)


class TestUnnestJoinRewrite(unittest.TestCase):
    def test_unnest_with_function_operand_balanced_parens(self):
        # The operand contains nested parentheses (after function rewriting);
        # the structural scan must capture it in full instead of stopping at the
        # first `)`.
        self.assertEqual(
            rewrite_query("SELECT * FROM t CROSS JOIN UNNEST(string_to_array(tags, ',')) AS u(tag)"),
            "SELECT * FROM t \nARRAY JOIN splitByString(',', assumeNotNull(tags)) AS tag",
        )

    def test_left_join_unnest_alias_col_on_true(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t LEFT JOIN UNNEST(arr) AS u(tag) ON TRUE WHERE id = 1"),
            "SELECT * FROM t \nLEFT ARRAY JOIN arr AS tag WHERE id = 1",
        )

    def test_unnest_in_expression_position_left_untouched(self):
        # `unnest(expr)` outside JOIN/FROM position is resolved by the native
        # alias and must not be rewritten.
        sql = "SELECT unnest(arr) FROM t"
        self.assertEqual(rewrite_query(sql), sql)

    def test_lateral_subquery_with_nested_parens(self):
        # The unnest operand gains nested parentheses after the function
        # rewrites run; the subquery form must capture it with a balanced scan
        # instead of leaving a correlated subquery behind.
        self.assertEqual(
            rewrite_query(
                "SELECT * FROM t CROSS JOIN LATERAL (SELECT unnest(string_to_array(tags, ',')) AS tag) u ON TRUE"
            ),
            "SELECT * FROM t ARRAY JOIN splitByString(',', assumeNotNull(tags)) AS tag",
        )

    def test_left_join_lateral_subquery_with_nested_parens(self):
        self.assertEqual(
            rewrite_query(
                "SELECT * FROM t LEFT JOIN LATERAL (SELECT unnest(string_to_array(tags, ',')) AS tag) u ON TRUE WHERE id = 1"
            ),
            "SELECT * FROM t LEFT ARRAY JOIN splitByString(',', assumeNotNull(tags)) AS tag WHERE id = 1",
        )

    def test_join_subquery_simple_operand_still_rewritten(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t CROSS JOIN (SELECT unnest(arr) AS a) u ON TRUE"),
            "SELECT * FROM t ARRAY JOIN arr AS a",
        )

    def test_join_subquery_alias_column_list(self):
        # `u(tag)` renames the unnest column; the rename must win and the
        # column list must not leak into the output.
        self.assertEqual(
            rewrite_query("SELECT * FROM t CROSS JOIN (SELECT unnest(arr) AS a) u(tag) ON TRUE"),
            "SELECT * FROM t ARRAY JOIN arr AS tag",
        )

    def test_join_subquery_with_real_condition_left_untouched(self):
        # A genuine join condition cannot be expressed as ARRAY JOIN; the
        # construct must be left alone rather than dropping the predicate.
        sql = "SELECT * FROM t JOIN (SELECT unnest(arr) AS a) u ON u.a = t.id"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_subquery_with_from_clause_left_untouched(self):
        # The subquery is more than a bare `SELECT unnest(expr) AS col`; it must
        # not be collapsed into an ARRAY JOIN.
        sql = "SELECT * FROM t CROSS JOIN (SELECT unnest(arr) AS a FROM u) v ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_subquery_on_true_and_predicate_left_untouched(self):
        # `ON TRUE AND ...` still carries a real predicate that `ARRAY JOIN`
        # cannot represent; consuming only `ON TRUE` would leave the boolean
        # tail dangling (`ARRAY JOIN arr AS a AND ...`), which is invalid SQL and
        # also silently drops the join filter. Leave the whole construct alone.
        sql = "SELECT * FROM t JOIN (SELECT unnest(arr) AS a) u ON TRUE AND u.a > 0 WHERE id = 1"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_subquery_on_true_or_predicate_left_untouched(self):
        sql = "SELECT * FROM t JOIN (SELECT unnest(arr) AS a) u ON TRUE OR u.a > 0"
        self.assertEqual(rewrite_query(sql), sql)

    def test_right_join_subquery_left_unchanged(self):
        # `RIGHT`/`FULL ARRAY JOIN` has no ClickHouse equivalent. The qualifier
        # must be recognised so it is not left dangling in the prefix as an
        # invalid `RIGHT ARRAY JOIN ...`; the construct is left unchanged,
        # matching the direct `arrayJoin(...)`/`UNNEST(...)` path.
        sql = "SELECT * FROM t RIGHT JOIN (SELECT unnest(arr) AS a) u ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)

    def test_full_outer_join_subquery_left_unchanged(self):
        sql = "SELECT * FROM t FULL OUTER JOIN (SELECT unnest(arr) AS a) u ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_subquery_using_clause_left_unchanged(self):
        # `USING (...)` is a real join predicate that `ARRAY JOIN` cannot carry;
        # leave the construct unchanged instead of emitting a trailing
        # `ARRAY JOIN arr AS a USING (id)`.
        sql = "SELECT * FROM t JOIN (SELECT unnest(arr) AS a) u USING (id)"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_unnest_qualified_table_alias_left_unchanged(self):
        # `UNNEST(arr) AS u(x)` in JOIN position would rewrite to
        # `ARRAY JOIN arr AS x`, which drops the table alias `u`. Because the
        # projection references `u.x`, the rewrite would leave `u` unresolved, so
        # the whole construct must be left unchanged instead.
        sql = "SELECT u.x FROM t CROSS JOIN UNNEST(arr) AS u(x)"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_arrayjoin_qualified_table_alias_left_unchanged(self):
        # The `arrayJoin(arr) AS tag (TagName)` form has the same alias loss; a
        # qualified projection `tag.TagName` keeps the construct unchanged.
        sql = "SELECT tag.TagName FROM t LEFT JOIN arrayJoin(arr) AS tag (TagName) ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_unnest_unqualified_column_still_rewritten(self):
        # The table alias `u` is never referenced as a qualifier, so the rewrite
        # is safe: the column alias `x` is reachable unqualified after the
        # `ARRAY JOIN`.
        self.assertEqual(
            rewrite_query("SELECT x FROM t CROSS JOIN UNNEST(arr) AS u(x)"),
            "SELECT x FROM t \nARRAY JOIN arr AS x",
        )

    def test_join_subquery_qualified_table_alias_left_unchanged(self):
        # The subquery `UNNEST` path has the same alias loss: the rewrite to
        # `ARRAY JOIN arr AS tag` drops `u`, so a `u.tag` projection keeps the
        # construct unchanged.
        sql = "SELECT u.tag FROM t CROSS JOIN (SELECT unnest(arr) AS a) u(tag) ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)

    def test_join_subquery_unqualified_column_still_rewritten(self):
        # No qualified `u.` reference, so the subquery collapse is safe.
        self.assertEqual(
            rewrite_query("SELECT tag FROM t CROSS JOIN (SELECT unnest(arr) AS a) u(tag) ON TRUE"),
            "SELECT tag FROM t ARRAY JOIN arr AS tag",
        )


class TestJsonExtractRewrite(unittest.TestCase):
    def test_arrow_text_operator_simple_identifier(self):
        # `expr ->> 'key'` must be translated to `JSONExtractString(expr, 'key')`,
        # not silently reduced to `expr` (which would benchmark a different
        # expression and could count a false success).
        self.assertEqual(
            rewrite_query("SELECT data ->> 'name' FROM t"),
            "SELECT JSONExtractString(data, 'name') FROM t",
        )

    def test_arrow_text_operator_in_predicate(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t WHERE data ->> 'name' = 'Alex'"),
            "SELECT * FROM t WHERE JSONExtractString(data, 'name') = 'Alex'",
        )

    def test_arrow_text_operator_qualified_identifier(self):
        self.assertEqual(
            rewrite_query("SELECT t.data ->> 'name' FROM t"),
            "SELECT JSONExtractString(t.data, 'name') FROM t",
        )

    def test_arrow_text_operator_complex_operand_left_unchanged(self):
        # The left operand is a function call, not a bare identifier, so the
        # conservative path leaves it unchanged instead of guessing a wrong
        # rewrite.
        sql = "SELECT json_extract(data) ->> 'name' FROM t"
        self.assertEqual(rewrite_query(sql), sql)

    def test_cast_to_jsonb_feeding_arrow_operator(self):
        # A removable `CAST(... AS JSONB)` feeding `->>` must be normalised away
        # before the operator translation, so the result is
        # `JSONExtractString(data, 'name')` rather than leaving the PostgreSQL
        # operator intact (`data ->> 'name'`) because the cast was only stripped
        # afterwards.
        self.assertEqual(
            rewrite_query("SELECT CAST(data AS JSONB) ->> 'name' FROM t"),
            "SELECT JSONExtractString(data, 'name') FROM t",
        )

    def test_pg_cast_jsonb_feeding_arrow_operator(self):
        # The `::jsonb` cast form feeding `->>` is likewise normalised first.
        self.assertEqual(
            rewrite_query("SELECT data::jsonb ->> 'name' FROM t"),
            "SELECT JSONExtractString(data, 'name') FROM t",
        )


class TestUnnestAliasWithoutAs(unittest.TestCase):
    def test_unnest_from_position_alias_without_as(self):
        # PostgreSQL allows omitting `AS` before a table-function alias; the
        # alias `u` is still preserved as the subquery alias.
        self.assertEqual(
            rewrite_query("SELECT * FROM UNNEST(arr) u(x)"),
            "SELECT * FROM (SELECT arrayJoin(arr) AS x) AS u",
        )

    def test_cross_join_unnest_alias_without_as(self):
        self.assertEqual(
            rewrite_query("SELECT * FROM t CROSS JOIN UNNEST(arr) u(x)"),
            "SELECT * FROM t \nARRAY JOIN arr AS x",
        )

    def test_unnest_without_alias_keyword_not_mistaken(self):
        # With `AS` omitted and no real alias, the trailing clause keyword `ON`
        # must not be captured as the alias; leave the construct unchanged.
        sql = "SELECT * FROM t LEFT JOIN UNNEST(arr) ON TRUE"
        self.assertEqual(rewrite_query(sql), sql)


class TestArrayJoinTrailingCommaSource(unittest.TestCase):
    def test_comma_joined_table_after_unnest_left_unchanged(self):
        # `v` is a separate comma-joined table source. Folding it into the new
        # `ARRAY JOIN` expression list (`ARRAY JOIN arr AS x, v`) would change
        # the query shape, so the construct is left unchanged.
        sql = "SELECT * FROM t, UNNEST(arr) AS u(x), v WHERE id = 1"
        self.assertEqual(rewrite_query(sql), sql)

    def test_comma_joined_table_after_unnest_on_true_left_unchanged(self):
        # Same hazard when a no-op `ON TRUE` is stripped first: the comma and the
        # following `v` must not be appended after the `ARRAY JOIN`.
        sql = "SELECT * FROM t LEFT JOIN UNNEST(arr) AS u(x) ON TRUE, v WHERE id = 1"
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
