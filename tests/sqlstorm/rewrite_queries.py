#!/usr/bin/env python3
"""
Rewrite SQLStorm PostgreSQL-dialect queries to ClickHouse-compatible SQL.

Only constructs that ClickHouse master still does not accept are rewritten
here. Function names and syntax sugar that the server already understands
either natively or via a case-insensitive alias are left alone (e.g.
`STDDEV`, `CARDINALITY`, `ARRAY_TO_STRING`, `REGEXP_SUBSTR`, `TRANSLATE`,
`ARRAY_AGG`, `STRING_AGG`, `EXTRACT(EPOCH|DOW|... FROM ...)`,
`date_part('unit', ...)`, `unnest(arr)` in expression position).

PostgreSQL `lhs = ANY(array_expr)` with a non-subquery operand is rewritten to
`has(array_expr, lhs)`, since ClickHouse only understands `ANY(subquery)`.
"""

import os
import re
import sys


def find_balanced_parens(s, start):
    """Find the matching closing paren for the opening paren at position start.
    Returns the index of the closing paren, or -1 if not found."""
    if start >= len(s) or s[start] != '(':
        return -1
    depth = 0
    i = start
    while i < len(s):
        if s[i] == '(':
            depth += 1
        elif s[i] == ')':
            depth -= 1
            if depth == 0:
                return i
        elif s[i] == "'" :
            # Skip string literals
            i += 1
            while i < len(s) and s[i] != "'":
                if s[i] == '\\':
                    i += 1
                i += 1
        i += 1
    return -1


def strip_outer_parens(s):
    """Strip surrounding whitespace and any fully-enclosing balanced parentheses.

    `((SELECT 1))` -> `SELECT 1`, but `(a) + (b)` is left unchanged because the
    first `(` does not match the final `)`."""
    s = s.strip()
    while s.startswith('(') and find_balanced_parens(s, 0) == len(s) - 1:
        s = s[1:-1].strip()
    return s


def rewrite_function_call(sql, func_name, rewriter):
    """Find and rewrite all calls to func_name(args) using the rewriter function.
    rewriter(args_string) -> replacement_string
    If the call is followed by FILTER (WHERE ...), the FILTER clause is relocated
    to the innermost groupArray/groupArrayIf call in the replacement."""
    result = []
    i = 0
    pat = re.compile(re.escape(func_name) + r'\s*\(', re.IGNORECASE)
    while i < len(sql):
        m = pat.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        # Check it's not part of a larger identifier
        if m.start() > 0 and (sql[m.start()-1].isalnum() or sql[m.start()-1] == '_'):
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        result.append(sql[i:m.start()])
        paren_start = m.end() - 1  # position of '('
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            # Unbalanced, leave as-is
            result.append(sql[m.start():])
            break
        args = sql[paren_start+1:paren_end]
        replacement = rewriter(args)
        if replacement is not None:
            # Check for trailing FILTER (WHERE ...) clause
            after = sql[paren_end + 1:]
            filter_match = re.match(r'\s*FILTER\s*\(', after, re.IGNORECASE)
            if filter_match:
                filter_paren_pos = paren_end + 1 + after.index('(')
                filter_paren_end = find_balanced_parens(sql, filter_paren_pos)
                if filter_paren_end != -1:
                    filter_clause = sql[paren_end + 1:filter_paren_end + 1].strip()
                    # Attach FILTER after the closing paren of innermost groupArray
                    # Find groupArray(...) and insert FILTER after its closing paren
                    ga_match = re.search(r'\b(groupArray(?:If)?)\s*\(', replacement)
                    if ga_match:
                        ga_paren = ga_match.end() - 1
                        ga_end = find_balanced_parens(replacement, ga_paren)
                        if ga_end != -1:
                            replacement = (
                                replacement[:ga_end + 1]
                                + ' ' + filter_clause
                                + replacement[ga_end + 1:]
                            )
                    i = filter_paren_end + 1
                    result.append(replacement)
                    continue
            result.append(replacement)
        else:
            # rewriter declined, keep original
            result.append(sql[m.start():paren_end+1])
        i = paren_end + 1
    return ''.join(result)


def split_top_level_args(args):
    """Split arguments at top-level commas (respecting parens and strings)."""
    parts = []
    depth = 0
    current = []
    i = 0
    while i < len(args):
        c = args[i]
        if c == '(' :
            depth += 1
            current.append(c)
        elif c == ')':
            depth -= 1
            current.append(c)
        elif c == ',' and depth == 0:
            parts.append(''.join(current).strip())
            current = []
        elif c == "'":
            current.append(c)
            i += 1
            while i < len(args) and args[i] != "'":
                current.append(args[i])
                i += 1
            if i < len(args):
                current.append(args[i])
        else:
            current.append(c)
        i += 1
    parts.append(''.join(current).strip())
    return parts


def rewrite_string_to_array(args):
    """string_to_array(str, sep) -> splitByString(sep, assumeNotNull(str))
    assumeNotNull prevents Array(Nullable(T)) which is illegal in ClickHouse."""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return None
    return f"splitByString({parts[1]}, assumeNotNull({parts[0]}))"


def rewrite_regexp_split_to_array(args):
    """regexp_split_to_array(str, pattern) -> splitByRegexp(pattern, str)"""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return None
    return f"splitByRegexp({parts[1]}, {parts[0]})"


def rewrite_random(args):
    """RANDOM() -> rand()"""
    return f"rand({args})"


def rewrite_age(args):
    """age(timestamp) -> dateDiff('year', timestamp, now())
       age(t1, t2) -> dateDiff('second', t2, t1)"""
    parts = split_top_level_args(args)
    if len(parts) == 1:
        return f"dateDiff('year', {parts[0]}, now())"
    elif len(parts) == 2:
        return f"dateDiff('second', {parts[1]}, {parts[0]})"
    return None


def rewrite_functions(sql):
    """Rewrite PostgreSQL function calls that ClickHouse master does not yet
    accept. Function names with a built-in case-insensitive alias (STDDEV,
    CARDINALITY, ARRAY_TO_STRING, REGEXP_SUBSTR, TRANSLATE, ARRAY_AGG,
    STRING_AGG, unnest, date_part, ...) are left for the server to resolve."""
    # STRING_AGGDistinct is a weird artifact — it's STRING_AGG(DISTINCT ...)
    # that got mangled in the upstream extraction. Rewrite as a groupArray
    # variant since `STRING_AGG(DISTINCT ...)` has no direct ClickHouse alias.
    def rewrite_string_agg_distinct(args):
        parts = split_top_level_args(args)
        if len(parts) == 1:
            parts.append("','")
        if len(parts) != 2:
            return None
        return f"arrayStringConcat(arrayDistinct(assumeNotNull(groupArray(assumeNotNull({parts[0]})))), {parts[1]})"
    sql = rewrite_function_call(sql, 'STRING_AGGDistinct', rewrite_string_agg_distinct)
    sql = rewrite_function_call(sql, 'string_aggDistinct', rewrite_string_agg_distinct)

    # string_to_array / STRING_TO_ARRAY (argument-reordered relative to splitByString)
    sql = rewrite_function_call(sql, 'string_to_array', rewrite_string_to_array)
    sql = rewrite_function_call(sql, 'STRING_TO_ARRAY', rewrite_string_to_array)

    # regexp_split_to_array / REGEXP_SPLIT_TO_ARRAY (argument-reordered relative
    # to splitByRegexp). REGEXP_SPLIT_TO_TABLE in PostgreSQL returns a set; we
    # approximate it as an array (caller has to ARRAY JOIN if a table is needed).
    sql = rewrite_function_call(sql, 'regexp_split_to_array', rewrite_regexp_split_to_array)
    sql = rewrite_function_call(sql, 'REGEXP_SPLIT_TO_ARRAY', rewrite_regexp_split_to_array)
    sql = rewrite_function_call(sql, 'REGEXP_SPLIT_TO_TABLE', rewrite_regexp_split_to_array)

    # RANDOM(): PostgreSQL's returns Float64 in [0, 1); rand() returns UInt32.
    # The semantics differ but most SQLStorm queries treat the return as an
    # opaque random value, so the substitution is acceptable in aggregate.
    sql = rewrite_function_call(sql, 'RANDOM', rewrite_random)

    # TO_TIMESTAMP(unix_seconds) -> toDateTime64(..., 6). fromUnixTimestamp only
    # accepts integers; toDateTime64 takes a Float and produces a microsecond
    # DateTime64, which matches PostgreSQL's `to_timestamp(double precision)`.
    sql = rewrite_function_call(sql, 'TO_TIMESTAMP', lambda args: f"toDateTime64({args}, 6)")

    # ARRAY_LENGTH(arr, dim) -> length(arr) (ignore dimension argument; all
    # ClickHouse arrays are 1-D from the user's perspective).
    def rewrite_array_length(args):
        parts = split_top_level_args(args)
        return f"length({parts[0]})"
    sql = rewrite_function_call(sql, 'ARRAY_LENGTH', rewrite_array_length)

    # SPLIT_PART(string, delimiter, position) -> splitByString(delimiter, string)[position]
    def rewrite_split_part(args):
        parts = split_top_level_args(args)
        if len(parts) == 3:
            return f"splitByString({parts[1]}, {parts[0]})[{parts[2]}]"
        return None
    sql = rewrite_function_call(sql, 'SPLIT_PART', rewrite_split_part)
    sql = rewrite_function_call(sql, 'split_part', rewrite_split_part)

    return sql


def rewrite_unnest_lateral(sql):
    """
    Rewrite UNNEST/LATERAL JOIN patterns to ClickHouse ARRAY JOIN. The standalone
    `unnest(expr)` function call form is handled natively by an alias on master.
    """
    # Remove LATERAL keyword (not supported in ClickHouse)
    sql = re.sub(r'\bLATERAL\s+', '', sql, flags=re.IGNORECASE)

    # Pattern: CROSS JOIN UNNEST(expr) AS alias(col) ON TRUE
    # -> ARRAY JOIN expr AS col
    sql = re.sub(
        r'\bCROSS\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+\w+\((\w+)\)\s*(?:ON\s+TRUE)?',
        r'ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: LEFT JOIN UNNEST(expr) AS alias(col) ON TRUE
    # -> LEFT ARRAY JOIN expr AS col
    sql = re.sub(
        r'\bLEFT\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+\w+\((\w+)\)\s*(?:ON\s+TRUE)?',
        r'LEFT ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: CROSS JOIN UNNEST(expr) AS col ON TRUE (no parens around col)
    sql = re.sub(
        r'\bCROSS\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*(?:ON\s+TRUE)?',
        r'ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: LEFT JOIN UNNEST(expr) AS col ON TRUE
    sql = re.sub(
        r'\bLEFT\s+JOIN\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*(?:ON\s+TRUE)?',
        r'LEFT ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: , UNNEST(expr) AS col (in FROM clause, comma-joined)
    sql = re.sub(
        r',\s*[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*(?:ON\s+TRUE)?',
        r' ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    # Pattern: JOIN (SELECT unnest(expr) AS col) alias ON TRUE
    # -> ARRAY JOIN expr AS col
    sql = re.sub(
        r'\b(?:CROSS\s+)?JOIN\s+\(\s*SELECT\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*\)\s*\w*\s*ON\s+TRUE',
        r'ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'\bLEFT\s+JOIN\s+\(\s*SELECT\s+[Uu][Nn][Nn][Ee][Ss][Tt]\(([^)]+)\)\s+AS\s+(\w+)\s*\)\s*\w*\s*ON\s+TRUE',
        r'LEFT ARRAY JOIN \1 AS \2',
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def rewrite_pg_cast(sql):
    """Fix PostgreSQL-specific cast patterns that ClickHouse doesn't support.
    ClickHouse supports :: for basic types, but not ::type[] (array casts),
    ::jsonb, ::json, or ::regclass."""
    # ::int[] or ::integer[] -> remove (ClickHouse doesn't have array type cast syntax)
    sql = re.sub(r'::\s*(?:int|integer|bigint|text|varchar|float|double)\s*\[\s*\]', '', sql, flags=re.IGNORECASE)
    # ::jsonb / ::json -> remove (ClickHouse has no JSON type in this sense)
    sql = re.sub(r'::\s*jsonb?\b', '', sql, flags=re.IGNORECASE)
    # ::regclass -> remove
    sql = re.sub(r'::\s*regclass\b', '', sql, flags=re.IGNORECASE)
    # PostgreSQL JSON operators: ->> 'key' -> JSONExtractString(expr, 'key')
    # This is complex to handle in general; just remove the operator for now
    sql = re.sub(r"\s*->>\s*'(\w+)'", r"", sql)
    return sql


def rewrite_arrayjoin_to_array_join(sql):
    """Convert arrayJoin(expr) in FROM/JOIN position to ARRAY JOIN expr AS alias.
    Uses find_balanced_parens to correctly handle nested parentheses."""
    # Find all occurrences of arrayJoin( in the SQL
    pat = re.compile(r'arrayJoin\s*\(', re.IGNORECASE)
    result = []
    i = 0
    while i < len(sql):
        m = pat.search(sql, i)
        if not m:
            result.append(sql[i:])
            break

        # Check if this arrayJoin is in a JOIN/FROM position by looking at preceding context.
        prefix = sql[:m.start()]
        prefix_stripped = prefix.rstrip()

        # Explicit JOIN keyword or FROM keyword means join/table context
        is_join_context = bool(re.search(
            r'(?:\bJOIN|\bLEFT\s+JOIN|\bCROSS\s+JOIN|\bRIGHT\s+JOIN|\bFROM)\s*$',
            prefix_stripped, re.IGNORECASE,
        ))

        # Comma before arrayJoin: only a join context if the most recent keyword
        # before the comma is FROM (not SELECT/ORDER BY/GROUP BY etc.)
        if not is_join_context and re.search(r',\s*$', prefix_stripped):
            # Find the last SELECT or FROM keyword to determine context
            from_match = list(re.finditer(r'\bFROM\b', prefix_stripped, re.IGNORECASE))
            select_match = list(re.finditer(r'\bSELECT\b', prefix_stripped, re.IGNORECASE))
            last_from = from_match[-1].start() if from_match else -1
            last_select = select_match[-1].start() if select_match else -1
            if last_from > last_select:
                is_join_context = True

        if not is_join_context:
            result.append(sql[i:m.end()])
            i = m.end()
            continue

        # Find the matching closing paren
        paren_start = m.end() - 1  # position of '('
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            result.append(sql[i:m.end()])
            i = m.end()
            continue

        expr = sql[paren_start + 1:paren_end]
        after = sql[paren_end + 1:]

        # Parse: AS alias(col) or AS alias, optionally followed by ON ...
        # Allow optional whitespace before the column alias list, since the
        # corpus also contains `AS tag (TagName) ON TRUE`. Without it `col`
        # stays `None` and the ` (TagName) ON TRUE` tail leaks into the result.
        alias_match = re.match(r'\s+AS\s+(\w+)(?:\s*\(\s*(\w+)\s*\))?(.*)', after, re.IGNORECASE | re.DOTALL)
        if not alias_match:
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        alias = alias_match.group(1)
        col = alias_match.group(2)  # may be None
        rest = alias_match.group(3)
        col_name = col if col else alias

        # Strip optional ON clause (ON TRUE, ON col IS NOT NULL, or arbitrary ON condition).
        # Bound the match before the next SQL clause keyword so the predicate
        # does not greedily consume the rest of the query (`WHERE`, `ORDER BY`, ...).
        on_match = re.match(
            r'\s+ON\s+(?:TRUE\b|(?:(?!\s+(?:WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|WINDOW|QUALIFY|SETTINGS|FORMAT|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|CROSS\s+JOIN|FULL\s+JOIN|INNER\s+JOIN)\b)[^\n,);])+)',
            rest,
            re.IGNORECASE,
        )
        if on_match:
            rest = rest[on_match.end():]

        # Determine join type from prefix
        join_type = "ARRAY JOIN"
        left_match = re.search(r'\bLEFT\s+JOIN\s*$', prefix_stripped, re.IGNORECASE)
        cross_match = re.search(r'\bCROSS\s+JOIN\s*$', prefix_stripped, re.IGNORECASE)
        plain_join_match = re.search(r'\bJOIN\s*$', prefix_stripped, re.IGNORECASE)
        from_match = re.search(r'\bFROM\s*$', prefix_stripped, re.IGNORECASE)
        comma_match = re.search(r',\s*$', prefix_stripped)

        if left_match:
            join_type = "LEFT ARRAY JOIN"
            prefix_stripped = prefix_stripped[:left_match.start()]
        elif cross_match:
            join_type = "ARRAY JOIN"
            prefix_stripped = prefix_stripped[:cross_match.start()]
        elif plain_join_match:
            join_type = "ARRAY JOIN"
            prefix_stripped = prefix_stripped[:plain_join_match.start()]
        elif from_match:
            # FROM arrayJoin(expr) AS alias -> FROM (SELECT arrayJoin(expr) AS alias)
            # Wrap in a subquery since arrayJoin is not a table function
            result.append(sql[i:len(prefix_stripped)])
            result.append(f" (SELECT arrayJoin({expr}) AS {col_name}) AS _aj")
            sql = rest
            i = 0
            continue
        elif comma_match:
            join_type = "ARRAY JOIN"
            prefix_stripped = prefix_stripped[:comma_match.start()]

        result.append(sql[i:len(prefix_stripped)])
        result.append(f"\n{join_type} {expr} AS {col_name}")
        sql = rest
        i = 0

    return ''.join(result)


def rewrite_any_array(sql):
    """Rewrite PostgreSQL `lhs = ANY(array_expr)` with a non-subquery operand to
    `has(array_expr, lhs)`.

    ClickHouse only has special `ANY` handling for subqueries, not for
    PostgreSQL-style array operands, so `lhs = ANY(SELECT ...)` /
    `lhs = ANY(WITH ...)` are left untouched for the native subquery path.
    Only simple or qualified left-hand identifiers are rewritten; more complex
    left-hand sides are left alone rather than risk producing wrong SQL."""
    result = []
    i = 0
    # Capture a simple or qualified (optionally quoted) identifier as the
    # left-hand side, immediately followed by `= ANY(`.
    pat = re.compile(r'("?\w+"?(?:\s*\.\s*"?\w+"?)*)\s*=\s*ANY\s*\(', re.IGNORECASE)
    while i < len(sql):
        m = pat.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        paren_start = m.end() - 1  # position of '('
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        inner = sql[paren_start + 1:paren_end]
        # Leave subquery operands to ClickHouse's native `ANY(subquery)` handling.
        # Strip any enclosing parentheses first so parenthesized subqueries such
        # as `ANY((SELECT ...))` are also recognized and left untouched, instead
        # of being rewritten to `has((SELECT ...), lhs)` (invalid: `has` requires
        # an array operand).
        if re.match(r'(?:SELECT|WITH)\b', strip_outer_parens(inner), re.IGNORECASE):
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue
        lhs = m.group(1)
        result.append(sql[i:m.start()])
        result.append(f"has({inner}, {lhs})")
        i = paren_end + 1
    return ''.join(result)


def rewrite_query(sql):
    """Apply all rewrites to a SQL query."""
    # 1. Function rewrites (handle balanced parens)
    sql = rewrite_functions(sql)

    # 2. Syntax pattern rewrites
    sql = rewrite_unnest_lateral(sql)
    sql = rewrite_pg_cast(sql)
    sql = rewrite_any_array(sql)

    # 3. Rewrite PostgreSQL `AT TIME ZONE 'tz'` -> `toTimezone(expr, 'tz')`.
    sql = re.sub(
        r"(\w+(?:\.\w+)?)\s+AT\s+TIME\s+ZONE\s+'([^']+)'",
        r"toTimezone(\1, '\2')",
        sql,
        flags=re.IGNORECASE,
    )

    # 4. Remove `CAST(... AS JSON/JSONB)` — ClickHouse has no JSON type for casts.
    sql = re.sub(r'\bCAST\s*\(([^)]+)\s+AS\s+JSON(?:B)?\s*\)', r'\1', sql, flags=re.IGNORECASE)

    # 5. Convert arrayJoin(...) in FROM/JOIN position to ARRAY JOIN syntax.
    sql = rewrite_arrayjoin_to_array_join(sql)

    # 6. Fix remaining "LEFT JOIN\nARRAY JOIN" -> "LEFT ARRAY JOIN" patterns.
    sql = re.sub(
        r'\bLEFT\s+JOIN\s*\n(\s*)ARRAY\s+JOIN\b',
        r'LEFT ARRAY JOIN',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'\bCROSS\s+JOIN\s*\n(\s*)ARRAY\s+JOIN\b',
        r'ARRAY JOIN',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(r'\bLEFT\s+JOIN\s+ARRAY\s+JOIN\b', 'LEFT ARRAY JOIN', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCROSS\s+JOIN\s+ARRAY\s+JOIN\b', 'ARRAY JOIN', sql, flags=re.IGNORECASE)

    # 7. Fix PostgreSQL `OFFSET X LIMIT Y` -> ClickHouse `LIMIT Y OFFSET X`.
    sql = re.sub(
        r'\bOFFSET\s+(\d+)\s+LIMIT\s+(\d+)',
        r'LIMIT \2 OFFSET \1',
        sql,
        flags=re.IGNORECASE,
    )

    # 8a. Rewrite SQL-standard `OFFSET N ROW[S] FETCH FIRST M ROW[S] ONLY` together.
    #     ClickHouse accepts this form, but only with ORDER BY; rewriting to
    #     `LIMIT M OFFSET N` works in both cases. Must run before 8b so that the
    #     standalone FETCH rewrite below does not strip FETCH while leaving the
    #     orphan `OFFSET ... ROWS` behind.
    sql = re.sub(
        r'\bOFFSET\s+(\d+)\s+ROWS?\s+FETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b',
        r'LIMIT \2 OFFSET \1',
        sql,
        flags=re.IGNORECASE,
    )

    # 8b. Convert standalone `FETCH FIRST N ROWS ONLY` to `LIMIT N`.
    sql = re.sub(
        r'\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b',
        r'LIMIT \1',
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <query_dir> [--dry-run] [--stats]")
        sys.exit(1)

    query_dir = sys.argv[1]
    dry_run = '--dry-run' in sys.argv
    show_stats = '--stats' in sys.argv

    files = sorted(f for f in os.listdir(query_dir) if f.endswith('.sql'))

    changed = 0
    unchanged = 0
    errors = 0

    for fname in files:
        path = os.path.join(query_dir, fname)
        try:
            with open(path, 'r') as f:
                original = f.read()

            rewritten = rewrite_query(original)

            if rewritten != original:
                changed += 1
                if not dry_run:
                    with open(path, 'w') as f:
                        f.write(rewritten)
                if show_stats and changed <= 5:
                    print(f"--- {fname} ---")
                    # Show a compact diff
                    orig_lines = original.splitlines()
                    new_lines = rewritten.splitlines()
                    for i, (o, n) in enumerate(zip(orig_lines, new_lines)):
                        if o != n:
                            print(f"  L{i+1} - {o.strip()[:100]}")
                            print(f"  L{i+1} + {n.strip()[:100]}")
            else:
                unchanged += 1
        except Exception as e:
            errors += 1
            print(f"ERROR processing {fname}: {e}", file=sys.stderr)

    print(f"\nResults: {changed} changed, {unchanged} unchanged, {errors} errors")
    print(f"Total: {changed + unchanged + errors} files")


if __name__ == '__main__':
    main()
