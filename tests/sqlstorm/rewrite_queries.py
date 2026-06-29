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


# String literals, quoted identifiers and comments are "protected spans": the
# syntax rewrites below must never fire inside them, otherwise they would alter
# literal values (e.g. `SELECT 'OFFSET 5 FETCH FIRST 10 ROWS ONLY'`) instead of
# dialect syntax. Before rewriting we replace each protected span with an opaque,
# identifier-shaped placeholder that survives every rewrite as a single token,
# and restore the original text at the end.
_PROTECTED_RE = re.compile(
    r"""
      '(?:[^'\\]|\\.|'')*'        # single-quoted string literal
    | "(?:[^"\\]|\\.|"")*"        # double-quoted identifier
    | `(?:[^`\\]|\\.|``)*`        # backtick-quoted identifier
    | \$(?P<dqtag>(?:[A-Za-z_]\w*)?)\$.*?\$(?P=dqtag)\$
                                  # PostgreSQL dollar-quoted literal:
                                  # $$...$$ or $tag$...$tag$. The tag group is
                                  # always-participating (possibly empty) so the
                                  # backreference also works for the bare $$ form.
    | --[^\n]*                    # line comment
    | /\*.*?\*/                   # block comment
    """,
    re.VERBOSE | re.DOTALL,
)

# Pattern fragment matching a placeholder, for embedding in rewrites that must
# still recognize a (now masked) string literal as a syntactic argument.
_PLACEHOLDER = r"__sqlstorm_protected_\d+__"

_RESTORE_RE = re.compile(r"__sqlstorm_protected_(\d+)__")

# Words that can syntactically follow a table expression but are clause
# keywords, not a table alias. Used to disambiguate an omitted-`AS` alias from a
# trailing clause when parsing `UNNEST(...)` / subquery joins.
_NOT_AN_ALIAS = {
    'ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'UNION',
    'INTERSECT', 'EXCEPT', 'WINDOW', 'QUALIFY', 'SETTINGS', 'FORMAT', 'USING',
    'JOIN', 'LEFT', 'RIGHT', 'CROSS', 'INNER', 'FULL',
}


def mask_protected_spans(sql):
    """Replace string literals, quoted identifiers and comments with opaque
    identifier-shaped placeholders so the syntax rewrites cannot fire inside
    them. Returns `(masked_sql, spans)` where `spans[i]` is the original text of
    placeholder `i`."""
    spans = []

    def repl(m):
        spans.append(m.group(0))
        return f"__sqlstorm_protected_{len(spans) - 1}__"

    return _PROTECTED_RE.sub(repl, sql), spans


def restore_protected_spans(sql, spans):
    """Inverse of `mask_protected_spans`. Restored text is not re-scanned, so a
    literal that happens to contain a placeholder-shaped substring is safe."""
    return _RESTORE_RE.sub(lambda m: spans[int(m.group(1))], sql)


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


def alias_used_as_qualifier(alias, *contexts):
    """True if `alias` is used as a table qualifier (`alias.column`) in any of
    the given SQL fragments.

    `UNNEST(arr) AS u(x)` / `(SELECT unnest(arr) AS a) u(x)` in JOIN position is
    rewritten to `ARRAY JOIN arr AS x`, which exposes the unnested column
    unqualified: the table alias `u` no longer exists afterwards. If the query
    still references that alias as a qualifier (`u.x`) anywhere outside the table
    expression, dropping it would leave the reference unresolved, so the caller
    leaves the whole construct unchanged instead. The `FROM`-position path keeps
    the alias via a subquery wrapper and does not need this guard.

    The lookbehind keeps the match precise: it skips `xu.` (a longer identifier
    ending in the alias) and `t.u.` (where the alias is an inner component of a
    different qualified name)."""
    if not alias:
        return False
    pat = re.compile(r'(?<![\w.])' + re.escape(alias) + r'\s*\.', re.IGNORECASE)
    return any(pat.search(ctx) for ctx in contexts if ctx)


def rewrite_unnest_lateral(sql):
    """
    Rewrite LATERAL and `JOIN (SELECT unnest(expr) AS col) ... ON TRUE` patterns.

    Direct `UNNEST(expr)` in JOIN/FROM position is handled structurally by
    `rewrite_arrayjoin_to_array_join` (balanced-parens scan), so it is not
    matched here. The standalone `unnest(expr)` function call form in expression
    position is handled natively by an alias on master.
    """
    # Remove LATERAL keyword (not supported in ClickHouse)
    sql = re.sub(r'\bLATERAL\s+', '', sql, flags=re.IGNORECASE)

    # Pattern: [LEFT|CROSS|INNER] JOIN (SELECT unnest(expr) AS col) [AS] [alias] [ON TRUE]
    # -> [LEFT] ARRAY JOIN expr AS col
    # The unnest operand is captured with a balanced-parentheses scan so nested
    # function calls such as `unnest(splitByString(',', tags))` (produced by the
    # earlier function rewrites) are taken in full; a `([^)]+)`-style regex would
    # stop at the first `)` and leave behind a correlated subquery that
    # ClickHouse cannot execute.
    # `RIGHT`/`FULL` are captured here only so the qualifier is not left dangling
    # in the prefix (which produced an invalid `RIGHT ARRAY JOIN ...`); those
    # joins are left unchanged below, mirroring the direct
    # `UNNEST(...)`/`arrayJoin(...)` path in `rewrite_arrayjoin_to_array_join`.
    join_pat = re.compile(
        r'\b(LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+|INNER\s+)?JOIN\s*\(',
        re.IGNORECASE,
    )
    result = []
    i = 0
    while i < len(sql):
        m = join_pat.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        join_kw = (m.group(1) or '').upper()
        if join_kw.startswith('RIGHT') or join_kw.startswith('FULL'):
            # `RIGHT`/`FULL ARRAY JOIN` has no ClickHouse equivalent; leave the
            # construct unchanged instead of dropping the qualifier.
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        paren_start = m.end() - 1
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        subquery = sql[paren_start + 1:paren_end]
        sub_m = re.match(r'\s*SELECT\s+unnest\s*\(', subquery, re.IGNORECASE)
        if not sub_m:
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        inner_paren = sub_m.end() - 1
        inner_end = find_balanced_parens(subquery, inner_paren)
        if inner_end == -1:
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        expr = subquery[inner_paren + 1:inner_end]
        # The subquery must consist of nothing but `SELECT unnest(expr) AS col`;
        # anything else (a FROM clause, extra select items, ...) disqualifies it.
        tail_m = re.match(r'\s+AS\s+(\w+)\s*$', subquery[inner_end + 1:], re.IGNORECASE)
        if not tail_m:
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        col = tail_m.group(1)
        # Consume an optional `[AS] alias` (with optional single-column list,
        # `u(tag)`, which renames the unnest column) after the closing
        # parenthesis.
        after = sql[paren_end + 1:]
        pos = 0
        table_alias = None
        alias_m = re.match(r'\s*(?:AS\s+)?(\w+)(?:\s*\(\s*(\w+)\s*\))?', after, re.IGNORECASE)
        if alias_m and alias_m.group(1).upper() not in _NOT_AN_ALIAS:
            pos = alias_m.end()
            table_alias = alias_m.group(1)
            if alias_m.group(2):
                col = alias_m.group(2)
        # Consume a no-op `ON TRUE`. A genuine join condition cannot be expressed
        # as `ARRAY JOIN`, so leave the whole construct untouched in that case.
        # `ON TRUE AND ...` / `ON TRUE OR ...` still carry a real predicate, so
        # the negative lookahead keeps them on the unchanged path instead of
        # consuming only `ON TRUE` and leaving the boolean tail dangling (which
        # would produce `ARRAY JOIN arr AS a AND ...`, invalid ClickHouse SQL).
        on_m = re.match(r'\s*ON\b', after[pos:], re.IGNORECASE)
        if on_m:
            on_true_m = re.match(r'\s*ON\s+TRUE\b(?!\s+(?:AND|OR)\b)', after[pos:], re.IGNORECASE)
            if not on_true_m:
                result.append(sql[i:m.end()])
                i = m.end()
                continue
            pos += on_true_m.end()
        elif re.match(r'\s*(?:USING\b|[,(])', after[pos:], re.IGNORECASE):
            # A `USING (...)` clause is a real join predicate that `ARRAY JOIN`
            # cannot carry, so it is left unchanged like a non-trivial `ON`.
            # Without `ON TRUE`, a following comma would also merge the next FROM
            # item into the ARRAY JOIN expression list, and a leftover `(` means
            # the alias tail was not fully parsed; be conservative in all cases.
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        # `ARRAY JOIN` exposes the unnested column unqualified, so the table
        # alias (`u` in `(SELECT unnest(arr) AS a) u(tag)`) ceases to exist. If
        # the query references it as a qualifier (`u.tag`) outside the table
        # expression, leave the whole construct unchanged rather than emit an
        # unresolved reference.
        if alias_used_as_qualifier(table_alias, sql[:m.start()], after[pos:]):
            result.append(sql[i:m.end()])
            i = m.end()
            continue
        join_type = 'LEFT ARRAY JOIN' if join_kw.startswith('LEFT') else 'ARRAY JOIN'
        result.append(sql[i:m.start()])
        result.append(f"{join_type} {expr} AS {col}")
        i = paren_end + 1 + pos

    return ''.join(result)


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
    # `CAST(expr AS JSON/JSONB)` -> `expr`. ClickHouse has no JSON type for
    # casts. This must run before the `->>` translation below: a JSON cast
    # feeding the operator (`CAST(data AS JSONB) ->> 'name'`) is first reduced to
    # the bare identifier `data ->> 'name'`, which the translation then turns
    # into `JSONExtractString(data, 'name')`. Removing the cast afterwards (the
    # previous ordering) left the PostgreSQL operator intact because the operand
    # was still a complex `CAST(...)` when the translation ran.
    sql = re.sub(r'\bCAST\s*\(([^)]+)\s+AS\s+JSON(?:B)?\s*\)', r'\1', sql, flags=re.IGNORECASE)
    # PostgreSQL JSON text-extraction operator: `expr ->> 'key'`.
    # Translate it to the ClickHouse equivalent `JSONExtractString(expr, 'key')`
    # when the left operand is a simple or qualified (optionally quoted)
    # identifier. A more complex operand (function call, arithmetic, cast, ...)
    # is left unchanged rather than guessed at: silently deleting the operator
    # and key (the previous behaviour) turned `data ->> 'name'` into `data`,
    # which changes the query and can count false successes for the wrong
    # expression. The key literal is masked at this point, so it is matched as a
    # placeholder and restored to the original quoted string at the end.
    sql = re.sub(
        r'("?\w+"?(?:\s*\.\s*"?\w+"?)*)\s*->>\s*(' + _PLACEHOLDER + r')',
        r'JSONExtractString(\1, \2)',
        sql,
    )
    return sql


def rewrite_arrayjoin_to_array_join(sql):
    """Convert arrayJoin(expr)/UNNEST(expr) in FROM/JOIN position to
    `ARRAY JOIN expr AS alias`. Uses find_balanced_parens to correctly handle
    nested parentheses, so function operands such as
    `UNNEST(splitByString(',', tags))` are captured in full. `unnest(expr)` in
    expression position (prefix is not a JOIN/FROM keyword) is left alone for the
    server's native alias to resolve."""
    # Find all occurrences of arrayJoin( or unnest( in the SQL
    pat = re.compile(r'(?:arrayJoin|unnest)\s*\(', re.IGNORECASE)
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

        # `UNNEST(arr) WITH ORDINALITY [AS] u(x, n)` is a PostgreSQL
        # table-function clause that adds a 1-based ordinality column. `ARRAY
        # JOIN` has no direct equivalent for it, and `WITH` is not a clause
        # keyword in `_NOT_AN_ALIAS`, so without this guard the omitted-`AS`
        # alias parser below would accept the bare `WITH` token as the table
        # alias and emit invalid `ARRAY JOIN arr AS WITH ORDINALITY AS u(x, n)`.
        # Leave the whole construct unchanged, mirroring the conservative
        # handling of other shapes that `ARRAY JOIN` cannot represent.
        if re.match(r'\s+WITH\s+ORDINALITY\b', after, re.IGNORECASE):
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        # Parse: [AS] alias(col) or [AS] alias, optionally followed by ON ...
        # `AS` is optional because PostgreSQL table-function aliases commonly
        # omit it (`UNNEST(arr) u(x)`). Allow optional whitespace before the
        # column alias list, since the corpus also contains `AS tag (TagName) ON
        # TRUE`. Without it `col` stays `None` and the ` (TagName) ON TRUE` tail
        # leaks into the result.
        alias_match = re.match(r'\s+(AS\s+)?(\w+)(?:\s*\(\s*(\w+)\s*\))?(.*)', after, re.IGNORECASE | re.DOTALL)
        if not alias_match:
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        as_kw = alias_match.group(1)  # the literal `AS `, or None when omitted
        alias = alias_match.group(2)
        col = alias_match.group(3)  # may be None
        rest = alias_match.group(4)
        col_name = col if col else alias

        # When `AS` is omitted, the token after the unnest is ambiguous: it can
        # be a real alias or a trailing clause keyword (`UNNEST(arr) ON TRUE`,
        # `UNNEST(arr) WHERE ...`). Only treat it as an alias when it is not a
        # clause keyword; otherwise leave the construct unchanged. With an
        # explicit `AS` the token is unambiguously an alias.
        if as_kw is None and alias.upper() in _NOT_AN_ALIAS:
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        # Determine the join type from the preceding keyword. `ARRAY JOIN`
        # carries no `ON` clause and has no RIGHT/FULL variant, so any shape
        # that cannot be represented is left unchanged rather than rewritten
        # into different (or invalid) SQL.
        from_match = re.search(r'\bFROM\s*$', prefix_stripped, re.IGNORECASE)
        comma_match = re.search(r',\s*$', prefix_stripped)
        # Capture the optional join qualifier (`LEFT [OUTER]`, `INNER`, ...)
        # together with the trailing `JOIN` so the whole clause is stripped,
        # not just the bare `JOIN` token (which would leave a stray `INNER`).
        join_kw_match = re.search(
            r'\b(?:(LEFT(?:\s+OUTER)?|RIGHT(?:\s+OUTER)?|FULL(?:\s+OUTER)?|INNER|CROSS)\s+)?JOIN\s*$',
            prefix_stripped, re.IGNORECASE,
        )

        wrap_in_subquery = False
        if from_match:
            join_type = None
            wrap_in_subquery = True
            new_prefix = prefix_stripped[:from_match.start()]
        elif comma_match:
            join_type = "ARRAY JOIN"
            new_prefix = prefix_stripped[:comma_match.start()]
        elif join_kw_match:
            qualifier = (join_kw_match.group(1) or "").strip().upper()
            if qualifier.startswith("LEFT"):
                join_type = "LEFT ARRAY JOIN"
            elif qualifier.startswith("RIGHT") or qualifier.startswith("FULL"):
                # RIGHT/FULL ARRAY JOIN has no ClickHouse equivalent; leave the
                # construct unchanged.
                result.append(sql[i:paren_end + 1])
                i = paren_end + 1
                continue
            else:
                # Plain JOIN, INNER JOIN and CROSS JOIN all map to ARRAY JOIN.
                join_type = "ARRAY JOIN"
            new_prefix = prefix_stripped[:join_kw_match.start()]
        else:
            # Unrecognised context; leave unchanged.
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        # Only a no-op `ON TRUE` predicate can be dropped. A real `ON` predicate
        # or a `USING (...)` clause is a genuine join condition that `ARRAY JOIN`
        # cannot carry, so the whole construct is left unchanged instead of
        # silently dropping the filter (which would turn a filtered join into an
        # unfiltered cross product) or emitting invalid SQL such as
        # `ARRAY JOIN arr AS id USING (id)`.
        on_true_match = re.match(r'\s+ON\s+TRUE\b(?!\s+(?:AND|OR)\b)', rest, re.IGNORECASE)
        if on_true_match:
            rest = rest[on_true_match.end():]
        elif re.match(r'\s+(?:ON|USING)\b', rest, re.IGNORECASE):
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        # A leading comma in the remainder means another `FROM` item (a comma
        # join) trails the unnest, e.g. `FROM t, UNNEST(arr) AS u(x), v`.
        # Appending it after the new `ARRAY JOIN` would fold that table into the
        # array-join expression list (`ARRAY JOIN arr AS x, v`), so `v` is no
        # longer a separate table source. A leftover `(` means the alias tail was
        # not fully parsed (e.g. a multi-column list `u(a, b)`). Mirror the
        # conservative guard in `rewrite_unnest_lateral` and leave the construct
        # unchanged in both cases.
        if re.match(r'\s*[,(]', rest):
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        # The comma/`JOIN` rewrite to `ARRAY JOIN expr AS col` drops the table
        # alias (`u` in `UNNEST(arr) AS u(x)`); only the column alias survives.
        # If the query references that table alias as a qualifier (`u.x`) outside
        # the table expression, leave the construct unchanged so the reference
        # does not become unresolved. The `FROM` path below keeps the alias via a
        # subquery wrapper and is exempt.
        if not wrap_in_subquery and alias_used_as_qualifier(alias, new_prefix, rest):
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        result.append(sql[i:len(new_prefix)])
        if wrap_in_subquery:
            # FROM UNNEST(arr) AS u(x) -> FROM (SELECT arrayJoin(arr) AS x) AS u
            # arrayJoin/UNNEST is not a table function, so it is wrapped in a
            # subquery. The original table alias (`u`) is kept as the subquery
            # alias so a qualified projection such as `u.x` still resolves; a
            # synthetic alias would silently break that reference. The `FROM`
            # keyword was dropped together with the rest of `new_prefix`, so it
            # is re-emitted here; otherwise the result is a stray
            # `SELECT ... (SELECT ...)` with no `FROM`.
            result.append(f"FROM (SELECT arrayJoin({expr}) AS {col_name}) AS {alias}")
        else:
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
    # Characters that, when they immediately precede the captured identifier,
    # prove it is only the tail of a larger left-hand expression rather than the
    # whole operand. Rewriting in that case would be wrong: `a + b = ANY(arr)`
    # would become `a + has(arr, b)` and `a::integer = ANY(arr)` would become
    # `a::has(arr, integer)`. The conservative contract above requires leaving
    # such expressions untouched.
    lhs_continuation = set("+-*/%|&^~:.)]'")
    while i < len(sql):
        m = pat.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        # Verify the captured identifier is the whole left-hand side: scan back
        # past whitespace and skip the rewrite if it is glued to a preceding
        # operator, cast (`::`), or closing bracket.
        j = m.start() - 1
        while j >= 0 and sql[j].isspace():
            j -= 1
        if j >= 0 and sql[j] in lhs_continuation:
            result.append(sql[i:m.end()])
            i = m.end()
            continue
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
    # 0. Hide string literals, quoted identifiers and comments so none of the
    #    syntax rewrites below can fire inside them. Restored at the end.
    sql, protected_spans = mask_protected_spans(sql)

    # 1. Function rewrites (handle balanced parens)
    sql = rewrite_functions(sql)

    # 2. Syntax pattern rewrites
    sql = rewrite_unnest_lateral(sql)
    sql = rewrite_pg_cast(sql)
    sql = rewrite_any_array(sql)

    # 3. Rewrite PostgreSQL `AT TIME ZONE 'tz'` -> `toTimezone(expr, 'tz')`.
    #    The timezone literal is masked, so match its placeholder; it restores
    #    to the original quoted string at the end.
    sql = re.sub(
        r"(\w+(?:\.\w+)?)\s+AT\s+TIME\s+ZONE\s+(" + _PLACEHOLDER + r")",
        r"toTimezone(\1, \2)",
        sql,
        flags=re.IGNORECASE,
    )

    # 4. Convert arrayJoin(...) in FROM/JOIN position to ARRAY JOIN syntax.
    sql = rewrite_arrayjoin_to_array_join(sql)

    # 5. Fix remaining "LEFT JOIN\nARRAY JOIN" -> "LEFT ARRAY JOIN" patterns.
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

    # 6. Fix PostgreSQL `OFFSET X LIMIT Y` -> ClickHouse `LIMIT Y OFFSET X`.
    sql = re.sub(
        r'\bOFFSET\s+(\d+)\s+LIMIT\s+(\d+)',
        r'LIMIT \2 OFFSET \1',
        sql,
        flags=re.IGNORECASE,
    )

    # 7a. Rewrite SQL-standard `OFFSET N ROW[S] FETCH FIRST M ROW[S] ONLY` together.
    #     ClickHouse accepts this form, but only with ORDER BY; rewriting to
    #     `LIMIT M OFFSET N` works in both cases. Must run before 7b so that the
    #     standalone FETCH rewrite below does not strip FETCH while leaving the
    #     orphan `OFFSET ... ROWS` behind.
    sql = re.sub(
        r'\bOFFSET\s+(\d+)\s+ROWS?\s+FETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b',
        r'LIMIT \2 OFFSET \1',
        sql,
        flags=re.IGNORECASE,
    )

    # 7b. Convert standalone `FETCH FIRST N ROWS ONLY` to `LIMIT N`.
    sql = re.sub(
        r'\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b',
        r'LIMIT \1',
        sql,
        flags=re.IGNORECASE,
    )

    # 8. Restore the string literals, quoted identifiers and comments hidden in
    #    step 0.
    sql = restore_protected_spans(sql, protected_spans)

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
