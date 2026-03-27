#!/usr/bin/env python3
"""
Rewrite SQLStorm PostgreSQL-dialect queries to ClickHouse-compatible SQL.
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


def rewrite_string_agg(args):
    """STRING_AGG(expr, sep) -> arrayStringConcat(assumeNotNull(groupArray(assumeNotNull(expr))), sep)
       STRING_AGG(DISTINCT expr, sep) -> arrayStringConcat(arrayDistinct(assumeNotNull(groupArray(assumeNotNull(expr)))), sep)
       STRING_AGG(expr) -> same with default separator ','
       STRING_AGG(expr, sep ORDER BY col) -> strip ORDER BY (not supported in groupArray)
    The outer assumeNotNull is needed because group_by_use_nulls = 1 makes aggregate results Nullable,
    and Array cannot be inside Nullable."""
    # Strip PostgreSQL aggregate ORDER BY clause from args
    args = re.sub(r'\s+ORDER\s+BY\s+[^,)]+$', '', args.rstrip(), flags=re.IGNORECASE)
    parts = split_top_level_args(args)
    if len(parts) == 1:
        # Single-arg STRING_AGG: default separator is ','
        parts.append("','")
    if len(parts) != 2:
        return None
    expr = parts[0].strip()
    sep = parts[1].strip()
    # Strip ORDER BY that ended up in the separator
    sep = re.sub(r'\s+ORDER\s+BY\s+.*$', '', sep, flags=re.IGNORECASE).strip()
    if not sep:
        sep = "','"
    distinct = False
    if expr.upper().startswith('DISTINCT '):
        distinct = True
        expr = expr[9:].strip()
    if distinct:
        return f"arrayStringConcat(arrayDistinct(assumeNotNull(groupArray(assumeNotNull({expr})))), {sep})"
    else:
        return f"arrayStringConcat(assumeNotNull(groupArray(assumeNotNull({expr}))), {sep})"


def rewrite_array_agg(args):
    """ARRAY_AGG(expr) -> assumeNotNull(groupArray(assumeNotNull(expr)))
       ARRAY_AGG(DISTINCT expr) -> arrayDistinct(assumeNotNull(groupArray(assumeNotNull(expr))))
       Strips PostgreSQL ORDER BY inside the aggregate.
    The outer assumeNotNull prevents Nullable(Array(...)) from group_by_use_nulls."""
    expr = args.strip()
    # Strip ORDER BY clause
    expr = re.sub(r'\s+ORDER\s+BY\s+\S+(?:\s+(?:ASC|DESC))?(?:\s+NULLS\s+(?:FIRST|LAST))?', '', expr, flags=re.IGNORECASE)
    if expr.upper().startswith('DISTINCT '):
        expr = expr[9:].strip()
        return f"arrayDistinct(assumeNotNull(groupArray(assumeNotNull({expr}))))"
    return f"assumeNotNull(groupArray(assumeNotNull({expr})))"


def rewrite_string_to_array(args):
    """string_to_array(str, sep) -> splitByString(sep, assumeNotNull(str))
    assumeNotNull prevents Array(Nullable(T)) which is illegal in ClickHouse."""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return None
    return f"splitByString({parts[1]}, assumeNotNull({parts[0]}))"


def rewrite_date_part(args):
    """DATE_PART('unit', expr) -> EXTRACT(unit FROM expr) -- ClickHouse supports EXTRACT natively."""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return f"EXTRACT({args})"
    unit = parts[0].strip().strip("'\"")
    expr = parts[1].strip()
    return f"EXTRACT({unit} FROM {expr})"


def rewrite_regexp_split_to_array(args):
    """regexp_split_to_array(str, pattern) -> splitByRegexp(pattern, str)"""
    parts = split_top_level_args(args)
    if len(parts) != 2:
        return None
    return f"splitByRegexp({parts[1]}, {parts[0]})"


def rewrite_stddev(args):
    """STDDEV(expr) -> stddevPop(expr)"""
    return f"stddevPop({args})"


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
    """Rewrite PostgreSQL function calls to ClickHouse equivalents."""
    # STRING_AGG and STRING_AGGDistinct
    sql = rewrite_function_call(sql, 'STRING_AGG', rewrite_string_agg)
    sql = rewrite_function_call(sql, 'string_agg', rewrite_string_agg)
    # STRING_AGGDistinct is a weird artifact — it's STRING_AGG(DISTINCT ...)
    # that got mangled. Rewrite as groupArray variant.
    def rewrite_string_agg_distinct(args):
        parts = split_top_level_args(args)
        if len(parts) == 1:
            parts.append("','")
        if len(parts) != 2:
            return None
        return f"arrayStringConcat(arrayDistinct(assumeNotNull(groupArray(assumeNotNull({parts[0]})))), {parts[1]})"
    sql = rewrite_function_call(sql, 'STRING_AGGDistinct', rewrite_string_agg_distinct)
    sql = rewrite_function_call(sql, 'string_aggDistinct', rewrite_string_agg_distinct)

    # ARRAY_AGG
    sql = rewrite_function_call(sql, 'ARRAY_AGG', rewrite_array_agg)
    sql = rewrite_function_call(sql, 'array_agg', rewrite_array_agg)

    # string_to_array / STRING_TO_ARRAY
    sql = rewrite_function_call(sql, 'string_to_array', rewrite_string_to_array)
    sql = rewrite_function_call(sql, 'STRING_TO_ARRAY', rewrite_string_to_array)

    # DATE_PART / date_part
    sql = rewrite_function_call(sql, 'DATE_PART', rewrite_date_part)
    sql = rewrite_function_call(sql, 'date_part', rewrite_date_part)

    # regexp_split_to_array / REGEXP_SPLIT_TO_ARRAY
    sql = rewrite_function_call(sql, 'regexp_split_to_array', rewrite_regexp_split_to_array)
    sql = rewrite_function_call(sql, 'REGEXP_SPLIT_TO_ARRAY', rewrite_regexp_split_to_array)
    sql = rewrite_function_call(sql, 'REGEXP_SPLIT_TO_TABLE', rewrite_regexp_split_to_array)

    # STDDEV -> stddevPop
    sql = rewrite_function_call(sql, 'STDDEV', rewrite_stddev)

    # RANDOM -> rand
    sql = rewrite_function_call(sql, 'RANDOM', rewrite_random)

    # TO_TIMESTAMP -> toDateTime64
    sql = rewrite_function_call(sql, 'TO_TIMESTAMP', lambda args: f"toDateTime64({args}, 6)")

    # ARRAY_LENGTH(arr, dim) -> length(arr) (ignore dimension argument)
    def rewrite_array_length(args):
        parts = split_top_level_args(args)
        return f"length({parts[0]})"
    sql = rewrite_function_call(sql, 'ARRAY_LENGTH', rewrite_array_length)

    # CARDINALITY -> length (PostgreSQL CARDINALITY returns array length)
    sql = rewrite_function_call(sql, 'CARDINALITY', lambda args: f"length({args})")
    sql = rewrite_function_call(sql, 'cardinality', lambda args: f"length({args})")

    # ARRAY_TO_STRING -> arrayStringConcat
    def rewrite_array_to_string(args):
        parts = split_top_level_args(args)
        if len(parts) >= 2:
            return f"arrayStringConcat({parts[0]}, {parts[1]})"
        return None
    sql = rewrite_function_call(sql, 'ARRAY_TO_STRING', rewrite_array_to_string)
    sql = rewrite_function_call(sql, 'array_to_string', rewrite_array_to_string)

    # SPLIT_PART(string, delimiter, position) -> splitByChar(delimiter, string)[position]
    def rewrite_split_part(args):
        parts = split_top_level_args(args)
        if len(parts) == 3:
            return f"splitByString({parts[1]}, {parts[0]})[{parts[2]}]"
        return None
    sql = rewrite_function_call(sql, 'SPLIT_PART', rewrite_split_part)
    sql = rewrite_function_call(sql, 'split_part', rewrite_split_part)

    # REGEXP_SUBSTR -> regexpExtract
    sql = rewrite_function_call(sql, 'REGEXP_SUBSTR', lambda args: f"regexpExtract({args})")

    # TRANSLATE -> replaceAll (best-effort, only works for single-char replacements)
    sql = rewrite_function_call(sql, 'TRANSLATE', lambda args: f"replaceAll({args})")

    return sql


def rewrite_extract_epoch(sql):
    """EXTRACT(EPOCH FROM expr) -> toUnixTimestamp(expr)"""
    pat = re.compile(r'\bEXTRACT\s*\(\s*EPOCH\s+FROM\s+', re.IGNORECASE)
    while True:
        m = pat.search(sql)
        if not m:
            break
        # Find the matching closing paren for this EXTRACT(
        paren_start = sql.index('(', m.start())
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            break
        # The expr is between "FROM " and the closing paren
        from_pos = m.end()
        expr = sql[from_pos:paren_end]
        sql = sql[:m.start()] + f'toUnixTimestamp({expr})' + sql[paren_end+1:]
    return sql


def rewrite_extract_unit(sql):
    """ClickHouse supports EXTRACT(YEAR/MONTH/... FROM expr) natively.
    No rewrite needed."""
    return sql


def rewrite_fetch_offset(sql):
    """ClickHouse supports OFFSET/FETCH natively (requires ORDER BY).
    No rewrite needed."""
    return sql


def rewrite_interval(sql):
    """ClickHouse supports INTERVAL '30 days' natively. No rewrite needed."""
    return sql


def rewrite_cast_timestamp(sql):
    """ClickHouse supports CAST(... AS TIMESTAMP) and TIMESTAMP '...' natively.
    No rewrite needed."""
    return sql


def rewrite_current_timestamp(sql):
    """ClickHouse supports CURRENT_TIMESTAMP natively. No rewrite needed."""
    return sql


def rewrite_unnest_lateral(sql):
    """
    Rewrite UNNEST/LATERAL JOIN patterns to ClickHouse ARRAY JOIN.
    Also replaces standalone unnest(expr) calls with arrayJoin(expr).
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

    # Remaining standalone unnest(expr) calls -> arrayJoin(expr)
    sql = re.sub(r'\bunnest\s*\(', 'arrayJoin(', sql, flags=re.IGNORECASE)

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


def rewrite_bool_literals(sql):
    """PostgreSQL TRUE/FALSE are fine in ClickHouse, no change needed."""
    return sql


def rewrite_ilike(sql):
    """ILIKE is supported in ClickHouse, no change needed."""
    return sql


def rewrite_no_supertype(sql):
    """Fix signed/unsigned conflicts in COALESCE/CASE with COUNT.
    COALESCE(COUNT(...), -1) -> use 0 instead of -1 won't work.
    Actually: wrap in toInt64. This is rare (21 cases), skip for now."""
    return sql


def rewrite_any_comparison(sql):
    """Rewrite 'expr = ANY(array_expr)' to 'has(array_expr, expr)'.
    Also handles != ANY -> NOT has, and <> ANY -> NOT has."""
    pat = re.compile(r'(=|!=|<>)\s*ANY\s*\(', re.IGNORECASE)
    result = []
    i = 0
    while i < len(sql):
        m = pat.search(sql, i)
        if not m:
            result.append(sql[i:])
            break

        # Find the LHS expression (the token/expression before the operator)
        # Walk backwards from the operator to find the LHS
        op = m.group(1)
        prefix = sql[i:m.start()]
        result.append(prefix)

        # Find matching paren for ANY(
        paren_start = m.end() - 1
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            result.append(sql[m.start():m.end()])
            i = m.end()
            continue

        array_expr = sql[paren_start + 1:paren_end]

        # Extract the LHS from what we've accumulated so far.
        # Only match simple identifiers (possibly qualified with table/db prefix),
        # not arbitrary expressions like "a + b" — rewriting those would change semantics.
        accumulated = ''.join(result)
        lhs_match = re.search(r'((?:\w+\.)*\w+)\s*$', accumulated)
        if lhs_match:
            lhs = lhs_match.group(1)
            # Remove the LHS from accumulated result
            accumulated = accumulated[:lhs_match.start()]
            if op == '=':
                replacement = f"has({array_expr}, {lhs})"
            else:
                replacement = f"NOT has({array_expr}, {lhs})"
            result = [accumulated, replacement]
        else:
            # Can't find LHS, keep original
            result.append(sql[m.start():paren_end + 1])

        i = paren_end + 1

    return ''.join(result)


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
        alias_match = re.match(r'\s+AS\s+(\w+)(?:\((\w+)\))?(.*)', after, re.IGNORECASE | re.DOTALL)
        if not alias_match:
            result.append(sql[i:paren_end + 1])
            i = paren_end + 1
            continue

        alias = alias_match.group(1)
        col = alias_match.group(2)  # may be None
        rest = alias_match.group(3)
        col_name = col if col else alias

        # Strip optional ON clause (ON TRUE, ON col IS NOT NULL, or arbitrary ON condition)
        on_match = re.match(r'\s+ON\s+(?:TRUE\b|[^\n,)]+)', rest, re.IGNORECASE)
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


def rewrite_query(sql):
    """Apply all rewrites to a SQL query."""
    # Order matters: do function rewrites before simpler regex replacements

    # 1. Function rewrites (handle balanced parens)
    sql = rewrite_functions(sql)

    # 2. EXTRACT rewrites
    sql = rewrite_extract_epoch(sql)
    sql = rewrite_extract_unit(sql)

    # 3. Syntax pattern rewrites
    sql = rewrite_fetch_offset(sql)
    sql = rewrite_interval(sql)
    sql = rewrite_cast_timestamp(sql)
    sql = rewrite_current_timestamp(sql)
    sql = rewrite_unnest_lateral(sql)
    sql = rewrite_pg_cast(sql)

    # ClickHouse supports DATE '...', ::, TIMESTAMP '...', INTERVAL '...',
    # CURRENT_TIMESTAMP, EXTRACT(UNIT FROM ...), and FETCH/OFFSET natively.
    # No rewrites needed for these.

    # 3b. Rewrite PostgreSQL AT TIME ZONE 'tz' -> toTimezone(expr, 'tz')
    sql = re.sub(
        r"(\w+(?:\.\w+)?)\s+AT\s+TIME\s+ZONE\s+'([^']+)'",
        r"toTimezone(\1, '\2')",
        sql,
        flags=re.IGNORECASE,
    )

    # 3c. Remove CAST(... AS JSON/JSONB) — ClickHouse has no JSON type for casts
    sql = re.sub(r'\bCAST\s*\(([^)]+)\s+AS\s+JSON(?:B)?\s*\)', r'\1', sql, flags=re.IGNORECASE)

    # 4. Rewrite PostgreSQL = ANY(array_expr) to ClickHouse has(array_expr, lhs)
    sql = rewrite_any_comparison(sql)

    # 5. Convert arrayJoin(...) in FROM/JOIN position to ARRAY JOIN syntax.
    #    Uses find_balanced_parens for robust matching of nested expressions.
    sql = rewrite_arrayjoin_to_array_join(sql)

    # 6. Fix remaining "LEFT JOIN\nARRAY JOIN" -> "LEFT ARRAY JOIN" patterns
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

    # 7. Fix PostgreSQL OFFSET X LIMIT Y -> ClickHouse LIMIT Y OFFSET X
    sql = re.sub(
        r'\bOFFSET\s+(\d+)\s+LIMIT\s+(\d+)',
        r'LIMIT \2 OFFSET \1',
        sql,
        flags=re.IGNORECASE,
    )

    # 8. Convert FETCH FIRST N ROWS ONLY without preceding ORDER BY to LIMIT N
    sql = re.sub(
        r'\bFETCH\s+FIRST\s+(\d+)\s+ROWS\s+ONLY\b',
        r'LIMIT \1',
        sql,
        flags=re.IGNORECASE,
    )

    # 9. Rewrite EXTRACT(DOW FROM expr) -> toDayOfWeek(expr)
    #    and EXTRACT(ISODOW FROM expr) -> toDayOfWeek(expr)
    pat_dow = re.compile(r'\bEXTRACT\s*\(\s*(?:ISO)?DOW\s+FROM\s+', re.IGNORECASE)
    while True:
        m = pat_dow.search(sql)
        if not m:
            break
        paren_start = sql.index('(', m.start())
        paren_end = find_balanced_parens(sql, paren_start)
        if paren_end == -1:
            break
        from_pos = m.end()
        expr = sql[from_pos:paren_end]
        sql = sql[:m.start()] + f'toDayOfWeek({expr})' + sql[paren_end+1:]

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
