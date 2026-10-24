#!/usr/bin/env bash
# Verbose diagnostics of the Template input format (TemplateRowInputFormat::parseRowAndPrintDiagnosticInfo):
# when a row does not parse, the format first reports that the resultset suffix does not match and then
# re-parses the previous and the current row delimiter by delimiter and field by field.
# Expected behaviour: docs/reference/formats/Template/Template.mdx and
# docs/reference/settings/formats/format-template.mdx (format_template_row, format_template_resultset,
# format_template_rows_between_delimiter). Asserted per case: the error code name and the set of stable
# diagnostic markers that the format writes for that case.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROW=$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_row.tpl
ROW_SKIP=$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_row_skip.tpl
RESULTSET=$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_resultset.tpl

trap 'rm -f "$ROW" "$ROW_SKIP" "$RESULTSET"' EXIT

# A Template *input* format string is always read from a file (format_template_row_format is output only),
# so the three templates are written out and passed by path. The trailing newline of printf '%s\n' is
# the delimiter after the last field of a row.
# shellcheck disable=SC2016
printf '%s\n' '${id:CSV},${name:CSV}' > "$ROW"
# shellcheck disable=SC2016
printf '%s\n' '${id:CSV},${name:CSV},${:JSON}' > "$ROW_SKIP"
# shellcheck disable=SC2016
printf '%s' '${data}END' > "$RESULTSET"

# input_format_parallel_parsing is pinned because the randomizer flips it and the diagnostics are
# collected by the single-threaded parse path.
OPTS=(--structure "id UInt64, name String" --input-format Template --input_format_parallel_parsing 0)

# Each fragment is written either by parseRowAndPrintDiagnosticInfo itself or by the delimiter / field
# helper it calls. The diagnostics run twice per failure (previous row, then the failing row), so the
# matches are sorted and deduplicated. The quoted texts are fully determined by the pinned input.
DIAG_RE='Suffix does not match'
DIAG_RE=$DIAG_RE'|There is some data after suffix'
DIAG_RE=$DIAG_RE'|Using format string \(from format_schema_rows\)'
DIAG_RE=$DIAG_RE'|Trying to parse next row'
DIAG_RE=$DIAG_RE'|There is no delimiter between rows: expected "[^"]*"'
DIAG_RE=$DIAG_RE'|There is no delimiter before field [0-9]+: expected "[^"]*"'
DIAG_RE=$DIAG_RE'|ERROR: text "[^"]*" is not like UInt64'
DIAG_RE=$DIAG_RE'|Maybe it.s not possible to deserialize field [0-9]+ as [A-Za-z]+'
DIAG_RE=$DIAG_RE'|name: <SKIPPED COLUMN>'

# The last line of a ClickHouse error ends with the error code name in parentheses; after it comes the
# set of diagnostic markers this case must produce.
check() {
    local text
    text=$(cat)
    printf '%s\n' "$text" | grep -oE '\([A-Z_]+\)$' | tail -n 1 | tr -d '()'
    printf '%s\n' "$text" | grep -oE "$DIAG_RE" | sort -u
}

# 1. Well-formed stream: two rows, no delimiter between them, then the resultset suffix "END".
printf '1,Alice\n2,Bob\nEND' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" --format_template_resultset "$RESULTSET" \
    --format_template_rows_between_delimiter '' -q 'SELECT count(), max(name) FROM table'

# 2. Row 2 has a non-numeric id. The suffix "END" does not match where row 2 starts, so the
#    diagnostics re-parse row 1 completely and then report field 0 of row 2.
echo '--- bad field'
printf '1,Alice\nx,Bob\nEND' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" --format_template_resultset "$RESULTSET" \
    --format_template_rows_between_delimiter '' -q 'SELECT count() FROM table' 2>&1 | check

# 3. Default resultset format: its suffix is empty, so the suffix check succeeds and the diagnostics
#    report the data found after it. The default row delimiter "\n" is then missing in front of
#    row 1, which stops the diagnostics before the fields.
echo '--- missing row delimiter'
printf '1,Alice\n\nx,Bob\n' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" -q 'SELECT count() FROM table' 2>&1 | check

# 4. Row 2 separates its fields with ';', so the delimiter in front of field 1 is reported.
echo '--- bad field delimiter'
printf '1,Alice\n1;Bob\n' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" --format_template_rows_between_delimiter '' \
    -q 'SELECT count() FROM table' 2>&1 | check

# 5. The third field is skipped with the JSON escaping rule. Row 1 holds a valid JSON value there and
#    row 2 does not, so the diagnostics report the skipped column of row 2.
echo '--- bad skipped column'
printf '1,Alice,123\n2,Bob,x\nEND' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW_SKIP" --format_template_resultset "$RESULTSET" \
    --format_template_rows_between_delimiter '' -q 'SELECT count() FROM table' 2>&1 | check
