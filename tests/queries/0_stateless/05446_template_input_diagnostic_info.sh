#!/usr/bin/env bash
# Verbose diagnostics of the Template input format (TemplateRowInputFormat::parseRowAndPrintDiagnosticInfo):
# when a row does not parse, the format first reports that the resultset suffix does not match and then
# re-parses the previous and the current row delimiter by delimiter and field by field.
# Expected behaviour: docs/reference/formats/Template/Template.mdx and
# docs/reference/settings/formats/format-template.mdx (format_template_row, format_template_resultset,
# format_template_rows_between_delimiter). Only the error code name is asserted, never the diagnostic text.

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

# The last line of a ClickHouse error ends with the error code name in parentheses.
error_code() { grep -oE '\([A-Z_]+\)$' | tail -n 1 | tr -d '()'; }

# 1. Well-formed stream: two rows, no delimiter between them, then the resultset suffix "END".
printf '1,Alice\n2,Bob\nEND' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" --format_template_resultset "$RESULTSET" \
    --format_template_rows_between_delimiter '' -q 'SELECT count(), max(name) FROM table'

# 2. Row 2 has a non-numeric id. The suffix "END" does not match where row 2 starts, so the
#    diagnostics re-parse row 1 completely and then report field 0 of row 2.
printf '1,Alice\nx,Bob\nEND' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" --format_template_resultset "$RESULTSET" \
    --format_template_rows_between_delimiter '' -q 'SELECT count() FROM table' 2>&1 | error_code

# 3. Default resultset format: its suffix is empty, so the suffix check succeeds and the diagnostics
#    report the data found after it. The default row delimiter "\n" is then missing in front of
#    row 1, which stops the diagnostics before the fields.
printf '1,Alice\n\nx,Bob\n' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" -q 'SELECT count() FROM table' 2>&1 | error_code

# 4. Row 2 separates its fields with ';', so the delimiter in front of field 1 is reported.
printf '1,Alice\n1;Bob\n' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW" --format_template_rows_between_delimiter '' \
    -q 'SELECT count() FROM table' 2>&1 | error_code

# 5. The third field is skipped with the JSON escaping rule. Row 1 holds a valid JSON value there and
#    row 2 does not, so the diagnostics report the skipped column of row 2.
printf '1,Alice,123\n2,Bob,x\nEND' | $CLICKHOUSE_LOCAL "${OPTS[@]}" \
    --format_template_row "$ROW_SKIP" --format_template_resultset "$RESULTSET" \
    --format_template_rows_between_delimiter '' -q 'SELECT count() FROM table' 2>&1 | error_code
