#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$dir"
printf '${c1:JSON} ${c2:Quoted}' > "$dir/json_quoted.format"
printf '${c1:Quoted} ${c2:JSON}' > "$dir/quoted_json.format"

echo "CustomSeparated"
$CLICKHOUSE_LOCAL -m -q "
    SET format_custom_escaping_rule = 'JSON';
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, '{\"k\":1}\n');
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, '{\"k\":1}\n{\"j\":\"a\"}\n');
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, '[{\"k\":1},{\"k\":2,\"j\":[1]}]\n');
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, '[1,\"x\"]\n');
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, 'null\n');
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, '{}\n');
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, '{\"k\":1}\n') SETTINGS input_format_json_try_infer_named_tuples_from_objects = 0;
    SELECT c1, toTypeName(c1) FROM format(CustomSeparated, '(1,\'x\')\n') SETTINGS format_custom_escaping_rule = 'Quoted';"

echo "Regexp"
$CLICKHOUSE_LOCAL -m -q "
    SET format_regexp = '(.*)', format_regexp_escaping_rule = 'JSON';
    SELECT c1, toTypeName(c1) FROM format(Regexp, '{\"k\":1}\n');
    SELECT c1, toTypeName(c1) FROM format(Regexp, '{\"k\":1}\n{\"j\":\"a\"}\n');
    SELECT c1, toTypeName(c1) FROM format(Regexp, '[1,\"x\"]\n');
    SELECT c1, toTypeName(c1) FROM format(Regexp, 'null\n');
    SELECT c1, toTypeName(c1) FROM format(Regexp, '(1,\'x\')\n') SETTINGS format_regexp_escaping_rule = 'Quoted';"

echo "Template"
$CLICKHOUSE_LOCAL -m -q "
    SELECT c1, c2, toTypeName(c1), toTypeName(c2) FROM format(Template, '{\"k\":1} (1,\'x\')') SETTINGS format_template_row = '$dir/json_quoted.format';
    SELECT c1, c2, toTypeName(c1), toTypeName(c2) FROM format(Template, '[1,\"x\"] (1,\'x\')') SETTINGS format_template_row = '$dir/json_quoted.format';
    SELECT c1, c2, toTypeName(c1), toTypeName(c2) FROM format(Template, '(1,\'x\') {\"k\":1}') SETTINGS format_template_row = '$dir/quoted_json.format';
    SELECT c1, c2, toTypeName(c1), toTypeName(c2) FROM format(Template, '(1,\'x\') [1,\"x\"]') SETTINGS format_template_row = '$dir/quoted_json.format';"

rm -rf "$dir"
