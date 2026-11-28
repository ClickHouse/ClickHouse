#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `arr` and `tup` are legacy two-level lists of a one-field struct: their repeated group is named `array` or
# `<list name>_tuple`, so it is the element, not the middle level. `oth` is a three-level list (`x_tuple`).
DATA_FILE="${CUR_DIR}/data_parquet/05872_parquet_two_level_list_single_field.parquet"

${CLICKHOUSE_LOCAL} --query "
    DESCRIBE file('${DATA_FILE}') SETTINGS print_pretty_type_names = 0;
    SELECT * FROM file('${DATA_FILE}') ORDER BY id;
    SELECT id, arr, tup FROM file('${DATA_FILE}', Parquet, 'id Int32, arr Array(Tuple(a Nullable(Int32))), tup Array(Tuple(a Nullable(Int32)))') ORDER BY id;
    SELECT id, \`arr.a\`, \`tup.a\` FROM file('${DATA_FILE}', Parquet, 'id Int32, \`arr.a\` Array(Nullable(Int32)), \`tup.a\` Array(Nullable(Int32))') ORDER BY id;"
