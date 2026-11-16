#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client must not send `session_timezone`, so that the server takes it from the user profile.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_OPT}" | sed -E 's/--session_timezone [^ ]+//g')
user="u_${CLICKHOUSE_DATABASE}"
# A profile time zone with another offset than the server time zone, the one the client parses in by default.
tz=$(${CLICKHOUSE_CLIENT} -q "SELECT if(toDateTime('2020-01-01 00:00:00', serverTimezone()) = toDateTime('2020-01-01 00:00:00', 'Asia/Tokyo'), 'America/New_York', 'Asia/Tokyo')")
${CLICKHOUSE_CLIENT} -q "
    CREATE USER ${user} IDENTIFIED WITH no_password SETTINGS session_timezone = '${tz}', apply_settings_from_server = 0;
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
    CREATE TABLE t_base (d DateTime) ENGINE = Memory;
    CREATE TABLE t (d DateTime) ENGINE = Memory;"
# shellcheck disable=SC2206
client=(${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --user "${user}" --use_client_time_zone 0 --async_insert 0
    --max_insert_block_size 10 --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0
    --input_format_values_interpret_expressions 1 --input_format_values_deduce_templates_of_expressions 0)
rows=$(seq 1 500 | sed "s/.*/(toDateTime('2020-01-01 00:00:00')),('2020-01-01 00:00:00')/" | paste -sd,)

echo "$rows" | "${client[@]}" -q "INSERT INTO t_base FORMAT Values"
echo "$rows" | "${client[@]}" -q "INSERT INTO t SELECT * FROM input('d DateTime') FORMAT Values"
echo "$rows" | "${client[@]}" -q "INSERT INTO t RETURNING (SELECT count() FROM t) FORMAT Values" > /dev/null

# The client's own time zone is not the server's one for this user (otherwise the test proves nothing).
"${client[@]}" -q "SELECT (SELECT any(d) FROM t_base) != toDateTime('2020-01-01 00:00:00')"
# Every row of both INSERTs is parsed in the server's `session_timezone`.
"${client[@]}" -q "SELECT count(), uniqExact(d), any(d) = toDateTime('2020-01-01 00:00:00') FROM t"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t; DROP TABLE t_base; DROP USER ${user}"
