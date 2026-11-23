#!/usr/bin/env bash
# Tags: use_maxminddb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

mkdir -p "$CLICKHOUSE_USER_FILES_UNIQUE"
python3 "$CUR_DIR/helpers/maxminddb.py" "$CLICKHOUSE_USER_FILES_UNIQUE"
relative="${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
error_file="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

query()
{
    $CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1 --print_pretty_type_names 0 -q "$1"
}

user_query()
{
    $CLICKHOUSE_CLIENT --user "$user" --allow_experimental_maxminddb_table_engine 1 --send_logs_level fatal -q "$1"
}

cleanup()
{
    query "DROP TABLE IF EXISTS projections SYNC; DROP TABLE IF EXISTS inferred SYNC; DROP TABLE IF EXISTS declared SYNC; DROP TABLE IF EXISTS attached SYNC; DROP USER IF EXISTS $user" >/dev/null
    rm -rf "$CLICKHOUSE_USER_FILES_UNIQUE" "$error_file"
}
trap cleanup EXIT

# Reading a subcolumn must return the same value as reading the whole column and taking the subcolumn of it.
echo "Dotted map keys"
query "CREATE TABLE projections ENGINE = MaxMindDB('$relative/projections.mmdb') SETTINGS refresh_interval = '0'"
query "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'projections' ORDER BY position FORMAT TSV"
# `x.a.b` names both the element `a.b` of `x` and the element `b` of the element `a` of `x`.
query "SELECT x FROM projections WHERE ip = toIPv4('8.8.8.8')"
query "SELECT x.a.b FROM projections WHERE ip = toIPv4('8.8.8.8')"
query "SELECT x.a FROM projections WHERE ip = toIPv4('8.8.8.8')"
query "SELECT x.c.d.e, x.c.d FROM projections WHERE ip = toIPv4('8.8.8.8')"
query "SELECT x.a.b, x.a, x.c.d.e, x FROM projections WHERE ip = toIPv4('8.8.8.8')"

# `o` is absent in one of the records, while the nested `t` and `s` are required whenever `o` is present.
echo "Optional parents"
query "SELECT ip, o FROM projections WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"
query "SELECT ip, o.t FROM projections WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"
query "SELECT ip, o.t.s FROM projections WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"
query "SELECT ip, o.u, o.null FROM projections WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"
# The optional parent is nested, so the path to it is read directly and the rest of the subcolumn is extracted from it.
query "SELECT ip, w FROM projections WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"
query "SELECT ip, w.o, w.o.t, w.o.t.s FROM projections WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"
query "SELECT ip, w.o.t.s FROM projections WHERE ip IN (toIPv4('1.1.1.1'), toIPv4('8.8.8.8')) ORDER BY ip"

# A fresh table definition reads a file of the server, so it requires the `READ ON FILE` grant.
echo "Grants"
query "DROP USER IF EXISTS $user; CREATE USER $user; GRANT CREATE TABLE, DROP TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user; GRANT TABLE ENGINE ON MaxMindDB TO $user"

expect_denied()
{
    if user_query "$1" >/dev/null 2>"$error_file"
    then
        echo "UNEXPECTED SUCCESS: $1"
    elif grep -q "ACCESS_DENIED" "$error_file" && grep -q "READ ON FILE" "$error_file"
    then
        echo "ACCESS_DENIED"
    else
        cat "$error_file"
    fi
}

inferred="CREATE TABLE inferred ENGINE = MaxMindDB('$relative/v4_A.mmdb') SETTINGS refresh_interval = '0'"
declared="CREATE TABLE declared (ip IPv4, version String) ENGINE = MaxMindDB('$relative/v4_A.mmdb') SETTINGS refresh_interval = '0'"
# A full definition in an `ATTACH` query is a fresh definition as well. An Atomic database requires its UUID.
uuid=$(query "SELECT generateUUIDv4()")
attached="ATTACH TABLE attached UUID '$uuid' (ip IPv4, version String) ENGINE = MaxMindDB('$relative/v4_A.mmdb') SETTINGS refresh_interval = '0'"

expect_denied "$inferred"
expect_denied "$declared"
expect_denied "$attached"
query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('inferred', 'declared', 'attached')"

query "GRANT READ ON FILE TO $user"
user_query "$inferred"
user_query "$declared"
user_query "$attached"
user_query "SELECT 'inferred', version FROM inferred WHERE ip = toIPv4('8.8.8.8')"
user_query "SELECT 'declared', version FROM declared WHERE ip = toIPv4('8.8.8.8')"
user_query "SELECT 'attached', version FROM attached WHERE ip = toIPv4('8.8.8.8')"

# An existing table is loaded from its stored definition, and that does not depend on the grants of the user.
query "REVOKE READ ON FILE FROM $user"
user_query "SELECT 'after revoke', version FROM declared WHERE ip = toIPv4('8.8.8.8')"
query "DETACH TABLE declared"
query "ATTACH TABLE declared"
user_query "SELECT 'after reattach', version FROM declared WHERE ip = toIPv4('8.8.8.8')"
