#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# The OIDs of the built-in types are fixed by PostgreSQL (`pg_type.dat`), and a PostgreSQL client may
# resolve a type by its OID alone. The range types are 3904, 3906, 3908, 3910, 3912 and 3926, the OID
# after each of them is the array of that range type, and the multirange types are 4451 and 4532 - 4536.
# `format_type` and the emulated `pg_type` and `pg_range` had the range types of timestamps and dates
# shifted and the multirange types on the OIDs of the arrays. `format_type` also printed `character`
# for the internal `"char"` type and for the blank-padded character type without a length.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05872_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-align --tuples-only --quiet -c "$1"
}

echo "--- format_type: range types, their arrays and multirange types; 3914 is not a type"
${CLICKHOUSE_CLIENT} -q "
    SELECT oid, format_type(oid, -1)
    FROM (SELECT arrayJoin([3904, 3905, 3906, 3907, 3908, 3909, 3910, 3911, 3912, 3913, 3914, 3926, 3927,
                            4451, 4532, 4533, 4534, 4535, 4536]) AS oid)
    ORDER BY oid"

echo "--- format_type: the internal one-byte type and the blank-padded character type"
${CLICKHOUSE_CLIENT} -q "
    SELECT format_type(18, -1), format_type(1042, NULL), format_type(1042, -1), format_type(1042, 4),
           format_type(1042, 5), format_type(1042, 14)"

echo "--- pg_type: range and multirange types"
run_psql "SELECT oid, typname, typtype FROM pg_type WHERE typtype IN ('r', 'm') ORDER BY oid"

echo "--- pg_range"
run_psql "SELECT rngtypid, rngsubtype, rngmultitypid FROM pg_range ORDER BY rngtypid"

echo "--- format_type gives the name that pg_type has for each of them"
run_psql "SELECT count() FROM pg_type WHERE typtype IN ('r', 'm') AND format_type(oid, -1) != typname"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
