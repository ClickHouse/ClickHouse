#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the S3, MySQL and PostgreSQL integrations are not compiled into the fast-test build.

# A `key = value` argument whose key the parser of the engine rejects (for example a misspelled
# secret key: `secret_acces_key`, `passwd`, `pass`) is rejected only after the query is formatted
# for logging. The whole argument must be hidden: the value can be a secret under a misspelled key,
# and the key itself can be the secret. The values of accepted non-secret keys stay visible.
# In the named-collection form of a table or database engine, every argument is hidden then: each
# argument of a definition has to be readable enough to tell that it carries no credential.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SECRET="SECRET_THAT_MUST_NOT_LEAK"

format() {
    echo "--- $1"
    local formatted
    formatted=$(echo "$2" | $CLICKHOUSE_FORMAT --oneline)
    echo "$formatted"
    if echo "$formatted" | grep -q "$SECRET"; then
        echo "FAIL: secret leaked in formatted query"
    fi
}

# Each query has an accepted non-secret key, an accepted secret key and a misspelled secret key.
format "s3 table function" \
    "SELECT * FROM s3(nc, filename = 'data.csv', secret_access_key = '${SECRET}', secret_acces_key = '${SECRET}')"
format "s3Cluster table function" \
    "SELECT * FROM s3Cluster('cluster', nc, format = 'CSV', session_token = '${SECRET}', sesion_token = '${SECRET}')"
format "S3 table engine" \
    "CREATE TABLE t (x Int32) ENGINE = S3(nc, format = 'CSV', secret_access_key = '${SECRET}', secret_acces_key = '${SECRET}')"
format "icebergS3 table function" \
    "SELECT * FROM icebergS3(nc, url = 'http://127.0.0.1:1/x/', secret_access_key = '${SECRET}', secret_acces_key = '${SECRET}')"
format "mysql table function" \
    "SELECT * FROM mysql(nc, table = 't', password = '${SECRET}', passwd = '${SECRET}')"
format "MySQL table engine" \
    "CREATE TABLE t (x Int32) ENGINE = MySQL(nc, connection_pool_size = 4, password = '${SECRET}', passwd = '${SECRET}')"
format "MySQL database engine" \
    "CREATE DATABASE d ENGINE = MySQL(nc, database = 'db', password = '${SECRET}', passwd = '${SECRET}')"
format "postgresql table function" \
    "SELECT * FROM postgresql(nc, schema = 's', password = '${SECRET}', pass = '${SECRET}')"
format "PostgreSQL table engine" \
    "CREATE TABLE t (x Int32) ENGINE = PostgreSQL(nc, postgresql_connection_pool_size = 4, password = '${SECRET}', pass = '${SECRET}')"
format "PostgreSQL database engine" \
    "CREATE DATABASE d ENGINE = PostgreSQL(nc, use_table_cache = 1, password = '${SECRET}', pass = '${SECRET}')"
format "MaterializedPostgreSQL database engine" \
    "CREATE DATABASE d ENGINE = MaterializedPostgreSQL(nc, schema = 's', password = '${SECRET}', pass = '${SECRET}')"
format "Remote database engine" \
    "CREATE DATABASE d ENGINE = Remote(nc, port = 9000, password = '${SECRET}', passwd = '${SECRET}')"

# The secret typed into the key.
format "secret as the key" \
    "SELECT * FROM s3(nc, format = 'CSV', '${SECRET}' = 'x')"
format "secret as the key, value that is an expression" \
    "SELECT * FROM s3(nc, format = 'CSV', '${SECRET}' = concat('x', 'y'))"
format "secret appended to the key" \
    "SELECT * FROM mysql(nc, table = 't', password_${SECRET} = 1)"
format "key that is an expression" \
    "SELECT * FROM postgresql(nc, schema = 's', concat('pass', 'word') = '${SECRET}')"

# The paths of the TLS credentials are accepted from a collection, but not as an override in a query.
format "mysql TLS path key" \
    "SELECT * FROM mysql(nc, table = 't', ssl_key = '${SECRET}')"
format "postgresql TLS path key" \
    "SELECT * FROM postgresql(nc, table = 't', sslkey = '${SECRET}')"

# `extra_credentials(...)` is hidden whole, its keys included: a key can be a secret too.
format "extra_credentials with the secret as the key" \
    "CREATE TABLE t (x Int32) ENGINE = S3(nc, format = 'CSV', extra_credentials('${SECRET}' = 'x'))"
format "extra_credentials with a computed key" \
    "SELECT * FROM s3(nc, extra_credentials(concat('${SECRET}', 'x') = 'x'), format = 'CSV')"
format "extra_credentials with valid keys" \
    "SELECT * FROM s3('http://127.0.0.1:1/x', 'CSV', extra_credentials(role_arn = 'arn', external_id = '${SECRET}'))"

echo "--- extra_credentials in the query tree"
dump=$($CLICKHOUSE_CLIENT --query "EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3(nc, extra_credentials('${SECRET}' = 'x'), format = 'CSV')")
if echo "$dump" | grep -q "$SECRET"; then
    echo "FAIL: secret leaked in the query tree"
else
    echo "OK"
fi

# The S3 database engine accepts fewer keys than the S3 table engine.
format "S3 database engine" \
    "CREATE DATABASE d ENGINE = S3(nc, url = 'http://127.0.0.1:1/x/', secret_access_key = '${SECRET}')"
format "S3 database engine, misspelled key" \
    "CREATE DATABASE d ENGINE = S3(nc, secret_acces_key = '${SECRET}')"
format "S3 database engine, explicit url" \
    "CREATE DATABASE d ENGINE = S3('http://127.0.0.1:1/x/', 'ak', '${SECRET}', use_environment_credentials = 1, secret_acces_key = '${SECRET}')"

# The explicit-url S3 form ignores a key it does not read: an unknown key hides the whole argument, a valid
# S3 key that this form does not read hides its value.
format "s3 explicit url, unknown and unread keys" \
    "SELECT * FROM s3('http://127.0.0.1:1/x', 'CSV', secret_acces_key = '${SECRET}', filename = '${SECRET}', structure = 'x Int32')"
# The parser strips only the first extra_credentials(...): with two, every argument is hidden.
format "s3 explicit url, two extra_credentials" \
    "SELECT * FROM s3('http://127.0.0.1:1/x', extra_credentials(role_arn = 'a'), extra_credentials(role_arn = 'b'), 'ak', '${SECRET}')"
# The value of an accepted key that is not a plain literal can nest a secret.
format "mysql value that is an expression" \
    "SELECT * FROM mysql(nc, table = headers('Authorization' = '${SECRET}'))"

# A key is compared as the parser compares it: case-sensitively, with the aliases of `user`, `db` and `host`.
format "aliases and case" \
    "SELECT * FROM mysql(nc, username = 'u', db = 'db', hostname = 'h', PASSWORD = '${SECRET}')"

# Outside the named-collection form, the explicit arguments of the S3 family keep their current rules.
format "s3 table function, explicit url" \
    "SELECT * FROM s3('http://127.0.0.1:1/x', 'CSV', secret_access_key = '${SECRET}')"

# The rejected queries are logged with the argument hidden, and the exception does not name the key.
DB="$CLICKHOUSE_DATABASE"
NC_S3="nc_s3_${DB}"
NC_MYSQL="nc_mysql_${DB}"
NC_PG="nc_pg_${DB}"

$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${NC_S3} AS url = 'http://127.0.0.1:1/bucket/', access_key_id = 'id', secret_access_key = 'key';
    CREATE NAMED COLLECTION ${NC_MYSQL} AS host = '127.0.0.1', port = 1, user = 'u', password = 'p', database = 'db', table = 't';
    CREATE NAMED COLLECTION ${NC_PG} AS host = '127.0.0.1', port = 1, user = 'u', password = 'p', database = 'db', table = 't';
"

$CLICKHOUSE_CLIENT -m -q "
    SELECT * FROM s3(${NC_S3}, filename = 'data.csv', format = 'CSV', secret_acces_key = '${SECRET}'); -- { serverError BAD_ARGUMENTS }
    CREATE TABLE t_s3 (x Int32) ENGINE = S3(${NC_S3}, filename = 'data.csv', format = 'CSV', secret_acces_key = '${SECRET}'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM mysql(${NC_MYSQL}, passwd = '${SECRET}'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM postgresql(${NC_PG}, pass = '${SECRET}'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM s3(${NC_S3}, filename = 'data.csv', format = 'CSV', secret_access_key_${SECRET} = 'x'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM mysql(${NC_MYSQL}, password_${SECRET} = 'x'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM postgresql(${NC_PG}, password_${SECRET} = 'x'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM mysql(${NC_MYSQL}, ssl_key = '${SECRET}'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM postgresql(${NC_PG}, sslkey = '${SECRET}'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM mysql(${NC_MYSQL}, \`${SECRET}[\` = 'x'); -- { serverError BAD_ARGUMENTS }
    SELECT * FROM s3('http://127.0.0.1:1/bucket/data.csv', 'CSV', 'x Int32', extra_credentials(role_arn_${SECRET} = 'x')); -- { serverError BAD_ARGUMENTS }
    CREATE DATABASE d_s3 ENGINE = S3(${NC_S3}, secret_acces_key = '${SECRET}'); -- { serverError BAD_ARGUMENTS }
    CREATE DATABASE d_s3 ENGINE = S3(${NC_S3}, password_${SECRET} = 'x'); -- { serverError BAD_ARGUMENTS }
"

$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS query_log;
    -- Neither the query nor the exception message keeps the secret.
    SELECT replaceAll(query, '_${DB}', ''), position(query, '${SECRET}') = 0, position(exception, '${SECRET}') = 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionBeforeStart'
    ORDER BY event_time_microseconds;
"

$CLICKHOUSE_CLIENT -m -q "
    DROP NAMED COLLECTION ${NC_S3};
    DROP NAMED COLLECTION ${NC_MYSQL};
    DROP NAMED COLLECTION ${NC_PG};
"
