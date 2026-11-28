#!/usr/bin/env bash
# Tags: no-replicated-database

# A `Buffer` table must apply the row policies of its destination, to the rows read from the destination
# and to the rows still in its buffers. Otherwise a user who may create a `Buffer` reads the rows a policy
# on the destination hides from them.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user05872_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;

CREATE TABLE ${db}.orders (tenant String, id UInt32, payload String) ENGINE = MergeTree ORDER BY tenant;
INSERT INTO ${db}.orders VALUES ('alice', 1, 'alice order'), ('bob', 2, 'bob order'), ('alice', 300, 'alice big order');

CREATE TABLE ${db}.ids (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.ids VALUES (2), (300);

GRANT SELECT ON ${db}.orders TO ${user};
GRANT SELECT ON ${db}.ids TO ${user};
GRANT CREATE TABLE, SELECT ON ${db}.orders_buffer TO ${user};
GRANT CREATE TABLE, SELECT ON ${db}.orders_buffer_narrow TO ${user};
GRANT CREATE TABLE, SELECT ON ${db}.orders_buffer_no_tenant TO ${user};
GRANT CREATE TABLE, SELECT ON ${db}.orders_buffer_in TO ${user};

CREATE ROW POLICY rp05872 ON ${db}.orders FOR SELECT USING tenant = 'alice' AND id < 100 TO ${user};
EOF

run_rows() { ${CLICKHOUSE_CLIENT} --user "${user}" --query "$1"; }
run_error() { ${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'; }

# The user creates the `Buffer` themselves, so no policy can be defined on it in advance.
run_rows "CREATE TABLE ${db}.orders_buffer ENGINE = Buffer(${db}, orders, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000)"
# The buffered rows are not in the destination yet, but the policy hides them as well.
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${db}.orders_buffer VALUES ('bob', 3, 'bob buffered'), ('alice', 4, 'alice buffered'), ('alice', 500, 'alice big buffered')"

echo "--- direct"
run_rows "SELECT * FROM ${db}.orders ORDER BY id"

echo "--- buffer"
run_rows "SELECT * FROM ${db}.orders_buffer ORDER BY id"

echo "--- buffer, the policy columns are not requested"
run_rows "SELECT payload FROM ${db}.orders_buffer ORDER BY payload"

echo "--- buffer, count"
run_rows "SELECT count() FROM ${db}.orders_buffer"

echo "--- buffer, aggregation"
run_rows "SELECT tenant, count(), sum(id) FROM ${db}.orders_buffer GROUP BY tenant ORDER BY tenant"

echo "--- buffer, PREWHERE does not see the hidden rows"
run_rows "SELECT id FROM ${db}.orders_buffer PREWHERE throwIf(tenant = 'bob' OR id >= 100) = 0 ORDER BY id"

echo "--- buffer, WHERE does not see the hidden rows"
run_rows "SELECT id FROM ${db}.orders_buffer WHERE throwIf(tenant = 'bob' OR id >= 100) = 0 ORDER BY id"

# The policy is evaluated on the destination's types: as a `UInt8`, 300 would pass `id < 100`.
echo "--- buffer with a narrower type of the policy column"
run_rows "CREATE TABLE ${db}.orders_buffer_narrow (tenant String, id UInt8, payload String) ENGINE = Buffer(${db}, orders, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000)"
run_rows "SELECT * FROM ${db}.orders_buffer_narrow ORDER BY id"

# The policy cannot be evaluated on the buffered rows, so the read is refused rather than unfiltered.
echo "--- buffer without the policy column"
run_rows "CREATE TABLE ${db}.orders_buffer_no_tenant (id UInt32, payload String) ENGINE = Buffer(${db}, orders, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000)"
run_error "SELECT * FROM ${db}.orders_buffer_no_tenant ORDER BY id"

echo "--- policy with a subquery"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY rp05872_in ON ${db}.orders AS RESTRICTIVE FOR SELECT USING id NOT IN (SELECT id FROM ${db}.ids) TO ${user}"
run_rows "CREATE TABLE ${db}.orders_buffer_in ENGINE = Buffer(${db}, orders, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${db}.orders_buffer_in VALUES ('alice', 2, 'alice buffered in ids'), ('alice', 5, 'alice buffered')"
run_rows "SELECT * FROM ${db}.orders_buffer_in ORDER BY id"

# A user the policy does not apply to sees everything.
echo "--- buffer, as a user without the policy"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${db}.orders_buffer ORDER BY id"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP ROW POLICY rp05872 ON ${db}.orders;
DROP ROW POLICY rp05872_in ON ${db}.orders;
DROP USER ${user};
EOF
