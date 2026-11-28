#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"

echo '<clickhouse><users_config>users.xml</users_config></clickhouse>' > "${DIR}/config.xml"

# Loads the users read from stdin next to the `default` user and lists their authentication types.
function load_users()
{
    {
        echo '<clickhouse><profiles><default/></profiles><quotas><default/></quotas><users>'
        echo '<default><password></password><profile>default</profile><quota>default</quota></default>'
        cat
        echo '</users></clickhouse>'
    } > "${DIR}/users.xml"
    ${CLICKHOUSE_LOCAL} --config-file "${DIR}/config.xml" \
        --query "SELECT name, auth_type FROM system.users WHERE name != 'default' ORDER BY name" 2>&1
}

echo "-- flat element and an auth_methods entry"
load_users <<'EOF'
<u_flat>
    <password_bcrypt_hash>$2a$04$5nQvqncp7WGRfBG.nGaU/O59eNFFYuWXpp.sjFAhbwYgxSywzWa3i</password_bcrypt_hash>
</u_flat>
<u_methods>
    <auth_methods>
        <a1><password_bcrypt_hash>$2b$04$lugLXmGfYi1tPY.sM82CE.Ho62rEICCYfn3H78KpyCPgIRQmuYxrS</password_bcrypt_hash></a1>
        <a2><password>plain_pass_2</password></a2>
    </auth_methods>
</u_methods>
EOF

echo "-- malformed hash"
load_users <<'EOF' | grep -o -m1 'must be 59 or 60 bytes'
<u_bad><password_bcrypt_hash>not_a_bcrypt_hash</password_bcrypt_hash></u_bad>
EOF

echo "-- 64-byte hash without zero padding"
load_users <<'EOF' | grep -o -m1 'has length 64 but must be 59 or 60 bytes'
<u_long><password_bcrypt_hash>$2a$04$5nQvqncp7WGRfBG.nGaU/O59eNFFYuWXpp.sjFAhbwYgxSywzWa3iabcd</password_bcrypt_hash></u_long>
EOF

echo "-- together with another flat method"
load_users <<'EOF' | grep -o -m1 'Cannot specify multiple authentication methods'
<u_both>
    <password_bcrypt_hash>$2a$04$5nQvqncp7WGRfBG.nGaU/O59eNFFYuWXpp.sjFAhbwYgxSywzWa3i</password_bcrypt_hash>
    <password>plain_pass</password>
</u_both>
EOF

echo "-- flat element together with auth_methods"
load_users <<'EOF' | grep -o -m1 "Cannot specify both 'auth_methods' and 'password_bcrypt_hash'"
<u_mixed>
    <password_bcrypt_hash>$2a$04$5nQvqncp7WGRfBG.nGaU/O59eNFFYuWXpp.sjFAhbwYgxSywzWa3i</password_bcrypt_hash>
    <auth_methods><a1><password>plain_pass</password></a1></auth_methods>
</u_mixed>
EOF

rm -r "${DIR}"
