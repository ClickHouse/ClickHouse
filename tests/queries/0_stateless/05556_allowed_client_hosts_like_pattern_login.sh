#!/usr/bin/env bash
# A login as another user is needed, so this is a shell test.
#
# Behaviour source: the HOST clause of CREATE USER, parsed in
# src/Parsers/Access/ParserCreateUserQuery.cpp, and AllowedClientHosts in
# src/Access/Common/AllowedClientHosts.h. A `HOST LIKE` pattern is stored verbatim and is
# only interpreted when a client logs in: it becomes an IP subnet, a host name or a host
# name regular expression, and the client address is then checked against it. A subnet whose
# mask is all ones falls back to the exact address check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user_ip="${CLICKHOUSE_TEST_UNIQUE_NAME}_ip"
user_subnet="${CLICKHOUSE_TEST_UNIQUE_NAME}_subnet"
user_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_name"
user_regexp="${CLICKHOUSE_TEST_UNIQUE_NAME}_regexp"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user_ip, $user_subnet, $user_name, $user_regexp"
}
trap cleanup EXIT
cleanup

# The address the server sees this client at, as an IPv6 subnet with an all-ones mask. The
# textual form of an IPv6 value is always an IPv6 literal, so the pattern is a valid subnet
# for an IPv4-mapped address too. Deriving the rule from the observed address keeps it an
# exact-address rule for whatever machine the client runs on, local or remote.
client_subnet=$(${CLICKHOUSE_CLIENT} --query "SELECT concat(toString(address), '/128') FROM system.processes WHERE query_id = queryID()")

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE USER $user_ip     HOST LIKE '$client_subnet';
    CREATE USER $user_subnet HOST LIKE '192.0.2.%';
    CREATE USER $user_regexp HOST LIKE 'nosuch%.invalid';
    CREATE USER $user_name   HOST LIKE 'nosuchhost.invalid';
"

# An all-ones mask makes the pattern an exact address, and that address is the one the server
# just reported for this client, so this login is allowed.
echo 'exact client address: allowed'
${CLICKHOUSE_CLIENT} --user "$user_ip" --query "SELECT 1"

# A wildcard IPv4 subnet in the TEST-NET-1 range of RFC 5737. That range is reserved for
# documentation and is never assigned to a host, so it cannot contain the client address.
echo 'other subnet: denied'
${CLICKHOUSE_CLIENT} --user "$user_subnet" --query "SELECT 1" 2>&1 | grep -o -m1 'AUTHENTICATION_FAILED'

# A pattern with a wildcard and no leading digits is a host name regular expression. It is
# matched against the names the client address resolves back to, and matches none of them.
echo 'other host name regexp: denied'
${CLICKHOUSE_CLIENT} --user "$user_regexp" --query "SELECT 1" 2>&1 | grep -o -m1 'AUTHENTICATION_FAILED'

# A pattern without a wildcard is a host name. `.invalid` never resolves (RFC 2606), so the
# name has no address that could match the client.
echo 'unresolvable host name: denied'
${CLICKHOUSE_CLIENT} --user "$user_name" --query "SELECT 1" 2>&1 | grep -o -m1 'AUTHENTICATION_FAILED'

# The restriction is on the user, not on the server: a user without a HOST clause still logs in.
echo 'unrestricted user: allowed'
${CLICKHOUSE_CLIENT} --query "SELECT 2"
