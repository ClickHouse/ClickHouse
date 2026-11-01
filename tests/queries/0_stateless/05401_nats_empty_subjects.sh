#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Empty, blank and empty-element subject lists must be rejected at CREATE time.
# No broker is needed: the check runs before any connection attempt.
for subjects in "''" "' '" "','" "'a,,b'"
do
    $CLICKHOUSE_LOCAL --query "
        CREATE TABLE t (s String) ENGINE = NATS
        SETTINGS nats_url = '127.0.0.1:1', nats_subjects = $subjects, nats_format = 'TSV'
    " 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
done

# Full-definition ATTACH followed by INSERT used to abort the server (see #125890).
for subjects in "''" "' '"
do
    $CLICKHOUSE_LOCAL --query "
        ATTACH TABLE t UUID '00000000-0000-0000-0000-000000000001' (s String) ENGINE = NATS
        SETTINGS nats_url = '127.0.0.1:1', nats_subjects = $subjects, nats_format = 'TSV';
        INSERT INTO t VALUES ('x');
    " 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
done

