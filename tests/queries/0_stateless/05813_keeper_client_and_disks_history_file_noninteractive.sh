#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Non-interactive runs of clickhouse-keeper-client and clickhouse-disks must neither need nor create
# the history file: only the interactive line reader uses it.

TESTDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_history_file"
rm -rf "$TESTDIR"
trap 'rm -rf "$TESTDIR"' EXIT
mkdir -p "$TESTDIR/home" "$TESTDIR/disk"
# Does not exist, so creating a file in it fails even as root.
MISSING="$TESTDIR/missing"

history_state() { [ -e "$TESTDIR/home/$1" ] && echo present || echo absent; }
path="/test-keeper-history-$CLICKHOUSE_DATABASE"

echo "-- keeper-client, missing home"
HOME="$MISSING" $CLICKHOUSE_KEEPER_CLIENT --help > /dev/null; echo "help: $?"
HOME="$MISSING" $CLICKHOUSE_KEEPER_CLIENT -q "exists /"; echo "query: $?"
echo "exists /" | HOME="$MISSING" $CLICKHOUSE_KEEPER_CLIENT --tests-mode 2> /dev/null | grep -cx 1; echo "tests-mode: ${PIPESTATUS[1]}"
# Without --tests-mode a non-tty stdin goes through the line reader: no history, but the command runs.
echo "create '$path' 'v'" | HOME="$MISSING" $CLICKHOUSE_KEEPER_CLIENT > /dev/null 2>&1; echo "line reader: ${PIPESTATUS[1]}"
$CLICKHOUSE_KEEPER_CLIENT -q "get '$path'"

echo "-- keeper-client, existing home"
HOME="$TESTDIR/home" $CLICKHOUSE_KEEPER_CLIENT --help > /dev/null
HOME="$TESTDIR/home" $CLICKHOUSE_KEEPER_CLIENT -q "exists /" > /dev/null
echo "after --help and --query: $(history_state .keeper-client-history)"
echo "exists /" | HOME="$TESTDIR/home" $CLICKHOUSE_KEEPER_CLIENT > /dev/null 2>&1
echo "after line reader: $(history_state .keeper-client-history)"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null

echo "-- disks"
cat > "$TESTDIR/config.xml" <<EOF
<clickhouse>
    <path>$TESTDIR/disk/</path>
</clickhouse>
EOF
printf "abc" | HOME="$MISSING" clickhouse-disks -C "$TESTDIR/config.xml" --query "write f"; echo "write: $?"
HOME="$MISSING" clickhouse-disks -C "$TESTDIR/config.xml" --query "read f"; echo "read: $?"
HOME="$TESTDIR/home" clickhouse-disks -C "$TESTDIR/config.xml" --query "read f" > /dev/null
echo "after --query: $(history_state .disks-file-history)"
