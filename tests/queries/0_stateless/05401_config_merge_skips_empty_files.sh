#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "$CUR_DIR/tmp"
test_dir=$(mktemp -d "$CUR_DIR/tmp/05401_XXXXXX")
trap 'rm -r "$test_dir"' EXIT

mkdir "$test_dir/config.d"

tee "$test_dir/config.xml" >/dev/null <<'EOF'
<clickhouse>
    <key>main</key>
</clickhouse>
EOF

tee "$test_dir/config.d/override.xml" >/dev/null <<'EOF'
<clickhouse>
    <key replace="1">override</key>
</clickhouse>
EOF

# Empty and whitespace-only files have nothing to merge and are skipped.
: > "$test_dir/config.d/empty.xml"
printf '\n  \t\n' > "$test_dir/config.d/blank.xml"

"$CLICKHOUSE_BINARY" extract-from-config --config-file "$test_dir/config.xml" --key key

# A malformed file is still an error.
echo '<clickhouse>' > "$test_dir/config.d/malformed.xml"
"$CLICKHOUSE_BINARY" extract-from-config --config-file "$test_dir/config.xml" --key key 2>&1 | grep -c "Failed to merge config with '.*/malformed.xml'"

# A read error is still an error: reading `/proc/self/mem` from offset 0 fails with EIO.
rm "$test_dir/config.d/malformed.xml"
ln -s /proc/self/mem "$test_dir/config.d/unreadable.xml"
"$CLICKHOUSE_BINARY" extract-from-config --config-file "$test_dir/config.xml" --key key 2>&1 | grep -c "Cannot read from file .*/unreadable.xml"
