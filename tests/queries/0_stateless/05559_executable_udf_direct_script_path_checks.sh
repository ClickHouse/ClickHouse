#!/usr/bin/env bash
# An executable UDF with execute_direct resolves <command> inside user_scripts_path and
# must refuse a command that escapes that folder, does not exist, or is not executable.
# Expected behaviour: docs/reference/functions/regular-functions/udf.mdx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_udf
rm -rf "$DIR"
# user_scripts_path is the nested folder, so $DIR itself is outside it.
SCRIPTS=$DIR/scripts
mkdir -p "$SCRIPTS"

cat > "$SCRIPTS"/ok.sh <<'EOS'
#!/usr/bin/env bash
while read -r line; do echo "ok_$line"; done
EOS

cat > "$SCRIPTS"/fail.sh <<'EOS'
#!/usr/bin/env bash
exit 3
EOS

# Writes two rows for one input row, so the result row count does not match.
cat > "$SCRIPTS"/rows.sh <<'EOS'
#!/usr/bin/env bash
cat > /dev/null
printf 'r1\nr2\n'
EOS

# A working script that sits outside user_scripts_path. It exists and it is executable,
# so only the containment check can refuse it: if that check went away, the UDF would
# succeed and print 'escaped_a' instead of an error code name.
cat > "$DIR"/escaped.sh <<'EOS'
#!/usr/bin/env bash
while read -r line; do echo "escaped_$line"; done
EOS

chmod 0755 "$SCRIPTS"/ok.sh "$SCRIPTS"/fail.sh "$SCRIPTS"/rows.sh "$DIR"/escaped.sh

# Readable but not executable: faccessat(X_OK) fails on mode 0400 even for root.
echo 'not a script' > "$SCRIPTS"/plain.txt
chmod 0400 "$SCRIPTS"/plain.txt

cat > "$DIR"/function.xml <<'EOS'
<functions>
    <function>
        <type>executable</type><name>cg_ok</name><format>TabSeparated</format>
        <argument><type>String</type></argument><return_type>String</return_type>
        <command>ok.sh</command>
    </function>
    <function>
        <type>executable</type><name>cg_outside</name><format>TabSeparated</format>
        <argument><type>String</type></argument><return_type>String</return_type>
        <command>../escaped.sh</command>
    </function>
    <function>
        <type>executable</type><name>cg_missing</name><format>TabSeparated</format>
        <argument><type>String</type></argument><return_type>String</return_type>
        <command>no_such.sh</command>
    </function>
    <function>
        <type>executable</type><name>cg_nonexec</name><format>TabSeparated</format>
        <argument><type>String</type></argument><return_type>String</return_type>
        <command>plain.txt</command>
    </function>
    <function>
        <type>executable</type><name>cg_fail</name><format>TabSeparated</format>
        <argument><type>String</type></argument><return_type>String</return_type>
        <command>fail.sh extra</command>
    </function>
    <function>
        <type>executable</type><name>cg_rows</name><format>TabSeparated</format>
        <argument><type>String</type></argument><return_type>String</return_type>
        <command>rows.sh</command>
    </function>
</functions>
EOS

# UDF definitions are server configuration, so they are injected as clickhouse-local
# server arguments, as in 02833_local_udf_options.sh. --ignore-error keeps the remaining
# statements running after a failing one, so one invocation covers every case:
#   cg_ok       a command inside user_scripts_path is accepted and its output is decoded
#   cg_outside  '../escaped.sh' leaves user_scripts_path lexically; the file is there and
#               it runs, so only the containment check can refuse it
#   cg_missing  the resolved path does not exist
#   cg_nonexec  the resolved path exists but is not executable
#   cg_fail     the script is accepted but exits non-zero, which the catch-all reports as
#               a UDF failure together with the quoted command arguments
#   cg_rows     the script returns two rows for one input row, so the result is refused
#               with UNSUPPORTED_METHOD, which the catch-all then reports as a UDF
#               failure: both code names appear on that line, the inner one first
# Only the code names and the decoded value are kept; the messages carry absolute paths.
$CLICKHOUSE_LOCAL --ignore-error -q "
    SELECT cg_ok('a');
    SELECT cg_outside('a');
    SELECT cg_missing('a');
    SELECT cg_nonexec('a');
    SELECT cg_fail('a');
    SELECT cg_rows('a');
    " -- \
    --user_scripts_path="$SCRIPTS" \
    --user_defined_executable_functions_config="$DIR"/function.xml \
    > "$DIR"/out 2> "$DIR"/err

grep -oa -E '^(ok|escaped)_[a-z]+$' "$DIR"/out
grep -oa -E '\((UNSUPPORTED_METHOD|UDF_EXECUTION_FAILED)\)' "$DIR"/err

rm -rf "$DIR"
