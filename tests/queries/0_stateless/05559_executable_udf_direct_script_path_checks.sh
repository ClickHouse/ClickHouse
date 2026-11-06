#!/usr/bin/env bash
# An executable UDF with execute_direct resolves <command> inside user_scripts_path and
# must refuse a command that escapes that folder, does not exist, or is not executable.
# Expected behaviour: docs/reference/functions/regular-functions/udf.mdx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_udf
rm -rf "$DIR"
mkdir -p "$DIR"

cat > "$DIR"/ok.sh <<'EOS'
#!/usr/bin/env bash
while read -r line; do echo "ok_$line"; done
EOS

cat > "$DIR"/fail.sh <<'EOS'
#!/usr/bin/env bash
exit 3
EOS

# Writes two rows for one input row, so the result row count does not match.
cat > "$DIR"/rows.sh <<'EOS'
#!/usr/bin/env bash
cat > /dev/null
printf 'r1\nr2\n'
EOS

chmod 0755 "$DIR"/ok.sh "$DIR"/fail.sh "$DIR"/rows.sh

# Readable but not executable: faccessat(X_OK) fails on mode 0400 even for root.
echo 'not a script' > "$DIR"/plain.txt
chmod 0400 "$DIR"/plain.txt

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
        <command>../ok.sh</command>
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
# server arguments, as in 02833_local_udf_options.sh. One invocation per case,
# because the first error aborts the rest of a single invocation.
run_udf()
{
    $CLICKHOUSE_LOCAL -q "SELECT $1" -- \
        --user_scripts_path="$DIR" \
        --user_defined_executable_functions_config="$DIR"/function.xml 2>&1 \
        | grep -oam1 -E '^ok_[a-z]+$|\((UNSUPPORTED_METHOD|UDF_EXECUTION_FAILED)\)'
}

# A command inside user_scripts_path is accepted and its output is decoded.
run_udf "cg_ok('a')"

# '../ok.sh' leaves user_scripts_path lexically, so it is refused before any exec.
run_udf "cg_outside('a')"

# The resolved path does not exist.
run_udf "cg_missing('a')"

# The resolved path exists but is not executable.
run_udf "cg_nonexec('a')"

# The script is accepted but exits non-zero, which is reported as a UDF failure
# together with the quoted command arguments.
run_udf "cg_fail('a')"

# The script runs but returns two rows for one input row, so the result is refused with
# UNSUPPORTED_METHOD, which the catch-all then reports as a UDF failure: both code names
# appear, the inner one first.
run_udf "cg_rows('a')"

rm -rf "$DIR"
