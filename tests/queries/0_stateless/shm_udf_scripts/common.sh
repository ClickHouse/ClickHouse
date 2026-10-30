# shellcheck shell=bash
# Shared by the `executable_udf_shared_memory` and `executable_udf_pipe` stateless tests (the commands of
# the latter are the `pipe_*` scripts here). Each scenario runs a `clickhouse-local`
# of its own: a pool lives as long as the process does, so the queries of one scenario share its
# workers, and every scenario starts from a process that holds no region and no charge at all.

SHM_UDF_SCRIPTS="$CUR_DIR/shm_udf_scripts"
SHM_UDF_WORK=$(mktemp -d)
trap 'rm -rf "$SHM_UDF_WORK"' EXIT

# The unit the server compares footprints and caps in (`SharedMemoryRegion::roundUpToPages`): the
# page size of this kernel, or the largest folio the kernel backs `shmem` with regardless of file size
# - the transparent huge page with `shmem_enabled` `always`/`force`, and, from Linux 6.11 on, any size
# whose own `hugepages-<size>kB/shmem_enabled` is `always` (or `inherit`, with the former). `st_blocks`
# of any region then reports at least one such folio.
THP_DIR=/sys/kernel/mm/transparent_hugepage
SHM_PAGE=$(getconf PAGESIZE)
thp_top_always=0
if grep -qE '\[(always|force)\]' "$THP_DIR/shmem_enabled" 2>/dev/null; then
    thp_top_always=1
    huge_page=$(cat "$THP_DIR/hpage_pmd_size")
    if [ "$huge_page" -gt "$SHM_PAGE" ]; then
        SHM_PAGE=$huge_page
    fi
fi
for knob in "$THP_DIR"/hugepages-*kB/shmem_enabled; do
    [ -r "$knob" ] || continue
    if grep -q '\[always\]' "$knob" || { [ "$thp_top_always" = 1 ] && grep -q '\[inherit\]' "$knob"; }; then
        size_kb=${knob#"$THP_DIR"/hugepages-}
        size_kb=${size_kb%%kB/*}
        if [ $((size_kb * 1024)) -gt "$SHM_PAGE" ]; then
            SHM_PAGE=$((size_kb * 1024))
        fi
    fi
done

# For a test whose commands lay a region out page by page (`shm_udf_region.py`, in base pages): on a
# host that backs `shmem` with large folios (`SHM_PAGE` larger than the page) the layout it builds is
# not the one its expectations describe, so it prints its reference and stops there.
function shm_require_base_pages()
{
    if [ "$SHM_PAGE" != "$(getconf PAGESIZE)" ]; then
        # The test that sourced this file: the outermost script, however it was started.
        cat "$CUR_DIR/$(basename "${BASH_SOURCE[${#BASH_SOURCE[@]} - 1]}" .sh).reference"
        exit 0
    fi
}

# `size` rounded up to whole pages.
function shm_pages()
{
    echo $(( ($1 + SHM_PAGE - 1) / SHM_PAGE * SHM_PAGE ))
}

# A function of the shared-memory transport, as a `<function>` element for `shm_functions`: the name,
# the type, the options that make it what it is, the command, the arguments (one `UInt64` if not
# given), the format (`TabSeparated` if not given).
function shm_function()
{
    echo "<function><type>$2</type><name>$1</name><return_type>String</return_type>"
    echo "${5-<argument><type>UInt64</type></argument>}<format>${6:-TabSeparated}</format>"
    echo "<use_shared_memory>1</use_shared_memory>$3<command>$4</command></function>"
}

# Writes the functions read from stdin - `<function>` elements - as the configuration of the next
# `shm_local`.
function shm_functions()
{
    {
        echo "<functions>"
        cat
        echo "</functions>"
    } > "$SHM_UDF_WORK/shm_function.xml"
}

# Runs the queries in one `clickhouse-local`, with the functions written by `shm_functions`. Two
# views are there for every scenario: `shm_regions` - the regions this process holds, re-read on
# every select - and `shm_pooled` - what pooled workers hold while idle, charged to the server.
# An exception does not stop the queries after it, and is printed as `error:` and the names of the
# error codes in its message - without the text of the query it failed, which is echoed after it and
# ends with `;)` - so that a reference does not depend on the wording around them. The output as it
# was is kept for `shm_output_contains`, and what the process logs goes to a file of its own, for
# `shm_log_contains`. Arguments after the queries are passed to `clickhouse-local` as they are.
function shm_local()
{
    rm -f "$SHM_UDF_WORK/local.log"
    $CLICKHOUSE_LOCAL --ignore-error --logger.log="$SHM_UDF_WORK/local.log" --logger.level=information "${@:2}" --query "
        CREATE VIEW shm_regions AS
            SELECT * FROM executable('shm_regions.sh', TSV, 'inode UInt64, size UInt64, committed UInt64');
        CREATE VIEW shm_pooled AS
            SELECT value FROM system.metrics WHERE metric = 'ExecutableUDFSharedMemoryPooledBytes';
        $1" \
        -- --user_scripts_path="$SHM_UDF_SCRIPTS" \
        --user_defined_executable_functions_config="$SHM_UDF_WORK/shm_function.xml" 2>&1 \
    | tee "$SHM_UDF_WORK/local.out" \
    | awk '
        # Prints the codes of the message gathered so far, as `error:` and their names.
        function flush_message(    codes, line)
        {
            codes = ""
            line = message
            while (match(line, /\([A-Z][A-Z_]+\)/))
            {
                codes = codes " " substr(line, RSTART + 1, RLENGTH - 2)
                line = substr(line, RSTART + RLENGTH)
            }
            print "error:" codes
            in_message = 0
            version_pending = 0
        }
        /^Logging .* to / { next }
        /^Received exception:$/ { next }
        skipping_query { if (/;\)$/) skipping_query = 0; next }
        /^\(query: / { if (!/;\)$/) skipping_query = 1; next }
        # A message ending with the version of the server (as a debug build ends every message, and
        # any build one received from another server) may be over, or may be quoted inside an outer
        # one that goes on on the next line, starting with the closing quote.
        version_pending {
            if (substr($0, 1, 1) == "\047")
                version_pending = 0
            else
                flush_message()
        }
        # A message spans several lines when what it quotes does (stderr of a command, for one): it
        # is gathered up to the line that ends it with the name of its code.
        /^Code: / { message = "" ; in_message = 1 }
        in_message {
            message = message " " $0
            if (/\([A-Z][A-Z_]+\)[.,]*$/)
                flush_message()
            else if (/\([A-Z][A-Z_]+\)[.,]* \(version [^)]*\)$/)
                version_pending = 1
            next
        }
        { print }
        END { if (in_message) flush_message() }'
}

# How many lines of the last `shm_local`'s own output - exception messages included - contain the text.
function shm_output_contains()
{
    # `grep -c` prints 0 and exits with 1 when nothing matches; a count of zero is an answer, not a
    # failure, and as the last command of a test it would fail the test with return code 1.
    grep -cF -- "$1" "$SHM_UDF_WORK/local.out" || true
}

# Whether the last `shm_local` logged the text: prints `1` or `0`.
function shm_log_contains()
{
    grep -qF -- "$1" "$SHM_UDF_WORK/local.log" && echo 1 || echo 0
}

# Like `shm_local`, in a process that listens on HTTP and has a user `other` that may select anything:
# the queries can send queries of that user to the same process (`shm_as_user`), and so to the same
# pools.
function shm_local_listening()
{
    shm_local "
        CREATE USER other IDENTIFIED WITH no_password;
        GRANT SELECT ON *.* TO other;
        SYSTEM START LISTEN HTTP;
        $1" --listen_host 127.0.0.1 --http_port 0 "${@:2}"
}

# A query that runs the query given as the user given, through the HTTP interface of the
# `shm_local_listening` it is run in. `http_make_head_request` is off, because a `HEAD` request runs
# the query too, and `url` does not retry: a retry would run the query again - on a fresh worker, say,
# hiding a borrow that failed.
function shm_as_user()
{
    echo "SELECT * FROM url('http://127.0.0.1:' || toString(getServerPort('http_port'))
        || '/?user=$1&query=' || encodeURLComponent('$2'), TSV, 'result String') SETTINGS http_make_head_request = 0, http_max_tries = 1;"
}

# Prints `gone` once the process is gone, or a zombie waiting to be reaped - either way it runs no
# code - and `running` if it is not within five seconds. A killed process is torn down by the kernel
# right after the signal, so the wait is for that, bounded.
function shm_process_state()
{
    local state
    for _ in $(seq 1 50); do
        state=$(awk '{ print $3 }' "/proc/$1/stat" 2>/dev/null)
        if [[ -z "$state" || "$state" == Z ]]; then
            echo "gone"
            return
        fi
        sleep 0.1
    done
    echo "running"
}
