# shellcheck shell=bash
# Shared by the `executable_udf_shared_memory` and `executable_udf_pipe` stateless tests (the commands of
# the latter are the `pipe_*` scripts here). Each scenario runs a `clickhouse-local`
# of its own: a pool lives as long as the process does, so the queries of one scenario share its
# workers, and every scenario starts from a process that holds no region and no charge at all.

SHM_UDF_SCRIPTS="$CUR_DIR/shm_udf_scripts"
SHM_UDF_WORK=$(mktemp -d)
trap 'rm -rf "$SHM_UDF_WORK"' EXIT

# The unit the server compares footprints and caps in: the page size of this kernel - or the
# transparent huge page, where the kernel backs `shmem` with those regardless of file size
# (`shmem_enabled` `always`/`force`), and `st_blocks` of any region reports at least one.
SHM_PAGE=$(getconf PAGESIZE)
if grep -qE '\[(always|force)\]' /sys/kernel/mm/transparent_hugepage/shmem_enabled 2>/dev/null; then
    huge_page=$(cat /sys/kernel/mm/transparent_hugepage/hpage_pmd_size)
    if [ "$huge_page" -gt "$SHM_PAGE" ]; then
        SHM_PAGE=$huge_page
    fi
fi

# `size` rounded up to whole pages.
function shm_pages()
{
    echo $(( ($1 + SHM_PAGE - 1) / SHM_PAGE * SHM_PAGE ))
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
        /^Logging .* to / { next }
        /^Received exception:$/ { next }
        skipping_query { if (/;\)$/) skipping_query = 0; next }
        /^\(query: / { if (!/;\)$/) skipping_query = 1; next }
        # A message spans several lines when what it quotes does (stderr of a command, for one): it
        # is gathered up to the line that ends it with the name of its code.
        /^Code: / { message = "" ; in_message = 1 }
        in_message {
            message = message " " $0
            if (!/\([A-Z][A-Z_]+\)[.,]*$/)
                next
            in_message = 0
            codes = ""
            line = message
            while (match(line, /\([A-Z][A-Z_]+\)/))
            {
                codes = codes " " substr(line, RSTART + 1, RLENGTH - 2)
                line = substr(line, RSTART + RLENGTH)
            }
            print "error:" codes
            next
        }
        { print }'
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
