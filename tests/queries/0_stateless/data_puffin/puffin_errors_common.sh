# Shared parallel harness for Puffin error suites (sourced by 04257 / 04612 / 04613).
# Caller must set CURDIR and source shell_config.sh first.

DATA="$CURDIR/data_puffin"
PUFFIN="$DATA/spark_deletion_vector.puffin"
PARALLEL="${PUFFIN_ERRORS_PARALLEL:-4}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

pids=()
id=0

wait_one()
{
    [[ ${#pids[@]} -eq 0 ]] && return
    wait "${pids[0]}" || true
    pids=("${pids[@]:1}")
}

run_case()
{
    local case_id="$1"
    local kind="$2"
    local path_or_label="$3"
    local needle="$4"
    local extra="${5:-}"
    local out="$TMP/$case_id.out"
    local err

    {
        case "$kind" in
            puffin)
                echo "--- $(basename "$path_or_label") ---"
                err=$($CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$path_or_label', Puffin)" 2>&1) || true
                echo "$err" | grep -oF "$needle" || true
                if [[ -n "$extra" ]]; then
                    echo "$err" | grep -oF "$extra" || true
                fi
                ;;
            raw_puffin)
                echo "--- $path_or_label ---"
                err=$($CLICKHOUSE_LOCAL -q "$extra" 2>&1) || true
                echo "$err" | grep -oF "$needle" || true
                ;;
            meta)
                echo "--- $(basename "$path_or_label") ---"
                $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$path_or_label', PuffinMetadata)" 2>&1 \
                    | grep -oF "$needle" || true
                ;;
            meta_re)
                echo "--- $(basename "$path_or_label") ---"
                $CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$path_or_label', PuffinMetadata)" 2>&1 \
                    | grep -oE "$needle" || true
                ;;
            raw_meta)
                echo "--- $path_or_label ---"
                err=$($CLICKHOUSE_LOCAL -q "$extra" 2>&1) || true
                echo "$err" | grep -oF "$needle" || true
                ;;
            *)
                echo "unknown puffin error case kind: $kind" >&2
                return 1
                ;;
        esac
    } > "$out"
}

launch()
{
    run_case "$@" &
    pids+=($!)
    while [[ ${#pids[@]} -ge $PARALLEL ]]; do
        wait_one
    done
}

finish_puffin_errors()
{
    while [[ ${#pids[@]} -gt 0 ]]; do
        wait_one
    done

    local i
    for ((i = 0; i < id; i++)); do
        cat "$TMP/$i.out"
    done
}
