#!/bin/bash

function start_minio()
{
    (
        tmp_dir='/tmp/minio'
        mkdir -p $tmp_dir
        echo "minio data dir: $tmp_dir"
        cd $tmp_dir || exit 1
        # Check minio is run
        if ! lsof -i :"11111" > /dev/null 2>&1; then
            rm -rf "${tmp_dir:?}"/minio_data
            echo "Setup minio"
            # Different ClickHouse versions have setup_minio.sh in different locations
            if [[ -f "$WORK_TREE/tests/docker_scripts/setup_minio.sh" ]]; then
                file_path="$WORK_TREE/tests/docker_scripts/setup_minio.sh"
            elif [[ -f "$WORK_TREE/ci/jobs/scripts/functional_tests/setup_minio.sh" ]]; then
                file_path="$WORK_TREE/ci/jobs/scripts/functional_tests/setup_minio.sh"
                export TEST_DIR=$WORK_TREE/tests
                export TEMP_DIR=$tmp_dir
            elif [[ -f "$WORK_TREE/docker/test/stateless/setup_minio.sh" ]]; then
                file_path="$WORK_TREE/docker/test/stateless/setup_minio.sh"
            else
                echo "minio setup script not found"
                exit 255
            fi

            $file_path stateless $WORK_TREE/tests
        fi
    )
}


function wait_ch_start()
{
    TIMEOUT=40
    INTERVAL=1
    elapsed=0

    # Start waiting for the ClickHouse server
    echo "Waiting for ClickHouse server to become available..."

    while true; do
        # Try to execute a simple query
        if $1 client --port $2 --query 'SELECT 1' &>/dev/null; then
            echo "ClickHouse server $2 is available."
            return
        else
            # Check if we've reached the timeout
            if [ "$elapsed" -ge "$TIMEOUT" ]; then
                echo "Timed out after $TIMEOUT seconds waiting for ClickHouse server $2."
                exit 1
            fi

            # Wait before retrying
            sleep "$INTERVAL"
            elapsed=$((elapsed + INTERVAL))
        fi
    done
}


run_with_retry()
{
    local command="$1"

    while true; do
        echo "Running: $command"

        # Run the command
        if $command; then
            echo "✓ Command succeeded!"
            break
        else
            echo "✗ Command failed with exit code: $?"
            echo
            echo "Please fix the compilation error and press Enter to retry..."
            echo "(Press Ctrl+C to exit)"

            # Wait for user input
            read -r

            echo "Retrying..."
            echo
        fi
    done
}
