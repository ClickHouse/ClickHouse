#!/bin/bash

user="default"
path="."

usage() {
    echo
    echo "A trivial script to upload your files into ClickHouse."
    echo "You might want to use something like Dropbox instead, but..."
    echo
    echo "Usage: $0 --host <hostname> [--user <username>] --password <password> <path>"
    exit 1
}

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --host)
            host="$2"
            shift 2
            ;;
        --user)
            user="$2"
            shift 2
            ;;
        --password)
            password="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            path="$1"
            shift 1
            ;;
    esac
done

if [ -z "$host" ] || [ -z "$password" ]; then
    echo "Error: --host and --password are mandatory."
    usage
fi

clickhouse-client --host "$host" --user "$user" --password "$password" --secure --query "CREATE TABLE IF NOT EXISTS default.files (time DEFAULT now(), path String, content String CODEC(ZSTD(6))) ENGINE = MergeTree ORDER BY (path, time)" &&
find "$path" -type f | clickhouse-local --input-format LineAsString \
    --max-block-size 1 --min-insert-block-size-rows 0 --min-insert-block-size-bytes '100M' --max-insert-threads 1 \
    --query "INSERT INTO FUNCTION remoteSecure('$host', default.files, '$user', '$password') (path, content) SELECT line, file(line) FROM table" --progress
