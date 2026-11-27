#!/bin/bash

# Simple File Cache Manager
# Uses clickhouse local for JSON metadata management
#
# HOW IT WORKS:
# - Add: Creates hard link (no data duplication) in ~/.file-cache/storage/
# - Get: Creates soft link at specified path pointing to cached file
# - Metadata stored in JSON format, queryable with ClickHouse
#
# METADATA STRUCTURE (metadata.json):
# {"id":"<inode>","name":"<filename>","path":"<original_path>","tag":"<tag>","size":<bytes>,"added":<timestamp>}
#
# EXAMPLE WORKFLOW:
# ./cache.sh add ~/large-file.bin v1.0     # Add file with tag 'v1.0'
# ./cache.sh get /tmp/myfile v1.0          # Retrieve as soft link
# ./cache.sh has v1.0                      # Check if cached
# ./cache.sh list                          # Show all cached files

export SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PARENT_DIR="$(dirname "$SCRIPT_DIR")"
CACHE_DIR="${CACHE_DIR:-$PARENT_DIR/data/file-cache}"
CACHE_STORAGE="$CACHE_DIR/storage"
CACHE_METADATA="$CACHE_DIR/metadata.json"
SCHEMA="id String, name String, path String, tag String, size UInt64, added UInt64"

# Initialize cache directory structure
init_cache() {
    mkdir -p "$CACHE_STORAGE"
    if [[ ! -f "$CACHE_METADATA" ]]; then
        touch "$CACHE_METADATA"
    fi
}

# Generate a unique ID for a file based on its inode
generate_file_id() {
    local file="$1"
    local stat_info=$(stat -c "%d:%i" "$file" 2>/dev/null || stat -f "%d:%i" "$file" 2>/dev/null)
    echo "${stat_info//:/}"
}

# Add a file to the cache
add_file() {
    local file="$1"
    local tag="$2"

    if [[ ! -f "$file" ]]; then
        echo "Error: File '$file' does not exist" >&2
        return 1
    fi

    if [[ -z "$tag" ]]; then
        echo "Error: Tag not specified" >&2
        echo "Usage: $0 add <file> <tag>" >&2
        return 1
    fi

    init_cache

    local file_id=$(generate_file_id "$file")
    local file_name=$(basename "$file")
    local file_path=$(realpath "$file")
    local file_size=$(stat -c%s "$file_path" 2>/dev/null || stat -f%z "$file_path" 2>/dev/null)
    local cache_path="$CACHE_STORAGE/$file_id"

    # Create hard link in cache
    if [[ ! -f "$cache_path" ]]; then
        ln "$file_path" "$cache_path" || {
            echo "Error: Failed to create hard link" >&2
            return 1
        }
    fi

    # Remove existing entry if present and add new one
    local temp_file=$(mktemp)

    # Check if metadata file is empty
    if [[ ! -s "$CACHE_METADATA" ]]; then
        # First entry
        clickhouse local -q "
            SELECT
                '$file_id' as id,
                '$file_name' as name,
                '$file_path' as path,
                '$tag' as tag,
                $file_size as size,
                $(date +%s) as added
        " --format=JSONEachRow > "$temp_file"
    else
        # Add to existing entries
        clickhouse local -q "
            WITH new_entry AS (
                SELECT
                    '$file_id' as id,
                    '$file_name' as name,
                    '$file_path' as path,
                    '$tag' as tag,
                    CAST($file_size AS UInt64) as size,
                    CAST($(date +%s) AS UInt64) as added
            )
            SELECT * FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')
            WHERE id != '$file_id'
            UNION ALL
            SELECT * FROM new_entry
        " --format=JSONEachRow > "$temp_file"
    fi

    mv "$temp_file" "$CACHE_METADATA"
    echo "Added file '$file_name' to cache with tag '$tag' (size: $(( file_size / 1048576 ))MB)"
}

# Get files by tag
get_files() {
    local output_path="$1"
    local tag="$2"

    if [[ -z "$output_path" ]]; then
        echo "Error: Output path not specified" >&2
        echo "Usage: $0 get <output_path> <tag>" >&2
        return 1
    fi

    if [[ -z "$tag" ]]; then
        echo "Error: Tag not specified" >&2
        echo "Usage: $0 get <output_path> <tag>" >&2
        return 1
    fi

    init_cache

    if [[ ! -s "$CACHE_METADATA" ]]; then
        echo "Cache is empty"
        return
    fi

    # Find matching file
    local file_info=$(clickhouse local -q "
        SELECT id, name
        FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')
        WHERE tag = '$tag'
        LIMIT 1
    " --format=TabSeparated 2>/dev/null)

    if [[ -z "$file_info" ]]; then
        echo "No file found with tag: $tag"
        return 1
    fi

    IFS=$'\t' read -r file_id file_name <<< "$file_info"
    local cache_path="$CACHE_STORAGE/$file_id"

    if [[ -f "$cache_path" ]]; then
        # Create parent directory if needed
        local output_dir=$(dirname "$output_path")
        mkdir -p "$output_dir"

        ln -sf "$cache_path" "$output_path"
        echo "Created soft link: $output_path -> $file_name"
        return 0
    else
        echo "Error: Cached file $file_id not found in storage" >&2
        return 1
    fi
}

# List all cached files
list_files() {
    init_cache

    if [[ ! -s "$CACHE_METADATA" ]]; then
        echo "Cache is empty"
        return
    fi

    local count=$(clickhouse local -q "SELECT count() FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')" 2>/dev/null || echo 0)

    if [[ "$count" -eq 0 ]]; then
        echo "Cache is empty"
        return
    fi

    echo "Cached files:"
    echo "=============================================="
    printf "%-30s %10s %16s %s\n" "Name" "Size" "Added" "Tag"
    echo "=============================================="

    clickhouse local -q "
        SELECT
            substring(name, 1, 30) as name,
            formatReadableSize(size) as size_human,
            formatDateTime(toDateTime(added), '%Y-%m-%d %H:%M') as added_date,
            tag
        FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')
        ORDER BY name
    " --format=TabSeparated 2>/dev/null | while IFS=$'\t' read -r name size added tag; do
        printf "%-30s %10s %16s %s\n" "$name" "$size" "$added" "$tag"
    done

    echo ""
    local total_size=$(clickhouse local -q "SELECT coalesce(sum(size), 0) FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')" 2>/dev/null || echo 0)
    echo "Total cache size: $(( total_size / 1048576 ))MB ($count files)"
}

# Check if file exists in cache
has_file() {
    local search_term="$1"

    init_cache

    if [[ ! -s "$CACHE_METADATA" ]]; then
        return 1
    fi

    # Check by name, ID, or tag
    local found=$(clickhouse local -q "
        SELECT count()
        FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')
        WHERE id = '$search_term'
           OR name = '$search_term'
           OR tag = '$search_term'
    " 2>/dev/null || echo 0)

    if [[ "$found" -gt 0 ]]; then
        # Show what was found
        clickhouse local -q "
            SELECT
                name,
                formatReadableSize(size) as size,
                tag
            FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')
            WHERE id = '$search_term'
               OR name = '$search_term'
               OR tag = '$search_term'
        " --format=TabSeparated 2>/dev/null | while IFS=$'\t' read -r name size tag; do
            echo "Found: $name ($size) [tag: $tag]"
        done
        return 0
    else
        return 1
    fi
}

# Remove file from cache
remove_file() {
    local search_term="$1"

    init_cache

    if [[ ! -s "$CACHE_METADATA" ]]; then
        echo "Cache is empty"
        return
    fi

    # Find file by name or ID
    local file_info=$(clickhouse local -q "
        SELECT id, name
        FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')
        WHERE id = '$search_term' OR name = '$search_term'
        LIMIT 1
    " --format=TabSeparated 2>/dev/null)

    if [[ -z "$file_info" ]]; then
        echo "Error: File '$search_term' not found in cache" >&2
        return 1
    fi

    IFS=$'\t' read -r file_id file_name <<< "$file_info"

    # Remove from storage
    rm -f "$CACHE_STORAGE/$file_id"

    # Update metadata
    local temp_file=$(mktemp)
    clickhouse local -q "
        SELECT * FROM file('$CACHE_METADATA', 'JSONEachRow', '$SCHEMA')
        WHERE id != '$file_id'
    " --format=JSONEachRow > "$temp_file"

    mv "$temp_file" "$CACHE_METADATA"
    echo "Removed file '$file_name' from cache"
}

# Show usage
usage() {
    cat << EOF
Simple File Cache Manager

Usage: $0 <command> [options]

Commands:
    add <file> <tag>                Add a file to cache with a tag
    get <output_path> <tag>         Get file with tag and create soft link at path
    list                            List all cached files
    has <name|id|tag>               Check if file exists in cache
    remove <name|id>                Remove a file from cache
    help                            Show this help message

Environment:
    CACHE_DIR                       Cache directory (default: ~/.file-cache)

Examples:
    $0 add bigfile.dat video
    $0 get /tmp/myvideo.dat video   # Create soft link at specific path
    $0 list
    $0 has video                    # Check by tag
    $0 has bigfile.dat              # Check by name
    $0 remove bigfile.dat

Note: Requires clickhouse to be installed
EOF
}

# Check if clickhouse is available
if ! command -v clickhouse &> /dev/null; then
    echo "Error: clickhouse is required but not found in PATH" >&2
    echo "Please install ClickHouse: https://clickhouse.com/docs/en/install" >&2
    exit 1
fi

# Main command handler
case "${1:-list}" in
    add)
        shift
        add_file "$@"
        ;;
    get)
        shift
        get_files "$@"
        ;;
    list)
        list_files
        ;;
    has)
        shift
        has_file "$@"
        ;;
    remove)
        shift
        remove_file "$@"
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        echo "Error: Unknown command '$1'" >&2
        usage
        exit 1
        ;;
esac
