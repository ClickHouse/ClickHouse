#!/bin/bash
# Parser Memory Profiler - Batch Processing Script
# Processes SQL queries and generates jemalloc memory profiling results
#
# This script runs the parser_memory_profiler tool for each query,
# collects heap profiles, and generates detailed reports using jeprof.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLICKHOUSE_ROOT="${SCRIPT_DIR}/../.."

# Default paths - can be overridden with arguments
BUILD_DIR="${CLICKHOUSE_ROOT}/build"
# The profiler binary is built from src/Parsers/examples/parser_memory_profiler.cpp
PROFILER="${BUILD_DIR}/src/Parsers/examples/parser_memory_profiler"
JEPROF="${HOME}/github/jemalloc/bin/jeprof"
FLAMEGRAPH="${HOME}/FlameGraph/flamegraph.pl"

# Output settings
OUTPUT_DIR="${SCRIPT_DIR}/profiler_output"
PROFILES_DIR="${OUTPUT_DIR}/profiles"
QUERIES_FILE="${SCRIPT_DIR}/test_queries.txt"
RESULTS_FILE="${OUTPUT_DIR}/results.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Parser Memory Profiler - Batch Processing Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -q, --queries FILE    Input queries file (default: test_queries.txt)"
    echo "  -o, --output DIR      Output directory name (default: profiler_output)"
    echo "  -b, --build DIR       ClickHouse build directory (default: ../../build)"
    echo "  -p, --profiler PATH   Path to parser_memory_profiler binary"
    echo "  -j, --jeprof PATH     Path to jeprof binary"
    echo "  -f, --flamegraph PATH Path to flamegraph.pl"
    echo "  -h, --help            Show this help"
    echo ""
    echo "Prerequisites:"
    echo "  1. Build parser_memory_profiler:"
    echo "     cd build && ninja parser_memory_profiler"
    echo ""
    echo "  2. Install jeprof (for detailed profiling):"
    echo "     git clone https://github.com/jemalloc/jemalloc ~/github/jemalloc"
    echo "     cd ~/github/jemalloc && ./autogen.sh && make"
    echo ""
    echo "  3. Install FlameGraph (for flame graphs):"
    echo "     git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph"
    echo ""
    echo "  4. Install graphviz (for call graphs):"
    echo "     brew install graphviz  # macOS"
    echo "     apt install graphviz   # Linux"
    echo ""
    echo "Examples:"
    echo "  # Basic usage with default paths"
    echo "  ./run_profiler.sh"
    echo ""
    echo "  # Custom output directory"
    echo "  ./run_profiler.sh -o my_results"
    echo ""
    echo "  # Custom build directory"
    echo "  ./run_profiler.sh -b ../../build-release"
    echo ""
    echo "  # Use custom queries file"
    echo "  ./run_profiler.sh -q my_queries.txt -o my_results"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -q|--queries)
            QUERIES_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="${SCRIPT_DIR}/$2"
            PROFILES_DIR="${OUTPUT_DIR}/profiles"
            RESULTS_FILE="${OUTPUT_DIR}/results.json"
            shift 2
            ;;
        -b|--build)
            BUILD_DIR="$2"
            PROFILER="${BUILD_DIR}/src/Parsers/examples/parser_memory_profiler"
            shift 2
            ;;
        -p|--profiler)
            PROFILER="$2"
            shift 2
            ;;
        -j|--jeprof)
            JEPROF="$2"
            shift 2
            ;;
        -f|--flamegraph)
            FLAMEGRAPH="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            exit 1
            ;;
    esac
done

# Check prerequisites
check_prerequisites() {
    local missing=0
    
    if [[ ! -x "$PROFILER" ]]; then
        echo -e "${RED}Error: parser_memory_profiler not found at: $PROFILER${NC}"
        echo "Build it with: cd ${BUILD_DIR} && ninja parser_memory_profiler"
        missing=1
    fi
    
    if [[ ! -x "$JEPROF" ]]; then
        echo -e "${YELLOW}Warning: jeprof not found at: $JEPROF${NC}"
        echo "Profiling analysis will be limited. Install from:"
        echo "  git clone https://github.com/jemalloc/jemalloc ~/github/jemalloc"
        echo "  cd ~/github/jemalloc && ./autogen.sh && make"
    fi
    
    if [[ ! -x "$FLAMEGRAPH" ]]; then
        echo -e "${YELLOW}Warning: flamegraph.pl not found at: $FLAMEGRAPH${NC}"
        echo "Flame graphs will be skipped. Install from:"
        echo "  git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph"
    fi
    
    if [[ ! -f "$QUERIES_FILE" ]]; then
        echo -e "${RED}Error: Queries file not found: $QUERIES_FILE${NC}"
        missing=1
    fi
    
    # Check for graphviz (needed for call graph SVGs)
    if ! command -v dot &> /dev/null; then
        echo -e "${YELLOW}Warning: graphviz 'dot' not found${NC}"
        echo "Call graph SVGs will be skipped. Install with:"
        echo "  brew install graphviz  # macOS"
        echo "  apt install graphviz   # Linux"
    fi
    
    if [[ $missing -eq 1 ]]; then
        exit 1
    fi
}

# Create output directories
setup_output() {
    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$PROFILES_DIR"
    rm -f "$RESULTS_FILE"
}

# Process a single query
process_query() {
    local id=$1
    local query=$2
    local profile_prefix="${PROFILES_DIR}/query_${id}_"
    
    echo -e "${GREEN}Processing query $id...${NC}" >&2
    
    # Run profiler with jemalloc profiling
    # JE_MALLOC_CONF is used on macOS, MALLOC_CONF on Linux
    local output
    output=$(JE_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0 \
        MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0 \
        "$PROFILER" --profile "$profile_prefix" <<< "$query" 2>/dev/null)
    
    # Parse the output (format: length \t before \t after \t diff)
    local query_length allocated_before allocated_after allocated_diff
    read -r query_length allocated_before allocated_after allocated_diff <<< "$output"
    
    # Find the generated profile files
    local profile_before profile_after
    profile_before=$(ls -t "${profile_prefix}before."*.heap 2>/dev/null | head -1)
    profile_after=$(ls -t "${profile_prefix}after."*.heap 2>/dev/null | head -1)
    
    # Generate jeprof outputs if available
    local jeprof_text="" jeprof_objects="" collapsed_stacks="" graph_svg=""
    
    if [[ -x "$JEPROF" && -f "$profile_before" && -f "$profile_after" ]]; then
        # Text report (bytes)
        jeprof_text=$(timeout 120 "$JEPROF" --text --show_bytes \
            --base="$profile_before" \
            "$PROFILER" "$profile_after" 2>/dev/null | grep -v "^Using local file" || true)
        
        # Objects report
        jeprof_objects=$(timeout 120 "$JEPROF" --text --inuse_objects \
            --base="$profile_before" \
            "$PROFILER" "$profile_after" 2>/dev/null | grep -v "^Using local file" || true)
        
        # Collapsed stacks for flame graph
        collapsed_stacks=$(timeout 120 "$JEPROF" --collapsed --show_bytes \
            --base="$profile_before" \
            "$PROFILER" "$profile_after" 2>/dev/null | grep -v "^Using local file" || true)
        
        # SVG call graph (requires graphviz)
        if command -v dot &> /dev/null; then
            graph_svg=$(timeout 120 "$JEPROF" --svg --show_bytes \
                --nodefraction=0 --edgefraction=0 \
                --base="$profile_before" \
                "$PROFILER" "$profile_after" 2>/dev/null | grep -v "^Using local file" || true)
        fi
    fi
    
    # Generate flame graph SVG if available
    local flame_svg=""
    if [[ -x "$FLAMEGRAPH" && -n "$collapsed_stacks" ]]; then
        flame_svg=$(echo "$collapsed_stacks" | "$FLAMEGRAPH" \
            --title "Query $id Memory Allocation" \
            --countname "bytes" \
            --colors mem 2>/dev/null || true)
    fi
    
    # Escape strings for JSON
    local query_escaped jeprof_text_escaped jeprof_objects_escaped
    query_escaped=$(echo "$query" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().strip()))')
    jeprof_text_escaped=$(echo "$jeprof_text" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
    jeprof_objects_escaped=$(echo "$jeprof_objects" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
    collapsed_escaped=$(echo "$collapsed_stacks" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
    graph_svg_escaped=$(echo "$graph_svg" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
    flame_svg_escaped=$(echo "$flame_svg" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')
    
    # Output JSON object for this query
    cat << EOF
    {
      "id": $id,
      "query": $query_escaped,
      "query_length": $query_length,
      "allocated_before": $allocated_before,
      "allocated_after": $allocated_after,
      "allocated_diff": $allocated_diff,
      "jeprof_text": $jeprof_text_escaped,
      "jeprof_objects": $jeprof_objects_escaped,
      "collapsed_stacks": $collapsed_escaped,
      "graph_svg": $graph_svg_escaped,
      "flame_svg": $flame_svg_escaped
    }
EOF
}

# Main processing
main() {
    check_prerequisites
    setup_output
    
    echo -e "${GREEN}Starting batch profiling...${NC}"
    echo "Queries file: $QUERIES_FILE"
    echo "Output directory: $OUTPUT_DIR"
    echo "Profiler: $PROFILER"
    echo ""
    
    # Count queries
    local total_queries
    total_queries=$(grep -c ';' "$QUERIES_FILE" || echo 0)
    echo "Total queries to process: $total_queries"
    echo ""
    
    # Start JSON output
    echo '{"queries": [' > "$RESULTS_FILE"
    
    local id=1
    local first=1
    local query=""
    
    # Read queries line by line, accumulating until we hit a semicolon
    while IFS= read -r line || [[ -n "$line" ]]; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        
        # Accumulate query
        query+="$line"
        
        # Check if query is complete (ends with semicolon)
        if [[ "$line" =~ \;[[:space:]]*$ ]]; then
            # Add comma separator for JSON array
            if [[ $first -eq 0 ]]; then
                echo "," >> "$RESULTS_FILE"
            fi
            first=0
            
            # Process this query
            process_query "$id" "$query" >> "$RESULTS_FILE"
            
            # Reset for next query
            query=""
            ((id++))
        fi
    done < "$QUERIES_FILE"
    
    # Close JSON array
    echo '' >> "$RESULTS_FILE"
    echo ']}' >> "$RESULTS_FILE"
    
    echo ""
    echo -e "${GREEN}Profiling complete!${NC}"
    echo "Results saved to: $RESULTS_FILE"
    echo "Profiles saved to: $PROFILES_DIR"
    echo ""
    echo "To generate HTML report, run:"
    echo "  python3 ${SCRIPT_DIR}/generate_report.py -i $RESULTS_FILE -o ${OUTPUT_DIR}/report.html"
    echo ""
    echo "To compare two result sets:"
    echo "  python3 ${SCRIPT_DIR}/generate_comparison_report.py \\"
    echo "    --before results_v1/results.json --after results_v2/results.json \\"
    echo "    --output comparison.html"
}

main "$@"
