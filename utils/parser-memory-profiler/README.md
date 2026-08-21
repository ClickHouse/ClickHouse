# Parser Memory Profiler

A tool for profiling memory allocations in the ClickHouse SQL parser using jemalloc's heap profiling capabilities.

## Overview

This utility helps analyze memory allocation patterns when parsing SQL queries. It's useful for:

- **Identifying memory hotspots** in the parser
- **Comparing memory usage** between different parser implementations
- **Optimizing AST node allocations** and data structures
- **Regression testing** for memory usage changes

## Components

| File | Description |
|------|-------------|
| `run_profiler.sh` | Bash script to batch-process multiple queries and generate JSON results |
| `generate_report.py` | Python script to generate an interactive HTML report for a single result set |
| `generate_comparison_report.py` | Python script to compare two result sets (before/after optimization) |
| `rebuild_reports.py` | Python script to regenerate reports from existing heap profile files |
| `test_queries.txt` | Sample set of 100 diverse SQL queries for testing |

The C++ profiler binary source is located at `src/Parsers/examples/parser_memory_profiler.cpp`.

## Prerequisites

### 1. Build the profiler binary

```bash
cd /path/to/clickhouse
mkdir -p build && cd build
cmake ..
ninja parser_memory_profiler
```

### 2. Install jeprof (from jemalloc)

jeprof is used to analyze heap profile files and generate detailed reports.

```bash
git clone https://github.com/jemalloc/jemalloc ~/github/jemalloc
cd ~/github/jemalloc
./autogen.sh
make
# Binary will be at ~/github/jemalloc/bin/jeprof
```

### 3. Install FlameGraph (for flame graphs)

```bash
git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph
```

### 4. Install graphviz (for call graph SVGs)

```bash
# macOS
brew install graphviz

# Ubuntu/Debian
apt install graphviz

# RHEL/CentOS
yum install graphviz
```

## Quick Start

### Basic Usage

```bash
cd utils/parser-memory-profiler

# Run profiler on default test queries
./run_profiler.sh -b ../../build

# Generate HTML report
python3 generate_report.py -i profiler_output/results.json -o profiler_output/report.html

# Open the report
open profiler_output/report.html  # macOS
xdg-open profiler_output/report.html  # Linux
```

### Custom Queries

Create a file with SQL queries (one per line, ending with semicolons):

```sql
-- my_queries.txt
SELECT * FROM users WHERE id = 1;
CREATE TABLE test (a Int64, b String) ENGINE = MergeTree() ORDER BY a;
INSERT INTO test VALUES (1, 'hello'), (2, 'world');
```

Run the profiler:

```bash
./run_profiler.sh -q my_queries.txt -o my_results -b ../../build
python3 generate_report.py -i my_results/results.json -o my_results/report.html
```

### Comparing Two Versions

Profile both versions and generate a comparison report:

```bash
# Profile original version
git checkout main
cd build && ninja parser_memory_profiler && cd ..
./run_profiler.sh -o results_before -b ../build

# Profile optimized version
git checkout my-optimization-branch
cd build && ninja parser_memory_profiler && cd ..
./run_profiler.sh -o results_after -b ../build

# Generate comparison report
python3 generate_comparison_report.py \
    --before results_before/results.json \
    --after results_after/results.json \
    --output comparison.html \
    --label-before "Original" \
    --label-after "Optimized"
```

## Output Files

After running `run_profiler.sh`, the output directory contains:

```
profiler_output/
├── profiles/           # jemalloc heap profile files
│   ├── query_1_before.*.heap
│   ├── query_1_after.*.heap
│   ├── query_2_before.*.heap
│   └── ...
├── results.json        # JSON with all profiling data
└── report.html         # Interactive HTML report (after generate_report.py)
```

## Understanding the Reports

### Single Report (generate_report.py)

The HTML report shows for each query:

- **Query text** with syntax highlighting
- **Memory stats**: bytes allocated before/after parsing, difference
- **Profile Report (Bytes)**: jeprof text output showing allocation sites
- **Profile Report (Objects)**: allocation counts by call site
- **Call Graph**: SVG visualization of allocation call tree
- **Flame Graph**: Interactive flame graph of allocations

### Comparison Report (generate_comparison_report.py)

Shows side-by-side comparison:

- **Summary**: Total memory difference across all queries
- **Per-query comparison**: Before vs After memory usage
- **Sortable table**: Sort by memory change, query length, etc.
- **Expandable details**: Click rows to see detailed profiles

## How It Works

1. **parser_memory_profiler.cpp**:
   - Initializes jemalloc profiling via `mallctl()`
   - Takes a heap dump before parsing
   - Parses the SQL query using ClickHouse's parser
   - Takes a heap dump after parsing
   - Reports memory statistics

2. **run_profiler.sh**:
   - Iterates through queries in the input file
   - Runs `parser_memory_profiler` with `JE_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0`
   - Uses `jeprof` to analyze heap profile diffs
   - Generates text reports, collapsed stacks, and SVGs
   - Collects everything into `results.json`

3. **generate_report.py**:
   - Reads `results.json`
   - Creates a self-contained HTML file with embedded data
   - Uses iframes for SVG content to preserve interactivity

## Environment Variables

| Variable | Description |
|----------|-------------|
| `JE_MALLOC_CONF` | jemalloc configuration (macOS with `je_` prefix) |
| `MALLOC_CONF` | jemalloc configuration (Linux) |

Required settings for profiling:
```bash
JE_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0
```

- `prof:true` - Enable profiling support
- `prof_active:true` - Activate profiling at startup  
- `lg_prof_sample:0` - Sample every allocation (log2(1) = 0)

## Troubleshooting

### "jemalloc profiling not enabled"

The jemalloc library must be built with profiling support. Check:
```bash
./parser_memory_profiler <<< "SELECT 1"
```

If it shows "config.prof: no", jemalloc was built without `--enable-prof`.

### Empty jeprof reports

Increase the sampling rate or check that `lg_prof_sample:0` is set. Higher values (like 19, the default) will miss small allocations.

### Timeout errors

jeprof can be slow when analyzing large binaries. Increase the timeout:
```bash
python3 rebuild_reports.py profiler_output --timeout 300
```

### "dot: command not found"

Install graphviz for call graph generation:
```bash
brew install graphviz  # macOS
apt install graphviz   # Linux
```

## Tips for Memory Optimization

1. **Focus on hotspots**: Look at the top entries in the profile reports
2. **Compare allocations vs objects**: High object count with low bytes suggests small allocations
3. **Check flame graphs**: Wide bars indicate major allocation sources
4. **Use comparison reports**: Track improvements across iterations

## Example Results

Typical findings from parser memory profiling:

- **AST node allocations**: `std::make_shared` overhead can be significant
- **String copies**: Identifier and literal strings are frequently copied
- **Vector resizing**: Pre-sizing vectors can reduce allocations
- **Small object overhead**: Many small allocations have per-allocation overhead

## License

This tool is part of ClickHouse and is licensed under the Apache License 2.0.
