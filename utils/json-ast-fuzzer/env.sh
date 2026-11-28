#!/bin/bash
# Shared environment for the JSON-AST fuzzer harness. Source this from every script: `. "$(dirname "$0")/env.sh"`.
# Override any of these by exporting them before running a script.
REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"            # ClickHouse checkout root
BUILD_FUZZ="${BUILD_FUZZ:-$REPO/build_fuzz}"                                   # asan+ubsan fuzzer build dir (default)
EXEC_BIN="${EXEC_BIN:-$BUILD_FUZZ/programs/local/fuzzers/json_ast_sql_execution_fuzzer}"
PARSER_BIN="${PARSER_BIN:-$BUILD_FUZZ/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast_sql_parser_fuzzer}"
CONVERTER_BIN="${CONVERTER_BIN:-$BUILD_FUZZ/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast_seed_converter}"
REF_CLICKHOUSE="${REF_CLICKHOUSE:-$REPO/build/programs/clickhouse}"            # a release build, for repro/seed-gen
WORK="${WORK:-$REPO/fuzz_work}"                                               # all corpora/sessions/logs live here
ARGS_FILE="${ARGS_FILE:-$REPO/utils/json-ast-fuzzer/exec_local_args.txt}"
# llvm-symbolizer: try versioned then plain
SYMBOLIZER="${SYMBOLIZER:-$(command -v llvm-symbolizer-21 || command -v llvm-symbolizer || echo /usr/bin/llvm-symbolizer)}"
export REPO BUILD_FUZZ EXEC_BIN PARSER_BIN CONVERTER_BIN REF_CLICKHOUSE WORK ARGS_FILE SYMBOLIZER
mkdir -p "$WORK"
