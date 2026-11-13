#!/usr/bin/env bash
# Download the clang that ClickHouse CI compiles with and print the CC and CXX to configure a build with it.
#
# It is the LLVM release that the build docs install with llvm.sh, from the same commit, built with PGO and BOLT
# by ci/jobs/build_toolchain.py. The build is pinned by TOOLCHAIN_COMMIT in ci/docker/binary-builder/Dockerfile,
# and CI compiles with it only while it is the compiler that ToolSet in ci/defs/defs.py names.
# As in CI, the build keeps using the installed lld and LLVM tools. The compiler needs glibc 2.38 or newer.
#
# Usage: eval "$(utils/download-ci-toolchain.sh [CACHE_DIRECTORY])"
# The compiler is kept in CACHE_DIRECTORY/<commit>-<arch>, by default in ~/.cache/clickhouse-ci-toolchain.
set -euo pipefail

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) arch=amd64 ;;
    Linux-aarch64) arch=aarch64 ;;
    *) echo "CI builds its compiler only for Linux x86_64 and aarch64" >&2; exit 1 ;;
esac
for tool in curl zstd; do
    command -v "$tool" > /dev/null || { echo "$tool is required" >&2; exit 1; }
done

root=$(cd "$(dirname "$0")/.." && pwd)
dockerfile="$root/ci/docker/binary-builder/Dockerfile"
commit=$(sed -n 's/^ARG TOOLCHAIN_COMMIT=//p' "$dockerfile")
s3_dir=$(sed -n 's/^ARG TOOLCHAIN_S3_DIR=//p' "$dockerfile")
s3_dir=${s3_dir//'${TOOLCHAIN_COMMIT}'/$commit}
ci_cc=$(sed -n 's/^ *COMPILER_C = "\(clang-[0-9][0-9]*\)"$/\1/p' "$root/ci/defs/defs.py")
ci_cxx=$(sed -n 's/^ *COMPILER_CPP = "\(clang++-[0-9][0-9]*\)"$/\1/p' "$root/ci/defs/defs.py")
if [ -z "$commit" ] || [ "$commit" = none ] || [ -z "$s3_dir" ] || [ -z "$ci_cc" ] || [ -z "$ci_cxx" ]; then
    echo "Cannot find the CI compiler in $dockerfile and $root/ci/defs/defs.py" >&2
    exit 1
fi

cache=${1:-"${XDG_CACHE_HOME:-$HOME/.cache}/clickhouse-ci-toolchain"}
dir="$cache/$commit-$arch"
if [ ! -e "$dir" ]; then
    mkdir -p "$cache"
    tmp=$(mktemp -d "$cache/.download.XXXXXX")
    trap 'rm -rf "$tmp"' EXIT
    # The archive (1.8 GB) holds the whole LLVM installation; keep only the compiler and its resource directory.
    curl -fSL "https://clickhouse-builds.s3.amazonaws.com/$s3_dir/build_toolchain_pgo_bolt_$arch/clang-pgo-bolt.tar.zst" \
        | zstd -dc \
        | tar -x -C "$tmp" --wildcards './bin/clang' './bin/clang-[0-9]*' './bin/clang++-[0-9]*' './lib/clang/*'
    "$tmp/bin/clang" --version > /dev/null
    mv -T "$tmp" "$dir"
fi
for compiler in "$ci_cc" "$ci_cxx"; do
    if [ ! -x "$dir/bin/$compiler" ]; then
        echo "$dir has no $compiler, which CI compiles with" >&2
        exit 1
    fi
done
dir=$(cd "$dir" && pwd)

echo "Configure a new build directory with it: CMake keeps the compiler of an existing one." >&2
# CI's versioned names: CMake looks for llvm-objcopy and the other LLVM tools with the compiler's version suffix.
printf 'export CC=%q CXX=%q\n' "$dir/bin/$ci_cc" "$dir/bin/$ci_cxx"
