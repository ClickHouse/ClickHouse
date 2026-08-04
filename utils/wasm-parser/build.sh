#!/usr/bin/env bash
# Build the ClickHouse SQL parser as a standalone WebAssembly module.
#
#   ./utils/wasm-parser/build.sh [--no-formatting] [--no-dcl] [output-directory]
#
# With --no-formatting the module can only answer whether a query parses, and is about a fifth
# smaller: turning an AST back into SQL is one virtual call away from every AST node, so it is all
# or nothing.
#
# With --no-dcl it does not accept access management - `CREATE USER`, `GRANT`, row policies,
# quotas, `SHOW GRANTS` and the rest - which a Web UI has little use for and which costs another
# quarter of what is left.
#
# Requires a wasi-sdk in $WASI_SDK (or ./tmp/wasi-sdk-*), for example:
#   curl -sL https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-33/wasi-sdk-33.0-$(uname -m)-linux.tar.gz | tar xz -C tmp
#
# The result exports a small C interface (see wasm_parser.cpp) and needs an engine with the
# WebAssembly exception-handling proposal. Test it with:
#   node --experimental-wasm-exnref utils/wasm-parser/test.mjs <output-directory>/parser.wasm

set -euo pipefail

NO_FORMATTING=
NO_DCL=
while true; do
    case "${1:-}" in
        --no-formatting) NO_FORMATTING=1; shift ;;
        --no-dcl) NO_DCL=1; shift ;;
        *) break ;;
    esac
done

CH=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
HERE=$CH/utils/wasm-parser
OUT=${1:-$CH/tmp/wasm-parser}
WASI=${WASI_SDK:-$(echo "$CH"/tmp/wasi-sdk-*-linux | cut -d' ' -f1)}

if [ ! -x "$WASI/bin/clang++" ]; then
    echo "wasi-sdk not found; set WASI_SDK to its root (looked in $WASI)" >&2
    exit 1
fi

CXX=$WASI/bin/clang++
mkdir -p "$OUT/obj" "$OUT/gen/Common"

# All optional features are off: this build has no compression, no formats, no network.
sed 's/#cmakedefine01 \(.*\)/#define \1 0/' "$CH/src/Common/config.h.in" > "$OUT/gen/Common/config.h"

INCS=(
  -I"$HERE/shim" -I"$OUT/gen" -I"$OUT/gen/Common"
  -I"$CH/src" -I"$CH/base"
  -I"$CH/base/poco/Foundation/include" -I"$CH/base/poco/Util/include"
  -I"$CH/base/poco/Net/include" -I"$CH/base/poco/JSON/include" -I"$CH/base/poco/XML/include"
  -I"$CH/base/pcg-random" -I"$CH/base/widechar_width"
  -I"$CH/contrib/fmtlib/include" -I"$CH/contrib/abseil-cpp" -I"$CH/contrib/boost"
  -I"$CH/contrib/magic_enum/include/magic_enum" -I"$CH/contrib/cityhash102/include"
  -I"$CH/contrib/libdivide" -I"$CH/contrib/miniselect/include" -I"$CH/contrib/pdqsort"
  -I"$CH/contrib/ipnsort" -I"$CH/contrib/driftsort" -I"$CH/contrib/wyhash"
  -I"$CH/contrib/xxHash" -I"$CH/contrib/double-conversion" -I"$CH/contrib/dragonbox/include"
  -I"$CH/contrib/fast_float/include" -I"$CH/contrib/re2" -I"$CH/contrib/sparsehash-c11"
  -I"$CH/contrib/croaring/include" -I"$CH/contrib/libmetrohash/src" -I"$CH/contrib/murmurhash/include"
  -I"$CH/contrib/cctz/include" -I"$CH/contrib/zmij"
)

CXXFLAGS=(
  --target=wasm32-wasip1 --sysroot="$WASI/share/wasi-sysroot"
  # -Oz rather than -Os: inlining is where most of the size goes - `std::string` and
  # `std::vector` operations are inlined into their callers rather than emitted as members, so
  # they do not show up in any per-symbol attribution. -Oz costs about 22% of parse throughput
  # (155 -> 190 microseconds for a mid-sized query) and saves 19% of the module, which is the
  # right way round for something downloaded once and then asked to parse one query at a time.
  -std=c++2b -Oz -DNDEBUG
  -D_LIBCPP_HAS_NO_THREADS
  -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_MMAN
  -D_WASI_EMULATED_PROCESS_CLOCKS -D_WASI_EMULATED_GETPID
  -DCLICKHOUSE_PARSER_MINIMAL_BUILD
  -DFMT_USE_LOCALE=0
  -fignore-exceptions
  # `setjmp`/`longjmp` is the error boundary around the parser - see wasm_runtime.cpp
  -mllvm -wasm-enable-sjlj
  -fno-asynchronous-unwind-tables
  # Link-time optimization, plus removal of virtual functions that nothing calls through a vtable.
  # `-fvirtual-function-elimination` needs full LTO, which is why the server build (ThinLTO) cannot
  # use it. Here the whole program is these 320 translation units, so it is available and cheap.
  -flto
  -fwhole-program-vtables
  -fvirtual-function-elimination
  -Wno-everything
)

if [ -n "$NO_FORMATTING" ]; then
    CXXFLAGS+=(-DCLICKHOUSE_PARSER_NO_FORMATTING)
fi

if [ -n "$NO_DCL" ]; then
    CXXFLAGS+=(-DCLICKHOUSE_PARSER_NO_DCL)
fi

# The transitive closure the parser actually needs. Everything not listed here is either
# unreachable or replaced by wasm_runtime.cpp - see the comments in that file.
mapfile -t SOURCES < <(
  ls "$CH"/src/Parsers/*.cpp "$CH"/src/Parsers/Access/*.cpp "$CH"/src/Access/Common/*.cpp
  cat <<EOF
$CH/src/Access/IAccessStorage.cpp
$CH/src/Common/Allocator.cpp
$CH/src/Common/AllocatorWithMemoryTracking.cpp
$CH/src/Common/CalendarTimeInterval.cpp
$CH/src/Common/ErrorCodes.cpp
$CH/src/Common/FieldVisitorConvertToNumber.cpp
$CH/src/Common/FieldVisitorDump.cpp
$CH/src/Common/FieldVisitorHash.cpp
$CH/src/Common/FieldVisitorToJSONElement.cpp
$CH/src/Common/FieldVisitorToString.cpp
$CH/src/Common/IntervalKind.cpp
$CH/src/Common/KnownObjectNames.cpp
$CH/src/Common/PODArray.cpp
$CH/src/Common/StringUtils.cpp
$CH/src/Common/UTF8Helpers.cpp
$CH/src/Common/ZooKeeper/ZooKeeperPathUtils.cpp
$CH/src/Common/checkStackSize.cpp
$CH/src/Common/formatIPv6.cpp
$CH/src/Common/formatReadable.cpp
$CH/src/Common/intExp.cpp
$CH/src/Common/iota.cpp
$CH/src/Common/likePatternToRegexp.cpp
$CH/src/Common/quoteString.cpp
$CH/src/Common/typeid_cast.cpp
$CH/src/Core/DecimalFunctions.cpp
$CH/src/Core/Field.cpp
$CH/src/Core/Joins.cpp
$CH/src/Core/QualifiedTableName.cpp
$CH/src/Core/Streaming/CursorTree.cpp
$CH/src/IO/DoubleConverter.cpp
$CH/src/IO/ReadBuffer.cpp
$CH/src/IO/ReadBufferFromMemory.cpp
$CH/src/IO/ReadHelpers.cpp
$CH/src/IO/SeekableReadBuffer.cpp
$CH/src/IO/WriteBuffer.cpp
$CH/src/IO/WriteBufferFromString.cpp
$CH/src/IO/WriteHelpers.cpp
$CH/src/IO/readFloatText.cpp
$CH/src/Interpreters/StorageID.cpp
$CH/src/Server/ServerType.cpp
$CH/base/base/Decimal.cpp
$CH/base/base/demangle.cpp
$CH/base/base/errnoToString.cpp
$CH/base/base/getPageSize.cpp
$CH/base/base/itoa.cpp
$CH/contrib/fmtlib/src/format.cc
$CH/contrib/zmij/zmij.cc
$CH/base/poco/Foundation/src/ASCIIEncoding.cpp
$CH/base/poco/Foundation/src/Ascii.cpp
$CH/base/poco/Foundation/src/AtomicCounter.cpp
$CH/base/poco/Foundation/src/Bugcheck.cpp
$CH/base/poco/Foundation/src/Channel.cpp
$CH/base/poco/Foundation/src/Configurable.cpp
$CH/base/poco/Foundation/src/DateTime.cpp
$CH/base/poco/Foundation/src/Debugger.cpp
$CH/base/poco/Foundation/src/DigestEngine.cpp
$CH/base/poco/Foundation/src/DigestStream.cpp
$CH/base/poco/Foundation/src/Exception.cpp
$CH/base/poco/Foundation/src/Format.cpp
$CH/base/poco/Foundation/src/Logger.cpp
$CH/base/poco/Foundation/src/LoggingRegistry.cpp
$CH/base/poco/Foundation/src/Message.cpp
$CH/base/poco/Foundation/src/Mutex.cpp
$CH/base/poco/Foundation/src/NumberFormatter.cpp
$CH/base/poco/Foundation/src/NumberParser.cpp
$CH/base/poco/Foundation/src/Path.cpp
$CH/base/poco/Foundation/src/RWLock.cpp
$CH/base/poco/Foundation/src/RefCountedObject.cpp
$CH/base/poco/Foundation/src/SHA1Engine.cpp
$CH/base/poco/Foundation/src/String.cpp
$CH/base/poco/Foundation/src/TextConverter.cpp
$CH/base/poco/Foundation/src/TextEncoding.cpp
$CH/base/poco/Foundation/src/TextIterator.cpp
$CH/base/poco/Foundation/src/Timestamp.cpp
$CH/base/poco/Foundation/src/UTF8Encoding.cpp
$CH/base/poco/Foundation/src/UTF8String.cpp
$CH/base/poco/Foundation/src/URI.cpp
$CH/base/poco/Net/src/IPAddress.cpp
$CH/base/poco/Net/src/IPAddressImpl.cpp
$CH/base/poco/Net/src/NetException.cpp
EOF
  ls "$CH"/contrib/double-conversion/double-conversion/*.cc
  echo "$HERE/wasm_parser.cpp"
  echo "$HERE/wasm_runtime.cpp"
)

echo "Compiling ${#SOURCES[@]} translation units..."
printf '%s\n' "${SOURCES[@]}" | xargs -P "$(getconf _NPROCESSORS_ONLN)" -I{} \
    bash -c 'obj="$1/$(echo "$2" | sed "s|/|__|g; s|\.cpp$|.o|; s|\.cc$|.o|")"; shift 2; "$@" -c "$0" -o "$obj"' \
    {} "$OUT/obj" {} "$CXX" "${CXXFLAGS[@]}" "${INCS[@]}" 2>&1 | grep -E 'error:' | head -20 || true

EXPORTS=(--export=ch_features --export=ch_check --export=ch_alloc --export=ch_free --export=ch_result_data --export=ch_result_size)
if [ -z "$NO_FORMATTING" ]; then
    EXPORTS+=(--export=ch_format)
fi

echo "Linking..."
"$CXX" "${CXXFLAGS[@]}" \
    -Wl,--no-entry -mexec-model=reactor -Wl,--strip-all \
    "${EXPORTS[@]/#/-Wl,}" \
    "$OUT"/obj/*.o -lsetjmp -lwasi-emulated-signal -lwasi-emulated-mman \
    -o "$OUT/parser.wasm"

printf '%s: %s bytes (%s gzipped)\n' "$OUT/parser.wasm" \
    "$(stat -c%s "$OUT/parser.wasm")" "$(gzip -9 -c "$OUT/parser.wasm" | wc -c)"
