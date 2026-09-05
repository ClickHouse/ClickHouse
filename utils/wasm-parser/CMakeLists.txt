# Standalone WebAssembly build of the ClickHouse SQL parser. See README.md.
#
# This is a project of its own rather than a part of the main build: it cross-compiles to
# `wasm32-wasip1` with a wasi-sdk toolchain, which cannot be mixed into a tree configured for the
# host. Configure it separately, pointing at the toolchain file the SDK ships:
#
#   cmake -S utils/wasm-parser -B build-wasm -G Ninja \
#       -DCMAKE_TOOLCHAIN_FILE=$WASI_SDK/share/cmake/wasi-sdk-p1.cmake \
#       -DWASI_SDK_PREFIX=$WASI_SDK
#   cmake --build build-wasm
#   ctest --test-dir build-wasm --output-on-failure

cmake_minimum_required (VERSION 3.24)

project (clickhouse-wasm-parser LANGUAGES C CXX)

if (NOT CMAKE_SYSTEM_NAME STREQUAL "WASI")
    message (FATAL_ERROR
        "This project only builds for WebAssembly. Configure it with the toolchain file from a "
        "wasi-sdk: -DCMAKE_TOOLCHAIN_FILE=<wasi-sdk>/share/cmake/wasi-sdk-p1.cmake")
endif ()

set (CH "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "The ClickHouse source tree")
cmake_path (NORMAL_PATH CH)

option (ENABLE_FORMATTING "Export `ch_format`, which turns a parsed query back into SQL" ON)
option (ENABLE_DCL "Accept access management - `CREATE USER`, `GRANT` and the rest" ON)

# Asserted by the `parser-size` test. A build that legitimately grows past its ceiling should have
# the number here raised in the same change, deliberately and visibly - these are what a browser
# downloads. Each is about 8% over the corresponding measurement in README.md.
set (MAX_SIZE_FULL              2220000 CACHE STRING "Byte ceiling for the default build")
set (MAX_SIZE_NO_DCL            2000000 CACHE STRING "Byte ceiling for -DENABLE_DCL=OFF")
set (MAX_SIZE_NO_FORMATTING     1065000 CACHE STRING "Byte ceiling for -DENABLE_FORMATTING=OFF")
set (MAX_SIZE_NO_FORMATTING_DCL  875000 CACHE STRING "Byte ceiling for both off")

if (ENABLE_FORMATTING AND ENABLE_DCL)
    set (MAX_SIZE ${MAX_SIZE_FULL})
elseif (ENABLE_FORMATTING)
    set (MAX_SIZE ${MAX_SIZE_NO_DCL})
elseif (ENABLE_DCL)
    set (MAX_SIZE ${MAX_SIZE_NO_FORMATTING})
else ()
    set (MAX_SIZE ${MAX_SIZE_NO_FORMATTING_DCL})
endif ()

# All optional features are off: this build has no compression, no formats, no network. Every
# `#cmakedefine01` in the template becomes `#define ... 0`, because none of the variables it names
# is set here.
configure_file ("${CH}/src/Common/config.h.in" "${CMAKE_BINARY_DIR}/gen/Common/config.h")

# The transitive closure the parser actually needs. Everything not listed here is either
# unreachable or replaced by wasm_runtime.cpp - see the comments in that file. A source that goes
# missing from this list surfaces as an undefined symbol at link time, which is what the
# `Build (wasm_parser)` CI job is for.
file (GLOB PARSER_SOURCES CONFIGURE_DEPENDS
    "${CH}/src/Parsers/*.cpp"
    "${CH}/src/Parsers/Access/*.cpp"
    "${CH}/src/Access/Common/*.cpp"
)

set (SUPPORT_SOURCES
    "${CH}/src/Access/IAccessStorage.cpp"
    "${CH}/src/Access/MaskingPolicy.cpp"
    "${CH}/src/Common/Allocator.cpp"
    "${CH}/src/Common/AllocatorWithMemoryTracking.cpp"
    "${CH}/src/Common/CalendarTimeInterval.cpp"
    "${CH}/src/Common/ErrorCodes.cpp"
    "${CH}/src/Common/FiberLocal.cpp"
    "${CH}/src/Common/FieldVisitorConvertToNumber.cpp"
    "${CH}/src/Common/FieldVisitorDump.cpp"
    "${CH}/src/Common/FieldVisitorHash.cpp"
    "${CH}/src/Common/FieldVisitorToJSONElement.cpp"
    "${CH}/src/Common/FieldVisitorToString.cpp"
    "${CH}/src/Common/IntervalKind.cpp"
    "${CH}/src/Common/KnownObjectNames.cpp"
    "${CH}/src/Common/PODArray.cpp"
    "${CH}/src/Common/StackfulCoroutine.cpp"
    "${CH}/src/Common/StringUtils.cpp"
    "${CH}/src/Common/UTF8Helpers.cpp"
    "${CH}/src/Common/ZooKeeper/ZooKeeperPathUtils.cpp"
    "${CH}/src/Common/checkStackSize.cpp"
    "${CH}/src/Common/formatIPv6.cpp"
    "${CH}/src/Common/formatReadable.cpp"
    "${CH}/src/Common/intExp.cpp"
    "${CH}/src/Common/iota.cpp"
    "${CH}/src/Common/likePatternToRegexp.cpp"
    "${CH}/src/Common/quoteString.cpp"
    "${CH}/src/Common/typeid_cast.cpp"
    "${CH}/src/Core/DecimalFunctions.cpp"
    "${CH}/src/Core/Field.cpp"
    "${CH}/src/Core/Joins.cpp"
    "${CH}/src/Core/QualifiedTableName.cpp"
    "${CH}/src/Core/Streaming/CursorTree.cpp"
    "${CH}/src/Core/Streaming/Settings.cpp"
    "${CH}/src/IO/DoubleConverter.cpp"
    "${CH}/src/IO/ReadBuffer.cpp"
    "${CH}/src/IO/ReadBufferFromMemory.cpp"
    "${CH}/src/IO/ReadHelpers.cpp"
    "${CH}/src/IO/SeekableReadBuffer.cpp"
    "${CH}/src/IO/WriteBuffer.cpp"
    "${CH}/src/IO/WriteBufferFromString.cpp"
    "${CH}/src/IO/WriteHelpers.cpp"
    "${CH}/src/IO/readFloatText.cpp"
    "${CH}/src/Interpreters/StorageID.cpp"
    "${CH}/src/Server/ServerType.cpp"
    "${CH}/base/base/Decimal.cpp"
    "${CH}/base/base/demangle.cpp"
    "${CH}/base/base/errnoToString.cpp"
    "${CH}/base/base/getPageSize.cpp"
    "${CH}/base/base/IPv4andIPv6.cpp"
    "${CH}/base/base/itoa.cpp"
    "${CH}/contrib/fmtlib/src/format.cc"
    "${CH}/contrib/zmij/zmij.cc"
    "${CH}/base/poco/Foundation/src/ASCIIEncoding.cpp"
    "${CH}/base/poco/Foundation/src/Ascii.cpp"
    "${CH}/base/poco/Foundation/src/AtomicCounter.cpp"
    "${CH}/base/poco/Foundation/src/Bugcheck.cpp"
    "${CH}/base/poco/Foundation/src/Channel.cpp"
    "${CH}/base/poco/Foundation/src/Configurable.cpp"
    "${CH}/base/poco/Foundation/src/DateTime.cpp"
    "${CH}/base/poco/Foundation/src/Debugger.cpp"
    "${CH}/base/poco/Foundation/src/DigestEngine.cpp"
    "${CH}/base/poco/Foundation/src/DigestStream.cpp"
    "${CH}/base/poco/Foundation/src/Exception.cpp"
    "${CH}/base/poco/Foundation/src/Format.cpp"
    "${CH}/base/poco/Foundation/src/Logger.cpp"
    "${CH}/base/poco/Foundation/src/LoggingRegistry.cpp"
    "${CH}/base/poco/Foundation/src/Message.cpp"
    "${CH}/base/poco/Foundation/src/Mutex.cpp"
    "${CH}/base/poco/Foundation/src/NumberFormatter.cpp"
    "${CH}/base/poco/Foundation/src/NumberParser.cpp"
    "${CH}/base/poco/Foundation/src/RWLock.cpp"
    "${CH}/base/poco/Foundation/src/RefCountedObject.cpp"
    "${CH}/base/poco/Foundation/src/SHA1Engine.cpp"
    "${CH}/base/poco/Foundation/src/String.cpp"
    "${CH}/base/poco/Foundation/src/TextConverter.cpp"
    "${CH}/base/poco/Foundation/src/TextEncoding.cpp"
    "${CH}/base/poco/Foundation/src/TextIterator.cpp"
    "${CH}/base/poco/Foundation/src/Timestamp.cpp"
    "${CH}/base/poco/Foundation/src/UTF8Encoding.cpp"
    "${CH}/base/poco/Foundation/src/UTF8String.cpp"
    "${CH}/base/poco/Foundation/src/URI.cpp"
    "${CH}/base/poco/Net/src/IPAddress.cpp"
    "${CH}/base/poco/Net/src/IPAddressImpl.cpp"
    "${CH}/base/poco/Net/src/NetException.cpp"
)

file (GLOB DOUBLE_CONVERSION_SOURCES CONFIGURE_DEPENDS
    "${CH}/contrib/double-conversion/double-conversion/*.cc")

# `ch_format_json` deserializes AST JSON through `IAST::createFromJSON`, which reads the document
# with `Poco::JSON::Parser` into `Poco::Dynamic::Var`s - the one place in the module that needs
# them. Only formatting builds export it, so only they carry these. `pdjson.c` is C and therefore
# outside the LTO unit, like `wasm_sjlj.c`; unlike it, nothing in pdjson interacts with setjmp or
# vtables, so that placement is harmless.
if (ENABLE_FORMATTING)
    set (AST_JSON_SOURCES
        "${CH}/base/poco/JSON/src/Array.cpp"
        "${CH}/base/poco/JSON/src/Handler.cpp"
        "${CH}/base/poco/JSON/src/JSONException.cpp"
        "${CH}/base/poco/JSON/src/Object.cpp"
        "${CH}/base/poco/JSON/src/ParseHandler.cpp"
        "${CH}/base/poco/JSON/src/Parser.cpp"
        "${CH}/base/poco/JSON/src/ParserImpl.cpp"
        "${CH}/base/poco/JSON/src/Stringifier.cpp"
        "${CH}/base/poco/JSON/src/pdjson.c"
        "${CH}/base/poco/Foundation/src/JSONString.cpp"
        "${CH}/base/poco/Foundation/src/Var.cpp"
        "${CH}/base/poco/Foundation/src/VarHolder.cpp"
        "${CH}/base/poco/Foundation/src/VarIterator.cpp"
        # What `Poco::Dynamic::Var` reaches for conversions: number-to-string formatting, and the
        # date-time types its holders can convert through. `Timezone` is deliberately absent:
        # its UNIX implementation reads `tzset`/`tzname`, which wasi-libc does not have, so
        # `wasm_runtime.cpp` answers UTC instead.
        "${CH}/base/poco/Foundation/src/DateTimeFormat.cpp"
        "${CH}/base/poco/Foundation/src/NumericString.cpp"
        "${CH}/base/poco/Foundation/src/LocalDateTime.cpp"
        "${CH}/base/poco/Foundation/src/Timespan.cpp"
    )
else ()
    set (AST_JSON_SOURCES)
endif ()

add_executable (parser
    ${PARSER_SOURCES}
    ${SUPPORT_SOURCES}
    ${DOUBLE_CONVERSION_SOURCES}
    ${AST_JSON_SOURCES}
    wasm_parser.cpp
    wasm_runtime.cpp
    wasm_sjlj.c
)

set_target_properties (parser PROPERTIES OUTPUT_NAME parser SUFFIX .wasm)

target_include_directories (parser SYSTEM PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/shim"
    "${CMAKE_BINARY_DIR}/gen"
    "${CMAKE_BINARY_DIR}/gen/Common"
    "${CH}/src"
    "${CH}/base"
    "${CH}/base/poco/Foundation/include"
    "${CH}/base/poco/Util/include"
    "${CH}/base/poco/Net/include"
    "${CH}/base/poco/JSON/include"
    "${CH}/base/poco/XML/include"
    "${CH}/base/pcg-random"
    "${CH}/base/widechar_width"
    "${CH}/contrib/fmtlib/include"
    "${CH}/contrib/abseil-cpp"
    "${CH}/contrib/boost"
    "${CH}/contrib/magic_enum/include/magic_enum"
    "${CH}/contrib/cityhash102/include"
    "${CH}/contrib/libdivide"
    "${CH}/contrib/miniselect/include"
    "${CH}/contrib/pdqsort"
    "${CH}/contrib/ipnsort"
    "${CH}/contrib/driftsort"
    "${CH}/contrib/wyhash"
    "${CH}/contrib/xxHash"
    "${CH}/contrib/double-conversion"
    "${CH}/contrib/fast_float/include"
    "${CH}/contrib/re2"
    "${CH}/contrib/sparsehash-c11"
    "${CH}/contrib/croaring/include"
    "${CH}/contrib/libmetrohash/src"
    "${CH}/contrib/murmurhash/include"
    "${CH}/contrib/cctz/include"
    "${CH}/contrib/zmij"
)

target_compile_definitions (parser PRIVATE
    NDEBUG
    _LIBCPP_HAS_NO_THREADS
    _WASI_EMULATED_SIGNAL
    _WASI_EMULATED_MMAN
    _WASI_EMULATED_PROCESS_CLOCKS
    _WASI_EMULATED_GETPID
    CLICKHOUSE_PARSER_MINIMAL_BUILD
    FMT_USE_LOCALE=0
    # `NumericString.cpp` spells `std::auto_ptr` without it; the server build gets it from
    # `base/poco/Foundation/CMakeLists.txt`.
    POCO_ENABLE_CPP11
    $<$<NOT:$<BOOL:${ENABLE_FORMATTING}>>:CLICKHOUSE_PARSER_NO_FORMATTING>
    $<$<NOT:$<BOOL:${ENABLE_DCL}>>:CLICKHOUSE_PARSER_NO_DCL>
)

# -Oz rather than -Os: inlining is where most of the size goes - `std::string` and `std::vector`
# operations are inlined into their callers rather than emitted as members, so they do not show up
# in any per-symbol attribution. -Oz costs about 22% of parse throughput (155 -> 190 microseconds
# for a mid-sized query) and saves 19% of the module, which is the right way round for something
# downloaded once and then asked to parse one query at a time.
#
# Link-time optimization, plus removal of virtual functions that nothing calls through a vtable.
# `-fvirtual-function-elimination` needs full LTO, which is why the server build (ThinLTO) cannot
# use it. Here the whole program is these translation units, so it is available and cheap.
target_compile_options (parser PRIVATE
    -Oz
    -fno-asynchronous-unwind-tables
    -Wno-everything
    $<$<COMPILE_LANGUAGE:CXX>:-std=c++2b>
    $<$<COMPILE_LANGUAGE:CXX>:-fignore-exceptions>
    $<$<COMPILE_LANGUAGE:CXX>:-flto>
    $<$<COMPILE_LANGUAGE:CXX>:-fwhole-program-vtables>
    $<$<COMPILE_LANGUAGE:CXX>:-fvirtual-function-elimination>
)

# The one file outside the LTO unit, and the only one that may call `setjmp`; see wasm_sjlj.c.
set_source_files_properties (wasm_sjlj.c PROPERTIES COMPILE_OPTIONS "-mllvm;-wasm-enable-sjlj")

set (EXPORTS ch_features ch_parse ch_alloc ch_free ch_result_data ch_result_size)
if (ENABLE_FORMATTING)
    list (APPEND EXPORTS ch_format ch_format_json)
endif ()
list (TRANSFORM EXPORTS PREPEND "-Wl,--export=")

# `-mexec-model=reactor` because the module is a library, not a program: it is initialized once and
# then called into. Note that `-mllvm -wasm-enable-sjlj` is deliberately absent here - the LTO unit
# must contain no `setjmp`, and leaving the flag off turns a stray one into a link error.
target_link_options (parser PRIVATE
    -Oz
    -flto
    -fwhole-program-vtables
    -fvirtual-function-elimination
    -mexec-model=reactor
    -Wl,--no-entry
    -Wl,--strip-all
    ${EXPORTS}
)

target_link_libraries (parser PRIVATE setjmp wasi-emulated-signal wasi-emulated-mman)

add_custom_command (TARGET parser POST_BUILD
    COMMAND "${CMAKE_COMMAND}"
        "-DWASM=$<TARGET_FILE:parser>" -DMAX_SIZE=${MAX_SIZE} -DREPORT_ONLY=ON
        -P "${CMAKE_CURRENT_SOURCE_DIR}/check_size.cmake"
    VERBATIM)

enable_testing ()

find_program (NODE NAMES node nodejs)
if (NODE)
    add_test (NAME parser-cases
        COMMAND "${NODE}" "${CMAKE_CURRENT_SOURCE_DIR}/test.mjs" "$<TARGET_FILE:parser>")
else ()
    # Still `parser-cases`, still failing: without Node.js the behavioral tests cannot run, and
    # `ctest` must say so rather than quietly narrow itself to `parser-size` - the throw-recovery
    # cases test.mjs drives are exactly what regressed under LTO. Building without testing
    # remains possible; a green `ctest` without Node.js does not.
    add_test (NAME parser-cases
        COMMAND "${CMAKE_COMMAND}" -E echo "parser-cases needs Node.js, and node was not found")
    set_tests_properties (parser-cases PROPERTIES WILL_FAIL ON)
    message (WARNING "node was not found, so the `parser-cases` test will fail instead of running")
endif ()

add_test (NAME parser-size
    COMMAND "${CMAKE_COMMAND}"
        "-DWASM=$<TARGET_FILE:parser>" -DMAX_SIZE=${MAX_SIZE}
        -P "${CMAKE_CURRENT_SOURCE_DIR}/check_size.cmake")
