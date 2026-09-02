/// Entry point for the standalone WebAssembly build of the ClickHouse SQL parser.
///
/// The exported interface is deliberately tiny and C-like, so that it can be driven from
/// JavaScript without any glue-code generator:
///
///   uint32_t  ch_features()                    - what this build can do, see CH_FEATURE_* below
///   uint8_t * ch_alloc(uint32_t size)          - allocate a buffer to write the query into
///   void      ch_free(uint8_t * ptr)           - release it
///   int       ch_check(const char *, uint32_t) - parse; 1 = ok, 0 = syntax error
///   int       ch_format(const char *, uint32_t, int one_line)
///                                              - parse, then format; 1 = ok, 0 = parse error
///   const char * ch_result_data()              - result (formatted query, or the error message)
///   uint32_t  ch_result_size()
///
/// `ch_format` is absent from a `--no-formatting` build, which only checks that a query parses.

#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <wasm_sjlj.h>

#include <cstdint>
#include <string>
#include <new>

namespace
{

std::string & result()
{
    static std::string value;
    return value;
}

constexpr size_t MAX_QUERY_SIZE = 1u << 20;
constexpr size_t MAX_PARSER_DEPTH = 1000;
constexpr size_t MAX_PARSER_BACKTRACKS = 1000000;

/// `tryParseQuery` reports a syntax error by returning null and filling in the message, and
/// nothing in `src/Parsers` catches. A few checks in the parser still report an invalid query by
/// throwing, and this build has no unwinding, so `wasm_runtime.cpp` turns such a throw into a jump
/// back to the boundary in `wasm_sjlj.c` - see the comment on `__cxa_throw` there.
DB::ASTPtr parse(const char * query, uint32_t size, std::string & error)
{
    const char * end = query + size;
    DB::ParserQuery parser(end);
    return DB::tryParseQuery(
        parser, query, end, error, /*hilite=*/false, "query", /*allow_multi_statements=*/false,
        MAX_QUERY_SIZE, MAX_PARSER_DEPTH, MAX_PARSER_BACKTRACKS, /*skip_insignificant=*/true);
}

/// What a `chParserProtectedCall` body is handed. `one_line` is only read by `formatBody`.
struct Request
{
    const char * query;
    uint32_t size;
    int one_line;
};

/// The bodies run below the boundary, so they answer exactly what `ch_check` and `ch_format`
/// answer - 1 for a query that parses, 0 for one that does not - and leave the throwing case,
/// which does not return here at all, to their callers.
extern "C" int checkBody(void * argument)
{
    const auto & request = *static_cast<const Request *>(argument);

    std::string error;
    if (!parse(request.query, request.size, error))
    {
        result() = std::move(error);
        return 0;
    }

    result().clear();
    return 1;
}

#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
extern "C" int formatBody(void * argument)
{
    const auto & request = *static_cast<const Request *>(argument);

    std::string error;
    DB::ASTPtr ast = parse(request.query, request.size, error);

    if (!ast)
    {
        result() = std::move(error);
        return 0;
    }

    /// Formatting throws for an AST the parser accepted but cannot print, so it stays inside the
    /// boundary too.
    result() = request.one_line ? ast->formatWithSecretsOneLine() : ast->formatWithSecretsMultiLine();
    return 1;
}
#endif

/// Turns the boundary's `CH_PARSER_THREW` into the same answer a syntax error gets, with the
/// message of the exception that ended the call.
int finish(int protected_call_result)
{
    if (protected_call_result == CH_PARSER_THREW)
    {
        result() = chParserRecoveryMessage();
        return 0;
    }

    return protected_call_result;
}

}

extern "C"
{

/** What the module was built with. The build leaves out whole classes of work - see
  * `CMakeLists.txt` - and a caller that has to cope with more than one build should ask rather
  * than guess.
  */
enum : uint32_t
{
    CH_FEATURE_FORMAT = 1,  /// `ch_format` is exported
    CH_FEATURE_DCL = 2,     /// `CREATE USER`, `GRANT` and the rest of access management parse
};

uint32_t ch_features()
{
    uint32_t features = 0;
#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
    features |= CH_FEATURE_FORMAT;
#endif
#if !defined(CLICKHOUSE_PARSER_NO_DCL)
    features |= CH_FEATURE_DCL;
#endif
    return features;
}

uint8_t * ch_alloc(uint32_t size)
{
    return static_cast<uint8_t *>(::operator new(size, std::nothrow));
}

void ch_free(uint8_t * ptr)
{
    ::operator delete(ptr);
}

int ch_check(const char * query, uint32_t size)
{
    Request request{query, size, /*one_line=*/0};
    return finish(chParserProtectedCall(checkBody, &request));
}

#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
int ch_format(const char * query, uint32_t size, int one_line)
{
    Request request{query, size, one_line};
    return finish(chParserProtectedCall(formatBody, &request));
}
#endif

const char * ch_result_data()
{
    return result().data();
}

uint32_t ch_result_size()
{
    return static_cast<uint32_t>(result().size());
}

}
