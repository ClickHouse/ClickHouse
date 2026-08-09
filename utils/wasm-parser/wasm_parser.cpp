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

#include <csetjmp>
#include <cstdint>
#include <string>
#include <new>

extern "C"
{
    /// The error boundary; defined in `wasm_runtime.cpp`.
    jmp_buf * chParserRecoveryPoint();
    void chParserArmRecovery(bool armed);
    const char * chParserRecoveryMessage();
}

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
/// back to the `setjmp` boundary the caller armed - see the comment on `__cxa_throw` there.
DB::ASTPtr parse(const char * query, uint32_t size, std::string & error)
{
    const char * end = query + size;
    DB::ParserQuery parser(end);
    return DB::tryParseQuery(
        parser, query, end, error, /*hilite=*/false, "query", /*allow_multi_statements=*/false,
        MAX_QUERY_SIZE, MAX_PARSER_DEPTH, MAX_PARSER_BACKTRACKS, /*skip_insignificant=*/true);
}

}

extern "C"
{

/** What the module was built with. The build leaves out whole classes of work - see `build.sh` -
  * and a caller that has to cope with more than one build should ask rather than guess.
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
    if (setjmp(*chParserRecoveryPoint()) != 0)
    {
        result() = chParserRecoveryMessage();
        return 0;
    }
    chParserArmRecovery(true);

    std::string error;
    if (!parse(query, size, error))
    {
        chParserArmRecovery(false);
        result() = std::move(error);
        return 0;
    }

    chParserArmRecovery(false);
    result().clear();
    return 1;
}

#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
int ch_format(const char * query, uint32_t size, int one_line)
{
    if (setjmp(*chParserRecoveryPoint()) != 0)
    {
        result() = chParserRecoveryMessage();
        return 0;
    }
    chParserArmRecovery(true);

    std::string error;
    DB::ASTPtr ast = parse(query, size, error);

    if (!ast)
    {
        chParserArmRecovery(false);
        result() = std::move(error);
        return 0;
    }

    /// Formatting throws for an AST the parser accepted but cannot print, so it stays inside the
    /// boundary too.
    result() = one_line ? ast->formatWithSecretsOneLine() : ast->formatWithSecretsMultiLine();
    chParserArmRecovery(false);
    return 1;
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
