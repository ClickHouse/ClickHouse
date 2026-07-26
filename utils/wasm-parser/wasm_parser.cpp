/// Entry point for the standalone WebAssembly build of the ClickHouse SQL parser.
///
/// The exported interface is deliberately tiny and C-like, so that it can be driven from
/// JavaScript without any glue-code generator:
///
///   uint8_t * ch_alloc(uint32_t size)          - allocate a buffer to write the query into
///   void      ch_free(uint8_t * ptr)           - release it
///   int       ch_format(const char *, uint32_t, int one_line)
///                                              - parse, then format; 1 = ok, 0 = parse error
///   const char * ch_result_data()              - result (formatted query, or the error message)
///   uint32_t  ch_result_size()

#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

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

}

extern "C"
{

uint8_t * ch_alloc(uint32_t size)
{
    return static_cast<uint8_t *>(::operator new(size, std::nothrow));
}

void ch_free(uint8_t * ptr)
{
    ::operator delete(ptr);
}

int ch_format(const char * query, uint32_t size, int one_line)
{
    try
    {
        DB::ParserQuery parser(query + size);
        DB::ASTPtr ast = DB::parseQuery(parser, query, query + size, "query", MAX_QUERY_SIZE, MAX_PARSER_DEPTH, MAX_PARSER_BACKTRACKS);
        result() = one_line ? ast->formatWithSecretsOneLine() : ast->formatWithSecretsMultiLine();
        return 1;
    }
    catch (const DB::Exception & e)
    {
        result() = e.message();
        return 0;
    }
    catch (const std::exception & e)
    {
        result() = e.what();
        return 0;
    }
}

const char * ch_result_data()
{
    return result().data();
}

uint32_t ch_result_size()
{
    return static_cast<uint32_t>(result().size());
}

}
