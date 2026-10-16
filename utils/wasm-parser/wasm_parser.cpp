/// Entry point for the standalone WebAssembly build of the ClickHouse SQL parser.
///
/// The exported interface is deliberately tiny and C-like, so that it can be driven from
/// JavaScript without any glue-code generator:
///
///   uint32_t  ch_features()                    - what this build can do, see CH_FEATURE_* below
///   uint8_t * ch_alloc(uint32_t size)          - allocate a buffer to write the query into
///   void      ch_free(uint8_t * ptr)           - release it
///   int       ch_parse(const char *, uint32_t) - parse; 1 = ok, 0 = error, and the result is
///                                                always a JSON document with the AST, the
///                                                highlights, and the error
///   int       ch_format(const char *, uint32_t, int one_line)
///                                              - parse, then format; 1 = ok, 0 = parse error
///   int       ch_format_json(const char *, uint32_t, int one_line)
///                                              - take AST JSON (the "ast" object of `ch_parse`,
///                                                or `parseQueryToJSON` on a server) and format
///                                                it back into SQL; 1 = ok, 0 = error
///   const char * ch_result_data()              - result (formatted query, JSON document, or the
///                                                error message)
///   uint32_t  ch_result_size()
///
/// `ch_format` and `ch_format_json` are absent from a `--no-formatting` build, and `ch_parse`
/// then reports no "ast" (see below).
///
/// The `ch_parse` result is UTF-8 JSON, in one of two shapes. For a query that parses:
///
///   {"ast": {...}, "highlights": [{"begin": 0, "end": 6, "type": "keyword"}, ...]}
///
/// where "ast" is the same JSON the SQL function `parseQueryToJSON` produces (round-trippable
/// through `ch_format_json` and the server's `formatQueryFromJSON`). A query can parse and still
/// have no JSON representation - `GRANT`, or `INSERT` with inline data - and then "ast" is null
/// and "ast_error" says why. A `--no-formatting` build omits "ast" entirely, because the strings
/// such a build stores in the tree (`CAST` types, for one) are the user's spelling rather than
/// the canonical one. For a query that does not parse:
///
///   {"error": {"message": "...", "begin": 7, "end": 8, "line": 1, "column": 8,
///              "expected": ["..."]}, "highlights": [...]}
///
/// "begin"/"end" are 0-based byte offsets of the offending token, "line"/"column" are 1-based
/// (column in bytes), all omitted when unknown (an error reported by throwing has no token);
/// "expected" lists what the parser would have accepted there, omitted when there is nothing to
/// say. "highlights" covers what parsed before the error, so an editor can keep coloring while
/// the user types. All highlight offsets are byte offsets, end-exclusive; the types are the
/// visible names of `enum Highlight` (`keyword`, `identifier`, `function`, `alias`,
/// `substitution`, `number`, `string`, `string_escape`, `string_metacharacter`).

#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
#include <Parsers/ASTToJSON.h>
#endif

#include <wasm_sjlj.h>

#include <Common/Exception.h>

#include <cstdint>
#include <string>
#include <new>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int TOO_BIG_AST;
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
/// What the server defaults `max_ast_elements` to; bounds `ch_format_json` deserialization.
constexpr size_t MAX_AST_ELEMENTS = 50000;

/// `tryParseQuery` reports a syntax error by returning null and filling in the message, and
/// nothing in `src/Parsers` catches. A few checks in the parser still report an invalid query by
/// throwing, and this build has no unwinding, so `wasm_runtime.cpp` turns such a throw into a jump
/// back to the boundary in `wasm_sjlj.c` - see the comment on `__cxa_throw` there.
DB::ASTPtr parse(const char * query, uint32_t size, std::string & error, DB::ParserDiagnostics * diagnostics = nullptr)
{
    const char * end = query + size;
    DB::ParserQuery parser(end);
    return DB::tryParseQuery(
        parser, query, end, error, /*hilite=*/false, "query", /*allow_multi_statements=*/false,
        MAX_QUERY_SIZE, MAX_PARSER_DEPTH, MAX_PARSER_BACKTRACKS, /*skip_insignificant=*/true, diagnostics);
}

void writeJSONText(std::string_view text, DB::WriteBuffer & out)
{
    static const DB::FormatSettings format_settings;
    DB::writeJSONString(text, out, format_settings);
}

/// The publicly visible highlight names. `string_like` and `string_regexp` do not survive
/// `expandHighlights`, and `none` marks nothing; all three answer null and are not emitted.
const char * highlightName(DB::Highlight type)
{
    if (type == DB::Highlight::none)
        return nullptr;

    switch (type)
    {
#define M(NAME) case DB::Highlight::NAME: return #NAME;
        APPLY_FOR_HIGHLIGHTS(M)
#undef M
        default:
            return nullptr;
    }
}

/// Both 1-based; the column is in bytes, as in the "(line N, col M)" of server error messages.
std::pair<size_t, size_t> lineAndColumn(const char * begin, const char * pos)
{
    size_t line = 1;
    const char * line_begin = begin;
    for (const char * c = begin; c < pos; ++c)
    {
        if (*c == '\n')
        {
            ++line;
            line_begin = c + 1;
        }
    }
    return {line, static_cast<size_t>(pos - line_begin) + 1};
}

void writeHighlights(const DB::ParserDiagnostics & diagnostics, const char * query_begin, DB::WriteBuffer & out)
{
    out << "\"highlights\":[";
    bool first = true;
    for (const auto & range : DB::expandHighlights(diagnostics.expected.highlights))
    {
        const char * name = highlightName(range.highlight);
        if (!name)
            continue;
        if (!first)
            out << ',';
        first = false;
        out << "{\"begin\":" << static_cast<UInt64>(range.begin - query_begin)
            << ",\"end\":" << static_cast<UInt64>(range.end - query_begin)
            << ",\"type\":\"" << name << "\"}";
    }
    out << ']';
}

#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
/// What a `chParserProtectedCall` body is handed. `one_line` is only read by `formatBody`.
struct Request
{
    const char * query;
    uint32_t size;
    int one_line;
};

/// The bodies run below the boundary, so they answer exactly what their entry points answer -
/// 1 for a query that parses and formats, 0 for one that does not - and leave the throwing case,
/// which does not return here at all, to their callers.
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

/// Every AST JSON document this module reads passes through here, `serializeBody` included: it is
/// what "the limits of `ch_format_json`" means, in one place, so that the producer can be held to
/// them by running them rather than by restating them.
DB::ASTPtr readASTJSON(const char * json, size_t size)
{
    /// `createFromJSON` bounds the AST it builds, but `Poco::JSON::Parser` materializes the whole
    /// document first, so the raw text is bounded up front, as `formatQueryFromJSON` does on the
    /// server with `max_query_size`.
    if (size > MAX_QUERY_SIZE)
        throw DB::Exception(DB::ErrorCodes::TOO_BIG_AST, "AST JSON is too big. Maximum: {}", MAX_QUERY_SIZE);

    /// Deserialization throws for anything wrong with the document - malformed JSON, an unknown
    /// node type, a field of the wrong shape, a tree past the depth or element limits - and the
    /// boundary turns that into the error message.
    DB::ASTPtr ast = DB::IAST::createFromJSON(std::string(json, size), MAX_PARSER_DEPTH, MAX_AST_ELEMENTS);

    /// `createFromJSON` counts what it builds as it goes, but a `readJSON` override can materialize
    /// nodes after that pass, so the assembled tree is measured once more - the same second pass
    /// `formatQueryFromJSON` makes on the server - before anything walks it.
    ast->checkDepth(MAX_PARSER_DEPTH);
    ast->checkSize(MAX_AST_ELEMENTS);

    return ast;
}

extern "C" int formatJSONBody(void * argument)
{
    const auto & request = *static_cast<const Request *>(argument);

    DB::ASTPtr ast = readASTJSON(request.query, request.size);

    result() = request.one_line ? ast->formatWithSecretsOneLine() : ast->formatWithSecretsMultiLine();
    return 1;
}
#endif

/// What `ch_parse` hands below the boundary. The outputs live in the caller's frame, above the
/// boundary, so whatever was filled in before a throw - the highlights above all - survives it.
struct ParseRequest
{
    const char * query;
    uint32_t size;
    DB::ParserDiagnostics * diagnostics;
    std::string * error;
    DB::ASTPtr * ast;
};

extern "C" int parseBody(void * argument)
{
    const auto & request = *static_cast<const ParseRequest *>(argument);
    *request.ast = parse(request.query, request.size, *request.error, request.diagnostics);
    return *request.ast ? 1 : 0;
}

#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
struct SerializeRequest
{
    const DB::IAST * ast;
    std::string * json;
};

/// Its own trip below the boundary: `writeJSON` is fail-closed and throws for a node with no
/// faithful JSON representation (access management, `INSERT` with inline data), and that must
/// come back as a null "ast" with a reason, not as a failure of the whole call.
extern "C" int serializeBody(void * argument)
{
    const auto & request = *static_cast<const SerializeRequest *>(argument);

    /// Only an AST that `ch_format_json` will read back is worth reporting. The node and depth
    /// budgets are checked here first, so a tree that cannot fit is turned away before it is
    /// serialized at all.
    request.ast->checkDepth(MAX_PARSER_DEPTH);
    request.ast->checkSize(MAX_AST_ELEMENTS);

    *request.json = DB::serializeASTToJSON(*request.ast);

    /// They are not the whole of what the consumer requires, though: its element budget counts more
    /// than AST nodes - every value of a structured `Field` inside a single `ASTLiteral`, the
    /// elements of a `SET`, the entries of a `RENAME`, and so on - so a tree of 49999 nodes with one
    /// `[0]` in it fits here and does not fit there. Restating those rules on this side would be a
    /// second implementation of them, free to drift; the document is read back instead, exactly as
    /// `ch_format_json` reads it. The tree it builds is discarded - that it builds at all is the
    /// answer - and one more pass over at most 1 MiB is what the guarantee costs. A tree that
    /// parsed but does not survive the round trip answers a null "ast" with the reason, the same way
    /// one with no JSON representation at all does.
    readASTJSON(request.json->data(), request.json->size());

    return 1;
}

/// Turns the boundary's `CH_PARSER_THREW` into the same answer a parse error gets, with the
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
#endif

}

extern "C"
{

/** What the module was built with. The build leaves out whole classes of work - see
  * `CMakeLists.txt` - and a caller that has to cope with more than one build should ask rather
  * than guess.
  */
enum : uint32_t
{
    CH_FEATURE_FORMAT = 1,    /// `ch_format` is exported
    CH_FEATURE_DCL = 2,       /// `CREATE USER`, `GRANT` and the rest of access management parse
    CH_FEATURE_AST_JSON = 4,  /// `ch_parse` reports an "ast", and `ch_format_json` is exported
};

uint32_t ch_features()
{
    uint32_t features = 0;
#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
    features |= CH_FEATURE_FORMAT | CH_FEATURE_AST_JSON;
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

#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
int ch_format(const char * query, uint32_t size, int one_line)
{
    Request request{query, size, one_line};
    return finish(chParserProtectedCall(formatBody, &request));
}

int ch_format_json(const char * json, uint32_t size, int one_line)
{
    Request request{json, size, one_line};
    return finish(chParserProtectedCall(formatJSONBody, &request));
}
#endif

int ch_parse(const char * query, uint32_t size)
{
    DB::ParserDiagnostics diagnostics;
    diagnostics.expected.enable_highlighting = true;

    std::string error;
    DB::ASTPtr ast;
    ParseRequest request{query, size, &diagnostics, &error, &ast};
    const int parsed = chParserProtectedCall(parseBody, &request);

    /// The envelope is assembled above the boundary, from parts that are all in hand by now.
    DB::WriteBufferFromOwnString out;
    out << '{';

    if (parsed == 1)
    {
#if !defined(CLICKHOUSE_PARSER_NO_FORMATTING)
        out << "\"ast\":";

        std::string ast_json;
        SerializeRequest serialize_request{ast.get(), &ast_json};
        if (chParserProtectedCall(serializeBody, &serialize_request) == 1)
        {
            out << ast_json;
        }
        else
        {
            out << "null,\"ast_error\":";
            writeJSONText(chParserRecoveryMessage(), out);
        }
        out << ',';
#endif
    }
    else
    {
        out << "\"error\":{\"message\":";
        writeJSONText(parsed == CH_PARSER_THREW ? std::string_view(chParserRecoveryMessage()) : std::string_view(error), out);

        /// A failure that came back through the boundary set no error token; the rightmost
        /// position the parser reached is the best that is known about where it happened.
        const char * error_begin = parsed == 0 ? diagnostics.error_token.begin : diagnostics.expected.max_parsed_pos;
        if (error_begin)
        {
            out << ",\"begin\":" << static_cast<UInt64>(error_begin - query);
            if (parsed == 0)
                out << ",\"end\":" << static_cast<UInt64>(diagnostics.error_token.end - query);
            const auto [line, column] = lineAndColumn(query, error_begin);
            out << ",\"line\":" << static_cast<UInt64>(line) << ",\"column\":" << static_cast<UInt64>(column);
        }

        if (!diagnostics.expected.variants.empty())
        {
            out << ",\"expected\":[";
            bool first = true;
            for (const char * variant : diagnostics.expected.variants)
            {
                if (!first)
                    out << ',';
                first = false;
                writeJSONText(variant, out);
            }
            out << ']';
        }

        out << "},";
    }

    writeHighlights(diagnostics, query, out);
    out << '}';

    result() = out.str();
    return parsed == 1;
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
