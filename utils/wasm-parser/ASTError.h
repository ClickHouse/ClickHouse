#pragma once

#include <Parsers/IAST.h>

#include <string>
#include <vector>


namespace DB
{

/// The limits `ch_parse` holds an `ast` to, as `MAX_PARSER_DEPTH` and `MAX_AST_ELEMENTS` in
/// `wasm_parser.cpp`, which checks that they agree. A partial tree past them is not captured.
constexpr size_t PARTIAL_AST_MAX_DEPTH = 1000;
constexpr size_t PARTIAL_AST_MAX_ELEMENTS = 50000;

/** The slot of a `partial_ast` (see `Parsers/PartialASTCapture.h`) that the parser failed in.
  *
  * Serialize-only: it has no SQL formatting, and `IAST::createFromJSON` does not know its type, so
  * `ch_format_json` rejects a document that contains one.
  */
class ASTError : public IAST
{
public:
    /// Where the parser stopped: `max_parsed_pos`, and the end of the token that starts there.
    /// `ch_parse` turns them into byte offsets with `setOffsets` once the query start is known.
    const char * begin_pos = nullptr;
    const char * end_pos = nullptr;

    UInt64 begin = 0;
    UInt64 end = 0;

    /// What the parser would have accepted at `begin_pos`.
    std::vector<std::string> expected;

    String getID(char) const override { return "Error"; }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;

    void setOffsets(const char * query_begin);
};

/** A captured `INSERT`, `CREATE` or `ALTER`, or a captured part of one (a storage definition, an
  * `ALTER` command), with the `Error` kept beside it rather than inside it.
  *
  * The slot that failed is often a typed member (`ASTCreateQuery::columns_list` is an `ASTColumns`)
  * or a string (`ASTInsertQuery::format`), so it cannot hold an `Error`, and these nodes do not
  * serialize `children`. `writeJSON` therefore writes the node and adds `"<key>": <error>` to its
  * object, which is how a consumer sees it: as the value of the key that failed. `error` is the
  * `Error`, or the captured part that failed in that slot, which holds the `Error` in turn. Only keys
  * that the node leaves out when the slot is empty are accepted, so the key never appears twice.
  */
class ASTPartialStatement : public IAST
{
public:
    ASTPtr statement;
    ASTPtr error;
    /// The JSON key of the failed slot in `statement`; statically allocated.
    const char * key = nullptr;

    String getID(char) const override { return "PartialStatement"; }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
};

}
