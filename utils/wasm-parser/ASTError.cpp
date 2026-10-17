#include <ASTError.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTToJSON.h>
#include <Common/Exception.h>

#include <algorithm>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The keys a capture site may name, per node: each one is written only when its slot is set, and a
/// failed slot is empty, so the `Error` is the only value the key gets.
bool isPartialStatementKey(const IAST & statement, std::string_view key)
{
    auto is_one_of = [&](std::initializer_list<std::string_view> keys)
    {
        return std::find(keys.begin(), keys.end(), key) != keys.end();
    };

    if (statement.as<ASTInsertQuery>())
        return is_one_of({"table", "table_function", "partition_by", "infile", "compression", "settings_ast", "format", "columns",
            "select"});
    if (statement.as<ASTCreateQuery>())
        return is_one_of({"table_ast", "cluster", "columns_list", "storage", "select", "as_table_function", "aliases_list",
            "refresh_strategy", "targets", "sql_security", "comment", "dictionary_attributes_list", "dictionary", "as_table",
            "attach_from_path", "attach_as_replicated"});
    if (statement.as<ASTAlterQuery>())
        return is_one_of({"database_ast", "table_ast", "cluster", "command_list"});
    if (statement.as<ASTAlterCommand>())
        return is_one_of({"col_decl", "column", "order_by", "sample_by", "index_decl", "index", "constraint_decl", "constraint",
            "projection_decl", "projection", "statistics_decl", "partition", "partitions", "predicate", "update_assignments",
            "comment", "ttl", "settings_changes", "settings_resets", "select", "sql_security", "rename_to", "refresh",
            "snapshot_desc", "execute_args", "add_enum_values", "move_destination_name", "from", "with_name", "from_table",
            "to_table", "snapshot_name", "execute_command_name", "remove_property"});
    if (statement.as<ASTStorage>())
        return is_one_of({"engine", "partition_by", "primary_key", "order_by", "unique_key", "sample_by", "ttl_table", "settings"});
    if (statement.as<ASTDictionary>())
        return is_one_of({"primary_key", "source", "lifetime", "layout", "range", "dict_settings"});
    if (statement.as<ASTRefreshStrategy>())
        return is_one_of({"period", "offset", "spread", "dependencies", "settings"});
    return false;
}

}

ASTPtr ASTError::clone() const
{
    return make_intrusive<ASTError>(*this);
}

void ASTError::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Error");
    w.writeUInt("begin", begin);
    w.writeUInt("end", end);

    w.writeKey("expected");
    out << '[';
    for (size_t i = 0; i < expected.size(); ++i)
    {
        if (i > 0)
            out << ',';
        writeJSONString(expected[i], out, w.getFormatSettings());
    }
    out << ']';
}

void ASTError::setOffsets(const char * query_begin)
{
    begin = static_cast<UInt64>(begin_pos - query_begin);
    end = static_cast<UInt64>(end_pos - query_begin);
}

ASTPtr ASTPartialStatement::clone() const
{
    auto res = make_intrusive<ASTPartialStatement>(*this);
    res->children.clear();
    res->statement = statement->clone();
    res->error = error->clone();
    res->children = {res->statement, res->error};
    return res;
}

void ASTPartialStatement::writeJSON(WriteBuffer & out) const
{
    if (!isPartialStatementKey(*statement, key))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key '{}' is not a capture slot of {}", key, statement->getID());

    const String json = serializeASTToJSON(*statement);
    if (json.empty() || json.back() != '}')
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The JSON of {} is not an object", statement->getID());

    out.write(json.data(), json.size() - 1);
    out << ",\"" << key << "\":";
    error->writeJSON(out);
    out << '}';
}

}
