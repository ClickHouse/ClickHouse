#pragma once

#include <substrait/plan.pb.h>


namespace dbms
{
class SerializedPlanBuilder
{
public:
    SerializedPlanBuilder& filter();
    SerializedPlanBuilder& aggregate();
    SerializedPlanBuilder& project();
    io::substrait::Plan build();
public:
    static SerializedPlanBuilder& read();
};

/**
 * build a schema, need define column name and column.
 * 1. column name
 * 2. column type
 * 3. nullability
 */
class SerializedSchemaBuilder {
public:
    io::substrait::Type_NamedStruct build();
    SerializedPlanBuilder& column(std::string name, std::string type, bool nullable = false);
public:
    static SerializedSchemaBuilder& builder();

private:
    std::map<std::string, std::string> type_map;
    std::map<std::string, bool> nullability_map;
};
}
