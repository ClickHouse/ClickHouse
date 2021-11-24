#pragma once

#include <Substrait/plan.pb.h>


namespace dbms
{

enum CompareOperator {
    LESS,
    EQUAL,
    GREATER
};
using SchemaPtr = std::unique_ptr<io::substrait::Type_NamedStruct>;
using Filter = std::tuple<std::string, CompareOperator, int>;

class SerializedPlanBuilder
{
public:
    SerializedPlanBuilder();
    SerializedPlanBuilder& filter(std::string lhs, CompareOperator compareOperator, int value);
    SerializedPlanBuilder& files(std::string path, SchemaPtr schema);
//    SerializedPlanBuilder& aggregate();
//    SerializedPlanBuilder& project();
    std::unique_ptr<io::substrait::Plan> build();

    std::vector<Filter> filters;
    std::string source;
    SchemaPtr data_schema;
    std::unique_ptr<io::substrait::Plan> plan;
};


using Type = io::substrait::Type;
/**
 * build a schema, need define column name and column.
 * 1. column name
 * 2. column type
 * 3. nullability
 */
class SerializedSchemaBuilder {
public:
    SerializedSchemaBuilder();
    std::unique_ptr<io::substrait::Type_NamedStruct> build();
    SerializedSchemaBuilder& column(std::string name, std::string type, bool nullable = false);
private:
    std::map<std::string, std::string> type_map;
    std::map<std::string, bool> nullability_map;
    std::unique_ptr<io::substrait::Type_NamedStruct> schema;
};
}
