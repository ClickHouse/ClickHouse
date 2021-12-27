#pragma once

#include <Substrait/plan.pb.h>


namespace dbms
{


enum Function
{
    IS_NOT_NULL=1,
    GREATER_THAN_OR_EQUAL,
    AND,
    LESS_THAN_OR_EQUAL,
    LESS_THAN,
    MULTIPLY,
    SUM
};

using SchemaPtr = io::substrait::Type_NamedStruct *;

class SerializedPlanBuilder
{
public:
    SerializedPlanBuilder();
    SerializedPlanBuilder& registerSupportedFunctions() {
        this->registerFunction(IS_NOT_NULL, "IS_NOT_NULL")
            .registerFunction(GREATER_THAN_OR_EQUAL, "GREATER_THAN_OR_EQUAL")
            .registerFunction(AND, "AND")
            .registerFunction(LESS_THAN_OR_EQUAL, "LESS_THAN_OR_EQUAL")
            .registerFunction(LESS_THAN, "LESS_THAN")
            .registerFunction(MULTIPLY, "MULTIPLY")
            .registerFunction(SUM, "SUM");
        return *this;
    }
    SerializedPlanBuilder& registerFunction(int id, std::string name);
    SerializedPlanBuilder& filter(io::substrait::Expression* condition);
    SerializedPlanBuilder& aggregate(std::vector<int32_t> keys, std::vector<io::substrait::AggregateRel_Measure *> aggregates);
    SerializedPlanBuilder& read(std::string path, SchemaPtr schema);
    std::unique_ptr<io::substrait::Plan> build();

private:
    void setInputToPrev(io::substrait::Rel * input);
    io::substrait::Rel * prev_rel;
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
    SchemaPtr build();
    SerializedSchemaBuilder& column(std::string name, std::string type, bool nullable = false);
private:
    std::map<std::string, std::string> type_map;
    std::map<std::string, bool> nullability_map;
    SchemaPtr schema;
};

using ExpressionList = std::vector<io::substrait::Expression *>;
using MeasureList = std::vector<io::substrait::AggregateRel_Measure *>;


io::substrait::Expression * scalarFunction(int32_t id, ExpressionList args);
io::substrait::AggregateRel_Measure * measureFunction(int32_t id, ExpressionList args);

io::substrait::Expression* literal(double_t value);
io::substrait::Expression* literal(int32_t value);
io::substrait::Expression* literal(std::string value);

io::substrait::Expression * selection(int32_t field_id);

}
