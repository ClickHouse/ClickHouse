#pragma once

#include <substrait/plan.pb.h>
#include <Storages/MergeTreeTool.h>


namespace dbms
{


enum Function
{
    IS_NOT_NULL=0,
    GREATER_THAN_OR_EQUAL,
    AND,
    LESS_THAN_OR_EQUAL,
    LESS_THAN,
    MULTIPLY,
    SUM,
    TO_DATE,
    EQUAL_TO
};

using SchemaPtr = substrait::NamedStruct *;

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
            .registerFunction(SUM, "SUM")
            .registerFunction(TO_DATE, "TO_DATE")
            .registerFunction(EQUAL_TO, "EQUAL_TO");
        return *this;
    }
    SerializedPlanBuilder& registerFunction(int id, std::string name);
    SerializedPlanBuilder& filter(substrait::Expression* condition);
    SerializedPlanBuilder& project(std::vector<substrait::Expression*> projections);
    SerializedPlanBuilder& aggregate(std::vector<int32_t> keys, std::vector<substrait::AggregateRel_Measure *> aggregates);
    SerializedPlanBuilder& read(std::string path, SchemaPtr schema);
    SerializedPlanBuilder& readMergeTree(std::string database, std::string table, std::string relative_path, int min_block, int max_block, SchemaPtr schema);
    std::unique_ptr<substrait::Plan> build();

private:
    void setInputToPrev(substrait::Rel * input);
    substrait::Rel * prev_rel = nullptr;
    std::unique_ptr<substrait::Plan> plan;
};


using Type = substrait::Type;
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

using ExpressionList = std::vector<substrait::Expression *>;
using MeasureList = std::vector<substrait::AggregateRel_Measure *>;


substrait::Expression * scalarFunction(int32_t id, ExpressionList args);
substrait::AggregateRel_Measure * measureFunction(int32_t id, ExpressionList args);

substrait::Expression* literal(double_t value);
substrait::Expression* literal(int32_t value);
substrait::Expression* literal(std::string value);
substrait::Expression* literalDate(int32_t value);

substrait::Expression * selection(int32_t field_id);

}
