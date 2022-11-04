#include "SerializedPlanBuilder.h"
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}
}

namespace dbms
{

using namespace DB;
SchemaPtr SerializedSchemaBuilder::build()
{
    for (const auto & [name, type] : this->type_map)
    {
        this->schema->add_names(name);
        auto * type_struct = this->schema->mutable_struct_();
        if (type == "I8")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_i8()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "I32")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_i32()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "I64")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_i64()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "Boolean")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_bool_()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "I16")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_i16()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "String")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_string()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "FP32")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_fp32()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "FP64")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_fp64()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "Date")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_date()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else if (type == "Timestamp")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_timestamp()->set_nullability(
                this->nullability_map[name] ? substrait::Type_Nullability_NULLABILITY_NULLABLE
                                            : substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }
        else
        {
            throw std::runtime_error("doesn't support type " + type);
        }
    }
    return std::move(this->schema);
}
SerializedSchemaBuilder & SerializedSchemaBuilder::column(const std::string & name, const std::string & type, bool nullable)
{
    this->type_map.emplace(name, type);
    this->nullability_map.emplace(name, nullable);
    return *this;
}
SerializedSchemaBuilder::SerializedSchemaBuilder() : schema(new substrait::NamedStruct())
{
}
SerializedPlanBuilder & SerializedPlanBuilder::registerFunction(int id, const std::string & name)
{
    auto * extension = this->plan->mutable_extensions()->Add();
    auto * function_mapping = extension->mutable_extension_function();
    function_mapping->set_function_anchor(id);
    function_mapping->set_name(name);
    return *this;
}

void SerializedPlanBuilder::setInputToPrev(substrait::Rel * input)
{
    if (!this->prev_rel)
    {
        auto * root = this->plan->mutable_relations()->Add()->mutable_root();
        root->set_allocated_input(input);
        return;
    }
    if (this->prev_rel->has_filter())
    {
        this->prev_rel->mutable_filter()->set_allocated_input(input);
    }
    else if (this->prev_rel->has_aggregate())
    {
        this->prev_rel->mutable_aggregate()->set_allocated_input(input);
    }
    else if (this->prev_rel->has_project())
    {
        this->prev_rel->mutable_project()->set_allocated_input(input);
    }
    else
    {
        throw std::runtime_error("does support rel type");
    }
}

SerializedPlanBuilder & SerializedPlanBuilder::filter(substrait::Expression * condition)
{
    substrait::Rel * filter = new substrait::Rel();
    filter->mutable_filter()->set_allocated_condition(condition);
    setInputToPrev(filter);
    this->prev_rel = filter;
    return *this;
}

SerializedPlanBuilder & SerializedPlanBuilder::read(const std::string & path, SchemaPtr schema)
{
    substrait::Rel * rel = new substrait::Rel();
    auto * read = rel->mutable_read();
    read->mutable_local_files()->add_items()->set_uri_file(path);
    read->set_allocated_base_schema(schema);
    setInputToPrev(rel);
    this->prev_rel = rel;
    return *this;
}

SerializedPlanBuilder & SerializedPlanBuilder::readMergeTree(
    const std::string & database,
    const std::string & table,
    const std::string & relative_path,
    int min_block,
    int max_block,
    SchemaPtr schema)
{
    substrait::Rel * rel = new substrait::Rel();
    auto * read = rel->mutable_read();
    read->mutable_extension_table()->mutable_detail()->set_value(local_engine::MergeTreeTable{.database=database,.table=table,.relative_path=relative_path,.min_block=min_block,.max_block=max_block}.toString());
    read->set_allocated_base_schema(schema);
    setInputToPrev(rel);
    this->prev_rel = rel;
    return *this;
}


std::unique_ptr<substrait::Plan> SerializedPlanBuilder::build()
{
    return std::move(this->plan);
}

SerializedPlanBuilder::SerializedPlanBuilder() : plan(std::make_unique<substrait::Plan>())
{
}

SerializedPlanBuilder & SerializedPlanBuilder::aggregate(std::vector<int32_t>  /*keys*/, std::vector<substrait::AggregateRel_Measure *> aggregates)
{
    substrait::Rel * rel = new substrait::Rel();
    auto * agg = rel->mutable_aggregate();
    // TODO support group
    auto * measures = agg->mutable_measures();
    for (auto * measure : aggregates)
    {
        measures->AddAllocated(measure);
    }
    setInputToPrev(rel);
    this->prev_rel = rel;
    return *this;
}

SerializedPlanBuilder & SerializedPlanBuilder::project(std::vector<substrait::Expression *> projections)
{
    substrait::Rel * project = new substrait::Rel();
    for (auto * expr : projections)
    {
        project->mutable_project()->mutable_expressions()->AddAllocated(expr);
    }
    setInputToPrev(project);
    this->prev_rel = project;
    return *this;
}

std::shared_ptr<substrait::Type> SerializedPlanBuilder::buildType(const DB::DataTypePtr & ch_type)
{
    const auto * ch_type_nullable = checkAndGetDataType<DataTypeNullable>(ch_type.get());
    const bool is_nullable = (ch_type_nullable != nullptr);
    auto type_nullability
        = is_nullable ? substrait::Type_Nullability_NULLABILITY_NULLABLE : substrait::Type_Nullability_NULLABILITY_REQUIRED;

    const auto ch_type_without_nullable = DB::removeNullable(ch_type);
    const DB::WhichDataType which(ch_type_without_nullable);

    auto res = std::make_shared<substrait::Type>();
    if (which.isUInt8())
        res->mutable_bool_()->set_nullability(type_nullability);
    else if (which.isInt8())
        res->mutable_i8()->set_nullability(type_nullability);
    else if (which.isInt16())
        res->mutable_i16()->set_nullability(type_nullability);
    else if (which.isInt32())
        res->mutable_i32()->set_nullability(type_nullability);
    else if (which.isInt64())
        res->mutable_i64()->set_nullability(type_nullability);
    else if (which.isString() || which.isAggregateFunction())
        res->mutable_binary()->set_nullability(type_nullability); /// Spark Binary type is more similiar to CH String type
    else if (which.isFloat32())
        res->mutable_fp32()->set_nullability(type_nullability);
    else if (which.isFloat64())
        res->mutable_fp64()->set_nullability(type_nullability);
    else if (which.isFloat64())
        res->mutable_fp64()->set_nullability(type_nullability);
    else if (which.isDateTime64())
    {
        const auto * ch_type_datetime64 = checkAndGetDataType<DataTypeDateTime64>(ch_type_without_nullable.get());
        if (ch_type_datetime64->getScale() != 6)
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support converting from {}", ch_type->getName());
        res->mutable_timestamp()->set_nullability(type_nullability);
    }
    else if (which.isDate32())
        res->mutable_date()->set_nullability(type_nullability);
    else if (which.isDecimal())
    {
        if (which.isDecimal256())
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support converting from {}", ch_type->getName());

        const auto scale = getDecimalScale(*ch_type_without_nullable, 0);
        const auto precision = getDecimalPrecision(*ch_type_without_nullable);
        if (scale == 0 && precision == 0)
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support converting from {}", ch_type->getName());
        res->mutable_decimal()->set_nullability(type_nullability);
        res->mutable_decimal()->set_scale(scale);
        res->mutable_decimal()->set_precision(precision);
    }
    else if (which.isTuple())
    {
        const auto * ch_tuple_type = checkAndGetDataType<DataTypeTuple>(ch_type_without_nullable.get());
        const auto & ch_field_types = ch_tuple_type->getElements();
        res->mutable_struct_()->set_nullability(type_nullability);
        for (const auto & ch_field_type: ch_field_types)
            res->mutable_struct_()->mutable_types()->Add(std::move(*buildType(ch_field_type)));
    }
    else if (which.isArray())
    {
        const auto * ch_array_type = checkAndGetDataType<DataTypeArray>(ch_type_without_nullable.get());
        const auto & ch_nested_type = ch_array_type->getNestedType();
        res->mutable_list()->set_nullability(type_nullability);
        *(res->mutable_list()->mutable_type()) = *buildType(ch_nested_type);
    }
    else if (which.isMap())
    {
        const auto & ch_map_type = checkAndGetDataType<DataTypeMap>(ch_type_without_nullable.get());
        const auto & ch_key_type = ch_map_type->getKeyType();
        const auto & ch_val_type = ch_map_type->getValueType();
        res->mutable_map()->set_nullability(type_nullability);
        *(res->mutable_map()->mutable_key()) = *buildType(ch_key_type);
        *(res->mutable_map()->mutable_value()) = *buildType(ch_val_type);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support converting from {}", ch_type->getName());

    return std::move(res);
}

void SerializedPlanBuilder::buildType(const DB::DataTypePtr & ch_type, String & substrait_type)
{
    auto pb = buildType(ch_type);
    substrait_type = pb->SerializeAsString();
}


substrait::Expression * selection(int32_t field_id)
{
    substrait::Expression * rel = new substrait::Expression();
    auto * selection = rel->mutable_selection();
    selection->mutable_direct_reference()->mutable_struct_field()->set_field(field_id);
    return rel;
}
substrait::Expression * scalarFunction(int32_t id, ExpressionList args)
{
    substrait::Expression * rel = new substrait::Expression();
    auto * function = rel->mutable_scalar_function();
    function->set_function_reference(id);
    std::for_each(args.begin(), args.end(), [function](auto * expr) { function->mutable_args()->AddAllocated(expr); });
    return rel;
}
substrait::AggregateRel_Measure * measureFunction(int32_t id, ExpressionList args)
{
    substrait::AggregateRel_Measure * rel = new substrait::AggregateRel_Measure();
    auto * measure = rel->mutable_measure();
    measure->set_function_reference(id);
    std::for_each(args.begin(), args.end(), [measure](auto * expr) { measure->mutable_args()->AddAllocated(expr); });
    return rel;
}
substrait::Expression * literal(double_t value)
{
    substrait::Expression * rel = new substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_fp64(value);
    return rel;
}

substrait::Expression * literal(int32_t value)
{
    substrait::Expression * rel = new substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_i32(value);
    return rel;
}

substrait::Expression * literal(const std::string & value)
{
    substrait::Expression * rel = new substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_string(value);
    return rel;
}

substrait::Expression* literalDate(int32_t value)
{
    substrait::Expression * rel = new substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_date(value);
    return rel;
}

/// Timestamp in units of microseconds since the UNIX epoch.
substrait::Expression * literalTimestamp(int64_t value)
{
    substrait::Expression * rel = new substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_timestamp(value);
    return rel;
}

}
