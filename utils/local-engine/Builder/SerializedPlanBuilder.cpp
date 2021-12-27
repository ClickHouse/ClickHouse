#include "SerializedPlanBuilder.h"

namespace dbms
{
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
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "I32")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_i32()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "I64")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_i64()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "Boolean")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_bool_()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "I16")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_i16()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "String")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_string()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "FP32")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_fp32()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "FP64")
        {
            auto * t = type_struct->mutable_types()->Add();
            t->mutable_fp64()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else
        {
            throw "doesn't support type " + type;
        }
    }
    return std::move(this->schema);
}
SerializedSchemaBuilder & SerializedSchemaBuilder::column(std::string name, std::string type, bool nullable)
{
    this->type_map.emplace(name, type);
    this->nullability_map.emplace(name, nullable);
    return *this;
}
SerializedSchemaBuilder::SerializedSchemaBuilder() : schema(new io::substrait::Type_NamedStruct())
{
}
SerializedPlanBuilder & SerializedPlanBuilder::registerFunction(int id, std::string name)
{
    auto * mapping = this->plan->mutable_mappings()->Add();
    auto * function_mapping = mapping->mutable_function_mapping();
    function_mapping->mutable_function_id()->set_id(id);
    function_mapping->set_name(name);
    return *this;
}

void SerializedPlanBuilder::setInputToPrev(io::substrait::Rel * input)
{
    if (!this->prev_rel)
    {
        this->plan->mutable_relations()->AddAllocated(input);
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

SerializedPlanBuilder & SerializedPlanBuilder::filter(io::substrait::Expression * condition)
{
    io::substrait::Rel * filter = new io::substrait::Rel();
    filter->mutable_filter()->set_allocated_condition(condition);
    setInputToPrev(filter);
    this->prev_rel = filter;
    return *this;
}

SerializedPlanBuilder & SerializedPlanBuilder::read(std::string path, SchemaPtr schema)
{
    io::substrait::Rel * rel = new io::substrait::Rel();
    auto * read = rel->mutable_read();
    read->mutable_local_files()->add_items()->set_uri_path(path);
    read->set_allocated_base_schema(schema);
    setInputToPrev(rel);
    this->prev_rel = rel;
    return *this;
}
std::unique_ptr<io::substrait::Plan> SerializedPlanBuilder::build()
{
    return std::move(this->plan);
}
SerializedPlanBuilder::SerializedPlanBuilder() : plan(std::make_unique<io::substrait::Plan>())
{
}
SerializedPlanBuilder & SerializedPlanBuilder::aggregate(std::vector<int32_t> keys, std::vector<io::substrait::AggregateRel_Measure *> aggregates)
{
    io::substrait::Rel * rel = new io::substrait::Rel();
    auto * agg = rel->mutable_aggregate();
    auto * grouping = agg->mutable_groupings()->Add();
    grouping->mutable_input_fields()->Add(keys.begin(),  keys.end());
    auto * measures = agg->mutable_measures();
    for (auto * measure : aggregates)
    {
        measures->AddAllocated(measure);
    }
    setInputToPrev(rel);
    this->prev_rel = rel;
    return *this;
}


io::substrait::Expression * selection(int32_t field_id)
{
    io::substrait::Expression * rel = new io::substrait::Expression();
    auto * selection = rel->mutable_selection();
    selection->mutable_direct_reference()->mutable_struct_field()->set_field(field_id);
    return rel;
}
io::substrait::Expression * scalarFunction(int32_t id, ExpressionList args)
{
    io::substrait::Expression * rel = new io::substrait::Expression();
    auto * function = rel->mutable_scalar_function();
    function->mutable_id()->set_id(id);
    std::for_each(args.begin(), args.end(), [function](auto * expr) { function->mutable_args()->AddAllocated(expr); });
    return rel;
}
io::substrait::AggregateRel_Measure * measureFunction(int32_t id, ExpressionList args)
{
    io::substrait::AggregateRel_Measure * rel = new io::substrait::AggregateRel_Measure();
    auto * measure = rel->mutable_measure();
    measure->mutable_id()->set_id(id);
    std::for_each(args.begin(), args.end(), [measure](auto * expr) { measure->mutable_args()->AddAllocated(expr); });
    return rel;
}
io::substrait::Expression * literal(double_t value)
{
    io::substrait::Expression * rel = new io::substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_fp64(value);
    return rel;
}

io::substrait::Expression * literal(int32_t value)
{
    io::substrait::Expression * rel = new io::substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_i32(value);
    return rel;
}

io::substrait::Expression * literal(std::string value)
{
    io::substrait::Expression * rel = new io::substrait::Expression();
    auto * literal = rel->mutable_literal();
    literal->set_string(value);
    return rel;
}
}
