#include "SerializedPlanBuilder.h"

namespace dbms
{
SchemaPtr SerializedSchemaBuilder::build()
{
    for (const auto & [name, type] : this->type_map)
    {
        this->schema->add_names(name);
        auto *type_struct = this->schema->mutable_struct_();
        if (type == "I8")
        {
                auto *t = type_struct->mutable_types()->Add();
                t->mutable_i8()->set_nullability(
                    this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "I32")
        {
            auto *t = type_struct->mutable_types()->Add();
            t->mutable_i32()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "I64")
        {
            auto *t = type_struct->mutable_types()->Add();
            t->mutable_i64()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "Boolean")
        {
            auto *t = type_struct->mutable_types()->Add();
            t->mutable_bool_()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "I16")
        {
            auto *t = type_struct->mutable_types()->Add();
            t->mutable_i16()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "String")
        {
            auto *t = type_struct->mutable_types()->Add();
            t->mutable_string()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "FP32")
        {
            auto *t = type_struct->mutable_types()->Add();
            t->mutable_fp32()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else if (type == "FP64")
        {
            auto *t = type_struct->mutable_types()->Add();
            t->mutable_fp64()->set_nullability(
                this->nullability_map[name] ? io::substrait::Type_Nullability_NULLABLE : io::substrait::Type_Nullability_REQUIRED);
        }
        else {
            throw "doesn't support type "+ type;
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
SerializedSchemaBuilder::SerializedSchemaBuilder():schema(new io::substrait::Type_NamedStruct())
{
}
SerializedPlanBuilder& SerializedPlanBuilder::registerFunction(int id, std::string name)
{
    auto *mapping = this->plan->mutable_mappings()->Add();
    auto *function_mapping = mapping->mutable_function_mapping();
    function_mapping->mutable_function_id()->set_id(id);
    function_mapping->set_name(name);
    return *this;
}

void SerializedPlanBuilder::setInputToPrev(io::substrait::Rel * input)
{
    if (!this->prev_rel) return;
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
    this->source = path;
    this->data_schema = std::move(schema);
    return *this;
}
std::unique_ptr<io::substrait::Plan> SerializedPlanBuilder::build()
{
//    for (const auto & [lhs, compareOperator, value] : this->filters)
//    {
//        auto filter_rel = std::make_shared<io::substrait::FilterRel>();
//        auto *function = filter_rel->mutable_condition()->mutable_scalar_function();
//        function->mutable_id()->set_id(1);
//        auto *args = function->mutable_args();
//
//        auto arg1 = io::substrait::Expression();
//        arg1.literal().i32();
//        args->Add(std::move(arg1));
//
//        auto arg2 = io::substrait::Expression();co
//        arg2.literal().i8()
//    }
//
//    filter_rel->mutable_input()->set_allocated_read(read_rel.get())
    auto *rel = this->plan->mutable_relations()->Add();
    auto *read_rel = rel->mutable_read();
    auto *local_files = read_rel->mutable_local_files();
    auto *file = local_files->mutable_items()->Add();
    file->set_uri_path(this->source);
    read_rel->mutable_base_schema()->CopyFrom(*this->data_schema);
    return std::move(this->plan);
}
SerializedPlanBuilder::SerializedPlanBuilder():plan(std::make_unique<io::substrait::Plan>())
{
}
}
