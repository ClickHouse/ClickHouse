#include "SerializedPlanBuilder.h"

namespace dbms
{
std::unique_ptr<io::substrait::Type_NamedStruct> SerializedSchemaBuilder::build()
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
SerializedSchemaBuilder::SerializedSchemaBuilder():schema(std::make_unique<io::substrait::Type_NamedStruct>())
{
}
SerializedPlanBuilder & SerializedPlanBuilder::filter(std::string lhs, CompareOperator compareOperator, int value)
{
    this->filters.push_back(std::make_tuple(lhs, compareOperator, value));
    return *this;
}
SerializedPlanBuilder & SerializedPlanBuilder::files(std::string path, SchemaPtr schema)
{
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
