#include <arrow/io/memory.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/api.h>
#include <arrow/util/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/adapters/orc/adapter.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include <memory>

void write_arrow(const arrow::Table & table)
{
    auto file = arrow::io::FileOutputStream::Open("nested_nested_table.arrow");
    
    auto writer = arrow::ipc::MakeFileWriter(file->get(), table.schema()).ValueOrDie();
    
    auto status = writer->WriteTable(table, 100000);

    if (!status.ok())
        throw std::runtime_error(status.ToString());

    status = writer->Close();

    if (!status.ok())
        throw std::runtime_error(status.ToString());
}

void write_parquet(const arrow::Table & table)
{
    auto file = arrow::io::FileOutputStream::Open("nested_nested_table.parquet");
    
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    parquet::WriterProperties::Builder prop_builder;
    auto props = prop_builder.build();
    auto status = parquet::arrow::FileWriter::Open(
            *table.schema(),
            arrow::default_memory_pool(),
            *file,
            props,
            &writer);

    
    status = writer->WriteTable(table, 100000);

    if (!status.ok())
        throw std::runtime_error(status.ToString());

    status = writer->Close();

    if (!status.ok())
        throw std::runtime_error(status.ToString());
}

void write_orc(const arrow::Table & table)
{
    auto file = arrow::io::FileOutputStream::Open("nested_nested_table.orc");
    
    auto writer = arrow::adapters::orc::ORCFileWriter::Open(file->get()).ValueOrDie();
    
    auto status = writer->Write(table);

    if (!status.ok())
        throw std::runtime_error(status.ToString());

    status = writer->Close();

    if (!status.ok())
        throw std::runtime_error(status.ToString());
}


void fillNested(arrow::ArrayBuilder * builder, bool nested)
{
    arrow::ListBuilder * list_builder = static_cast<arrow::ListBuilder *>(builder);
    arrow::StructBuilder * struct_builder = static_cast<arrow::StructBuilder *>(list_builder->value_builder());
    arrow::Int32Builder * elem1_builder = static_cast<arrow::Int32Builder *>(struct_builder->field_builder(0));
    arrow::BinaryBuilder * elem2_builder = static_cast<arrow::BinaryBuilder *>(struct_builder->field_builder(1));
    arrow::FloatBuilder * elem3_builder = static_cast<arrow::FloatBuilder *>(struct_builder->field_builder(2));

    arrow::ListBuilder * nested_list_builder = nullptr;
    if (nested)
        nested_list_builder = static_cast<arrow::ListBuilder *>(struct_builder->field_builder(3));

    arrow::Status status;
    status = list_builder->Append();

    std::vector<int> elem1 = {1, 2, 3};
    std::vector<std::string> elem2 = {"123", "456", "789"};
    std::vector<float> elem3 = {9.8, 10.12, 11.14};
    status = elem1_builder->AppendValues(elem1);
    status = elem2_builder->AppendValues(elem2);
    status = elem3_builder->AppendValues(elem3);
    if (nested)
        fillNested(nested_list_builder, false);

    for (size_t i = 0; i != elem1.size(); ++i)
        status = struct_builder->Append();

    status = list_builder->Append();

    elem1 = {4, 5, 6};
    elem2 = {"101112", "131415", "161718"};
    elem3 = {123.8, 10.2, 11.414};
    status = elem1_builder->AppendValues(elem1);
    status = elem2_builder->AppendValues(elem2);
    status = elem3_builder->AppendValues(elem3);
    if (nested)
        fillNested(nested_list_builder, false);

    for (size_t i = 0; i != elem1.size(); ++i)
        status = struct_builder->Append();

    status = list_builder->Append();

    elem1 = {7, 8, 9};
    elem2 = {"101", "415", "118"};
    elem3 = {13.08, 1.12, 0.414};
    status = elem1_builder->AppendValues(elem1);
    status = elem2_builder->AppendValues(elem2);
    status = elem3_builder->AppendValues(elem3);
    if (nested)
        fillNested(nested_list_builder, false);

    for (size_t i = 0; i != elem1.size(); ++i)
        status = struct_builder->Append();
}

int main()
{
    std::vector<std::shared_ptr<arrow::Field>> nested_struct_fields;
    nested_struct_fields.push_back(std::make_shared<arrow::Field>("elem1", arrow::int32()));
    nested_struct_fields.push_back(std::make_shared<arrow::Field>("elem2", arrow::binary()));
    nested_struct_fields.push_back(std::make_shared<arrow::Field>("elem3", arrow::float32()));
    auto nested_struct_type = arrow::struct_(nested_struct_fields);
    auto nested_field = std::make_shared<arrow::Field>("nested", nested_struct_type);
    auto nested_list_type = arrow::list(nested_field);
    auto nested_list_field = std::make_shared<arrow::Field>("nested", nested_list_type);

    std::vector<std::shared_ptr<arrow::Field>> struct_fields;
    struct_fields.push_back(std::make_shared<arrow::Field>("elem1", arrow::int32()));
    struct_fields.push_back(std::make_shared<arrow::Field>("elem2", arrow::binary()));
    struct_fields.push_back(std::make_shared<arrow::Field>("elem3", arrow::float32()));
    struct_fields.push_back(std::make_shared<arrow::Field>("nested", nested_list_type));


    auto struct_type = arrow::struct_(struct_fields);
    auto field = std::make_shared<arrow::Field>("table", struct_type);
    auto list_type = arrow::list(field);

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::ArrayBuilder> tmp;
    auto status = MakeBuilder(pool, list_type, &tmp);

    if (!status.ok())
        throw std::runtime_error(status.ToString());

    fillNested(tmp.get(), true);

    std::shared_ptr<arrow::Array> array;
    status = tmp->Finish(&array);

    if (!status.ok())
        throw std::runtime_error(status.ToString());

    std::vector<std::shared_ptr<arrow::Field>> fields_for_schema = {std::make_shared<arrow::Field>("table", list_type)};
    auto schema = std::make_shared<arrow::Schema>(std::move(fields_for_schema));
    auto table = arrow::Table::Make(schema, {array});

    if (!table)
        throw std::runtime_error("WTF");

    write_orc(*table);
    write_arrow(*table);
    write_parquet(*table);

    return 0;
}
