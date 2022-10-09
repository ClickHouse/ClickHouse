#include <functional>
#include <memory>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Processors/Chunk.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <base/types.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Core/Types.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <substrait/plan.pb.h>

#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <magic_enum.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
}
}
namespace local_engine
{
SubstraitFileSource::SubstraitFileSource(DB::ContextPtr context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos)
    : DB::SourceWithProgress(header_, false)
    , context(context_)
    , output_header(header_)
{
    if (!output_header.columns())
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Empty columns to read. Maybe use count(col) instead of count(1)/count(*)");
    }

    to_read_header = output_header;
    if (file_infos.items_size())
    {
        Poco::URI file_uri(file_infos.items().Get(0).uri_file());
        read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);
        for (const auto & item : file_infos.items())
        {
            files.emplace_back(FormatFileUtil::createFile(context, read_buffer_builder, item));
        }

        auto partition_keys = files[0]->getFilePartitionKeys();
        /// file partition keys are read from the file path
        for (const auto & key : partition_keys)
        {
            to_read_header.erase(key);
        }
    }
}


DB::Chunk SubstraitFileSource::generate()
{
    while(current_file_index < files.size())
    {
        if (!tryPrepareReader())
        {
            /// all files finished
            return {};
        }

        DB::Chunk chunk;
        if (file_reader->pull(chunk))
            return chunk;

        /// try to read from next file
        file_reader.reset();
    }
    return {};
}

bool SubstraitFileSource::tryPrepareReader()
{
    if (file_reader) [[likely]]
        return true;

    if (current_file_index >= files.size())
        return false;

    auto current_file = files[current_file_index];
    current_file_index += 1;

    if (!current_file->supportSplit() && current_file->getStartOffset())
    {
        /// For the files do not support split strategy, the task with not 0 offset will generate empty data
        file_reader = std::make_unique<EmptyFileReader>(current_file);
        return true;
    }
    if (!to_read_header.columns())
    {
        auto total_rows = current_file->getTotalRows();
        if (total_rows)
        {
            file_reader = std::make_unique<ConstColumnsFileReader>(current_file, context, output_header, *total_rows);
        }
        else
        {
            /// TODO: It may be a text format file that we do not have the stat metadata, e.g. total rows.
            /// If we can get the file's schema, we can try to read all columns out. maybe consider make this
            /// scan action fallback into spark
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "All columns to read is partition columns, but this file({}) doesn't support this case.",
                current_file->getURIPath());
        }
    }
    else
    {
        file_reader = std::make_unique<NormalFileReader>(current_file, context, to_read_header, output_header);
    }

    return true;
}


DB::ColumnPtr FileReaderWrapper::createConstColumn(DB::DataTypePtr data_type, const DB::Field & field, size_t rows)
{
    auto nested_type = DB::removeNullable(data_type);
    auto column = nested_type->createColumnConst(rows, field);

    if (data_type->isNullable())
        column = DB::ColumnNullable::create(column, DB::ColumnUInt8::create(rows, 0));
    return column;
}

#define BUILD_INT_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    {\
        type value = 0;\
        DB::readIntText(value, in);\
        return DB::Field(value);\
    }

#define BUILD_FP_FIELD(type) \
    [](DB::ReadBuffer & in, const String &) \
    {\
        type value = 0.0;\
        DB::readFloatText(value, in);\
        return DB::Field(value);\
    }

DB::Field FileReaderWrapper::buildFieldFromString(const String & str_value, DB::DataTypePtr type)
{
    using FieldBuilder =  std::function<DB::Field(DB::ReadBuffer &, const String &)>;
    static std::map<int, FieldBuilder> field_builders
        = {{magic_enum::enum_integer(DB::TypeIndex::Int8), BUILD_INT_FIELD(Int8) },
           {magic_enum::enum_integer(DB::TypeIndex::Int16), BUILD_INT_FIELD(Int16) },
           {magic_enum::enum_integer(DB::TypeIndex::Int32), BUILD_INT_FIELD(Int32) },
           {magic_enum::enum_integer(DB::TypeIndex::Int64), BUILD_INT_FIELD(Int64) },
           {magic_enum::enum_integer(DB::TypeIndex::Float32), BUILD_FP_FIELD(DB::Float32) },
           {magic_enum::enum_integer(DB::TypeIndex::Float64), BUILD_FP_FIELD(DB::Float64)},
           {magic_enum::enum_integer(DB::TypeIndex::String), [](DB::ReadBuffer &, const String & val) { return DB::Field(val); }},
           {magic_enum::enum_integer(DB::TypeIndex::Date),
            [](DB::ReadBuffer & in, const String &)
            {
                DayNum value;
                readDateText(value, in);
                return DB::Field(value);
            }},
           {magic_enum::enum_integer(DB::TypeIndex::Date32),
            [](DB::ReadBuffer & in, const String &)
            {
                ExtendedDayNum value;
                readDateText(value, in);
                return DB::Field(value.toUnderType());
            }}};

    auto nested_type = DB::removeNullable(type);
    DB::ReadBufferFromString read_buffer(str_value);
    auto it = field_builders.find(magic_enum::enum_integer(nested_type->getTypeId()));
    if (it == field_builders.end())
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported data type {}", type->getFamilyName());
    return it->second(read_buffer, str_value);
}

ConstColumnsFileReader::ConstColumnsFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & header_, size_t block_size_)
    : FileReaderWrapper(file_)
    , context(context_)
    , header(header_)
    , remained_rows(0)
    , block_size(block_size_)
{
    auto rows = file->getTotalRows();
    if (!rows)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot get total rows number from file : {}", file->getURIPath());
    remained_rows = *rows;
}

bool ConstColumnsFileReader::pull(DB::Chunk & chunk)
{
    if (!remained_rows) [[unlikely]]
        return false;
    size_t to_read_rows = 0;
    if (remained_rows < block_size)
    {
        to_read_rows = remained_rows;
        remained_rows = 0;
    }
    else
    {
        to_read_rows = block_size;
        remained_rows -= block_size;
    }
    DB::Columns res_columns;
    size_t columns_num = header.columns();
    res_columns.reserve(columns_num);
    const auto & partition_values = file->getFilePartitionValues();
    for (size_t pos = 0; pos < columns_num; ++pos)
    {
        auto col_with_name_and_type = header.getByPosition(pos);
        auto type = col_with_name_and_type.type;
        const auto & name = col_with_name_and_type.name;
        auto it = partition_values.find(name);
        if (it == partition_values.end()) [[unlikely]]
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow partition column : {}", name);
        }
        auto field = buildFieldFromString(it->second, type);
        auto column = createConstColumn(type, field, to_read_rows);
        res_columns.emplace_back(column);
    }

    chunk = DB::Chunk(std::move(res_columns), to_read_rows);
    return true;
}

NormalFileReader::NormalFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & to_read_header_, const DB::Block & output_header_)
    : FileReaderWrapper(file_)
    , context(context_)
    , to_read_header(to_read_header_)
    , output_header(output_header_)
{
    input_format = file->createInputFormat(to_read_header);
    DB::Pipe pipe(input_format->input);
    pipeline = std::make_unique<DB::QueryPipeline>(std::move(pipe));
    reader = std::make_unique<DB::PullingPipelineExecutor>(*pipeline);
}


bool NormalFileReader::pull(DB::Chunk & chunk)
{
    DB::Chunk tmp_chunk;
    auto status = reader->pull(tmp_chunk);
    if (!status)
    {
        return false;
    }

    size_t rows = tmp_chunk.getNumRows();
    if (!rows)
        return false;

    auto read_columns = tmp_chunk.detachColumns();
    DB::Columns res_columns;
    auto columns_with_name_and_type = output_header.getColumnsWithTypeAndName();
    auto partition_values = file->getFilePartitionValues();

    for (auto & column : columns_with_name_and_type)
    {
        if (to_read_header.has(column.name))
        {
            auto pos = to_read_header.getPositionByName(column.name);
            res_columns.push_back(read_columns[pos]);
        }
        else
        {
            auto it = partition_values.find(column.name);
            if (it == partition_values.end())
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR, "Not found column({}) from file({}) partition keys.", column.name, file->getURIPath());
            }
            auto field = buildFieldFromString(it->second, column.type);
            res_columns.push_back(createConstColumn(column.type, field, rows));
        }
    }
    chunk = DB::Chunk(std::move(res_columns), rows);
    return true;
}
}
