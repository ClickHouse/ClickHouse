#include "ParquetRowInputFormat.h"


#include <Columns/ColumnString.h>
#include <Common/DateLUTImpl.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromAzureBlobStorage.h>
#include <Common/DebugUtils.h>

local_engine::ParquetRowInputFormat::ParquetRowInputFormat(ReadBuffer & in_, Block header_, size_t block_size) : IInputFormat(std::move(header_), in_), prefer_block_size(block_size)
{
}
std::unique_ptr<local_engine::InputStreamFileSystem> local_engine::ParquetRowInputFormat::inputStreamFileSystem = std::make_unique<local_engine::InputStreamFileSystem>();
void local_engine::ParquetRowInputFormat::prepareReader()
{
    if (!reader)
    {
        if (dynamic_cast<ReadBufferFromAzureBlobStorage *>(in))
        {
            {
                WriteBufferFromString file_buffer(cache_data);
                copyData(*in, file_buffer, std::atomic_int(0));
            }
            cached_reader = std::make_unique<ReadBufferFromString>(cache_data);
            reader = std::make_unique<duckdb::ParquetReader>(this->allocator, inputStreamFileSystem->openStream(*cached_reader));
        }
        else
        {
            reader = std::make_unique<duckdb::ParquetReader>(this->allocator, inputStreamFileSystem->openStream(*in));
        }
    }
    int index = 0;
    int cols = this->getPort().getHeader().columns();
    column_indices.reserve(cols);
    row_type.reserve(cols);
    column_indices.assign(cols, 0);
    row_type.assign(cols, duckdb::LogicalType(duckdb::LogicalType::SQLNULL));
    for (const auto& col : reader->names)
    {
        if (this->getPort().getHeader().has(col))
        {
            int position = this->getPort().getHeader().getPositionByName(col);
            column_indices[position] = index;
            row_type[position] = convertCHTypeToDuckDbType(this->getPort().getHeader().getByName(col).type);
        }
        index++;
    }
    for (duckdb::idx_t i = 0; i < reader->NumRowGroups(); ++i)
    {
        row_group_ids.push_back(i);
    }
    state = std::make_unique<duckdb::ParquetReaderScanState>();
    reader->InitializeScan(*state, column_indices, row_group_ids, nullptr);
    duckdb_output = std::make_unique<duckdb::DataChunk>();
    duckdb_output->Initialize(row_type);
}

Chunk local_engine::ParquetRowInputFormat::generate()
{
    if (!reader)
        prepareReader();

    // don't use buffer
    if (prefer_block_size == 0)
    {
        auto chunk = getNextChunk();
        return chunk;
    }
    else
    {
        while(buffer.size() < prefer_block_size)
        {
            auto res = getNextChunk();
            if (!res.hasRows())
                break ;
            buffer.add(res, 0, buffer.size());
        }
        return buffer.releaseColumns();
    }
}
duckdb::LogicalType local_engine::ParquetRowInputFormat::convertCHTypeToDuckDbType(DataTypePtr type)
{
    WhichDataType which(type);
    if (which.isInt8())
    {
        return duckdb::LogicalType(duckdb::LogicalType::TINYINT);
    }
    else if (which.isInt16())
    {
        return duckdb::LogicalType(duckdb::LogicalType::SMALLINT);
    }
    else if (which.isInt32())
    {
        return duckdb::LogicalType(duckdb::LogicalType::INTEGER);
    }
    else if (which.isInt64())
    {
        return duckdb::LogicalType(duckdb::LogicalType::BIGINT);
    }
    else if (which.isString())
    {
        return duckdb::LogicalType(duckdb::LogicalType::VARCHAR);
    }
    else if (which.isFloat32())
    {
        return duckdb::LogicalType(duckdb::LogicalType::FLOAT);
    }
    else if (which.isFloat64())
    {
        return duckdb::LogicalType(duckdb::LogicalType::DOUBLE);
    }
    else if (which.isDate())
    {
        return duckdb::LogicalType(duckdb::LogicalType::DATE);
    }
    else {
        throw std::runtime_error("doesn't support CH type " + type->getName());
    }
}
void local_engine::ParquetRowInputFormat::duckDbChunkToCHChunk(duckdb::DataChunk & dataChunk, Chunk & chunk)
{
    Columns columns_list;
    UInt64 num_rows = 0;
    auto header = this->getPort().getHeader();

    columns_list.reserve(header.columns());

    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);
        MutableColumnPtr read_column = header_column.type->createColumn();
        readColumnFromDuckVector(*read_column, dataChunk.data[column_i], dataChunk.size());
        ColumnWithTypeAndName column;
        column.name = header_column.name;
        column.type = header_column.type;
        column.column = std::move(read_column);

        num_rows = column.column->size();
        columns_list.push_back(std::move(column.column));
    }
    chunk.setColumns(columns_list, num_rows);
}
void local_engine::ParquetRowInputFormat::readColumnFromDuckVector(IColumn & internal_column, duckdb::Vector & vector, idx_t num_rows)
{
    switch(vector.GetType().id())
    {

        case duckdb::LogicalTypeId::TINYINT:
            fillColumnWithNumericData<int8_t, ColumnVector<int8_t>>(vector, internal_column, num_rows);
            break;
        case duckdb::LogicalTypeId::SMALLINT:
            fillColumnWithNumericData<int16_t, ColumnVector<int16_t>>(vector, internal_column, num_rows);
            break;
        case duckdb::LogicalTypeId::INTEGER:
            fillColumnWithNumericData<int32_t, ColumnVector<int32_t>>(vector, internal_column, num_rows);
            break;
        case duckdb::LogicalTypeId::BIGINT:
            fillColumnWithNumericData<int64_t, ColumnVector<int64_t>>(vector, internal_column, num_rows);
            break;
        case duckdb::LogicalTypeId::DATE:
            fillColumnWithDate32Data(vector, internal_column, num_rows);
            break;
        case duckdb::LogicalTypeId::FLOAT:
            fillColumnWithNumericData<float_t, ColumnVector<float_t>>(vector, internal_column, num_rows);
            break;
        case duckdb::LogicalTypeId::DOUBLE:
            fillColumnWithNumericData<double_t, ColumnVector<double_t>>(vector, internal_column, num_rows);
            break;
        case duckdb::LogicalTypeId::VARCHAR:
            fillColumnWithStringData(vector, internal_column, num_rows);
            break;
        default:
            throw std::runtime_error("unsupported type " + LogicalTypeIdToString(vector.GetType().id()));
    }
}
template <typename NumericType, typename VectorType>
void local_engine::ParquetRowInputFormat::fillColumnWithNumericData(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows)
{
    auto & column_data = static_cast<VectorType &>(internal_column).getData();
    column_data.reserve(num_rows);
    const auto * raw_data = reinterpret_cast<const NumericType *>(vector.GetData());
    column_data.insert_assume_reserved(raw_data, raw_data + num_rows);
}
void local_engine::ParquetRowInputFormat::fillColumnWithStringData(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows)
{
    assert(vector.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
    auto* duck_data = duckdb::FlatVector::GetData<duckdb::string_t>(vector);
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(internal_column).getOffsets();
    column_offsets.reserve(num_rows);
    size_t chars_t_size = 0;

    for (idx_t i = 0; i < num_rows; ++i)
    {
        chars_t_size += duck_data[i].GetSize();
    }
    chars_t_size += num_rows;
    column_chars_t.reserve(chars_t_size);
    column_offsets.reserve(num_rows);
    for (idx_t i = 0; i < num_rows; ++i)
    {
        column_chars_t.insert_assume_reserved(duck_data[i].GetDataUnsafe(), duck_data[i].GetDataUnsafe() + duck_data[i].GetSize());
        column_chars_t.emplace_back('\0');
        column_offsets.emplace_back(column_chars_t.size());
    }
}
void local_engine::ParquetRowInputFormat::fillColumnWithDate32Data(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows)
{
    PaddedPODArray<UInt16> & column_data = assert_cast<ColumnVector<UInt16> &>(internal_column).getData();
    column_data.reserve(num_rows);
    auto* duck_data = duckdb::FlatVector::GetData<duckdb::date_t>(vector);
    for (idx_t i = 0; i < num_rows; ++i)
    {
        UInt16 days_num = static_cast<UInt16>(duck_data[i].days);
        if (days_num > DATE_LUT_MAX_DAY_NUM)
            throw std::runtime_error("data is out of range (ClickHouse Date)");
        column_data.emplace_back(days_num);
    }
}
void local_engine::ParquetRowInputFormat::resetParser()
{
    IInputFormat::resetParser();
    state.reset();
    state = std::make_unique<duckdb::ParquetReaderScanState>();
}
Chunk local_engine::ParquetRowInputFormat::getNextChunk()
{
    Chunk res;
    duckdb_output->Reset();
    reader->Scan(*state, *duckdb_output);
    if (duckdb_output->size() > 0)
    {
        duckDbChunkToCHChunk(*duckdb_output, res);
    }
    return res;
}
