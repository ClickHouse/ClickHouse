#pragma once

//#define STANDARD_VECTOR_SIZE 1024 * 4

#include <Processors/Formats/IInputFormat.h>
#include <Columns/ColumnVector.h>

#include <External/parquet-amalgamation.hpp>
#include <External/duckdb.hpp>
#include <Common/ChunkBuffer.h>
#include "InputStreamFileSystem.h"

using namespace DB;

namespace local_engine
{
class ParquetRowInputFormat : public IInputFormat
{
public:
    ParquetRowInputFormat(ReadBuffer & in_, Block header_, size_t prefer_block_size = 0);

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    void prepareReader();
    static duckdb::LogicalType convertCHTypeToDuckDbType(DataTypePtr type);
    void duckDbChunkToCHChunk(duckdb::DataChunk & dataChunk, Chunk & chunk);
    void readColumnFromDuckVector(IColumn & internal_column, duckdb::Vector & vector, idx_t num_rows);
    Chunk getNextChunk();
    template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
    static void fillColumnWithNumericData(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows);
    static void fillColumnWithStringData(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows);
    static void fillColumnWithDate32Data(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows);

    static std::unique_ptr<InputStreamFileSystem> inputStreamFileSystem;
    duckdb::Allocator allocator;
    std::string cache_data;
    std::unique_ptr<ReadBuffer> cached_reader;
    std::unique_ptr<duckdb::ParquetReader> reader;
    std::unique_ptr<duckdb::ParquetReaderScanState> state;
    std::vector<duckdb::column_t> column_indices;
    std::vector<idx_t> row_group_ids;
    std::vector<::duckdb::LogicalType> row_type;
    local_engine::ChunkBuffer buffer;
    // Used for insert data, the max row number is not exactly equal to this value.
    const size_t prefer_block_size = 0;
    std::unique_ptr<duckdb::DataChunk> duckdb_output;
};
}



