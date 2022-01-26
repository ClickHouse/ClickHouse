#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Columns/ColumnVector.h>

#include "parquet-amalgamation.hpp"
#include "duckdb.hpp"
#include "InputStreamFileSystem.h"

using namespace DB;

namespace local_engine
{
class ParquetRowInputFormat : public IInputFormat
{
public:
    ParquetRowInputFormat(ReadBuffer & in_, Block header_);

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    void prepareReader();
    static duckdb::LogicalType convertCHTypeToDuckDbType(DataTypePtr type);
    void duckDbChunkToCHChunk(duckdb::DataChunk & dataChunk, Chunk & chunk);
    void readColumnFromDuckVector(IColumn & internal_column, duckdb::Vector & vector, idx_t num_rows);
    template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
    static void fillColumnWithNumericData(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows);
    static void fillColumnWithStringData(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows);
    static void fillColumnWithDate32Data(duckdb::Vector & vector, IColumn & internal_column, idx_t num_rows);

    static std::unique_ptr<InputStreamFileSystem> inputStreamFileSystem;
    duckdb::Allocator allocator;
    std::unique_ptr<duckdb::ParquetReader> reader;
    std::unique_ptr<duckdb::ParquetReaderScanState> state;
    std::vector<duckdb::column_t> column_indices;
    std::vector<idx_t> row_group_ids;
    std::vector<::duckdb::LogicalType> row_type;
    ::duckdb::DataChunk output;
};
}



