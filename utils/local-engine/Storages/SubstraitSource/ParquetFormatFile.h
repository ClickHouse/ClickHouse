#pragma once
#include <memory>
#include <Storages/SubstraitSource/FormatFile.h>
#include <parquet/arrow/reader.h>
namespace local_engine
{
class ParquetFormatFile : public FormatFile
{
public:
    explicit ParquetFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~ParquetFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;
    std::optional<size_t> getTotalRows() override;
    bool supportSplit() override { return true; }

private:
    std::mutex mutex;
    std::optional<size_t> total_rows;

    std::unique_ptr<parquet::arrow::FileReader> reader;
    void prepareReader();

    std::vector<int> collectRowGroupIndices();
};

}
