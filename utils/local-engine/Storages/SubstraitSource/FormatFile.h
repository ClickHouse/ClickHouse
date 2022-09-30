#pragma once
#include <optional>
#include <Processors/Formats/IInputFormat.h>
#include <Interpreters/Context_fwd.h>
#include <substrait/plan.pb.h>
#include <Poco/URI.h>
#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <vector>
#include <memory>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>

namespace local_engine
{
class FormatFile
{
public:
    struct InputFormat
    {
    public:
        explicit InputFormat(DB::InputFormatPtr input_, std::unique_ptr<DB::ReadBuffer> read_buffer_)
            : input(input_), read_buffer(std::move(read_buffer_))
        {
        }
        DB::InputFormatPtr input;
    private:
        std::unique_ptr<DB::ReadBuffer> read_buffer;
    };
    using InputFormatPtr = std::shared_ptr<InputFormat>;

    FormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    virtual ~FormatFile() = default;

    /// create a new input format for reading this file
    virtual InputFormatPtr createInputFormat(const DB::Block & header) = 0;

    /// Spark would split a large file into small segements and read in different tasks
    /// If this file doesn't support the split feacture, only the task with offset 0 will generate data.
    virtual bool supportSplit() { return false; }

    /// try to get rows from file metadata
    virtual std::optional<size_t> getTotalRows() { return {}; }

    /// get partition keys from file path
    inline const std::vector<String> & getFilePartitionKeys() const { return partition_keys; }

    inline const std::map<String, String> & getFilePartitionValues() const { return partition_values; }

    virtual String getURIPath() const { return file_info.uri_file(); }

    virtual size_t getStartOffset() const { return file_info.start(); }
    virtual size_t getLength() const { return file_info.length(); }

protected:
    DB::ContextPtr context;
    substrait::ReadRel::LocalFiles::FileOrFiles file_info;
    ReadBufferBuilderPtr read_buffer_builder;
    std::vector<String> partition_keys;
    std::map<String, String> partition_values;

};
using FormatFilePtr = std::shared_ptr<FormatFile>;
using FormatFiles = std::vector<FormatFilePtr>;

class FormatFileUtil
{
public:
    static FormatFilePtr createFile(DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file);
};
}
