#include <memory>
#include <type_traits>
#include <unistd.h>
#include <IO/ReadBuffer.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Storages/PartitionCommands.h>
#include <sys/stat.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/config.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}
namespace local_engine
{
FormatFile::FormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : context(context_), file_info(file_info_), read_buffer_builder(read_buffer_builder_)
{
    PartitionValues part_vals = StringUtils::parsePartitionTablePath(file_info.uri_file());
    for (const auto & part : part_vals)
    {
        partition_keys.push_back(part.first);
        partition_values[part.first] = part.second;
    }
}

FormatFilePtr FormatFileUtil::createFile(DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file)
{
    if (file.has_parquet())
    {
        return std::make_shared<ParquetFormatFile>(context, file, read_buffer_builder);
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not suupported:{}", file.DebugString());
    }

    __builtin_unreachable();
}
}
