#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

#if USE_AZURE_BLOB_STORAGE
#include <azure/storage/blobs.hpp>
#endif

#if USE_AWS_S3
#include <aws/s3/S3Client.h>
#endif


namespace local_engine
{

struct FilesInfo
{
    std::vector<std::string> files;
    std::atomic<size_t> next_file_to_read = 0;
};
using FilesInfoPtr = std::shared_ptr<FilesInfo>;

class BatchParquetFileSource : public DB::SourceWithProgress
{
public:
    BatchParquetFileSource(FilesInfoPtr files, const DB::Block & header, const DB::ContextPtr & context_);

private:
    String getName() const override
    {
        return "BatchParquetFileSource";
    }

    std::unique_ptr<DB::ReadBuffer> getReadBufferFromFileURI(const String &file);

    std::unique_ptr<DB::ReadBuffer> getReadBufferFromLocal(const String &file);

#if USE_AWS_S3
    std::unique_ptr<DB::ReadBuffer> getReadBufferFromS3(const String &bucket, const String &key);
#endif

#if USE_AZURE_BLOB_STORAGE
    std::unique_ptr<DB::ReadBuffer> getReadBufferFromBlob(const String &file);
#endif

protected:
    DB::Chunk generate() override;

private:
    FilesInfoPtr files_info;
    std::unique_ptr<DB::ReadBuffer> read_buf;
    DB::QueryPipeline pipeline;
    std::unique_ptr<DB::PullingPipelineExecutor> reader;
    bool finished_generate = false;
    std::string current_path;
    DB::Block header;
    DB::ContextPtr context;
#if USE_AZURE_BLOB_STORAGE
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
#endif

#if USE_AWS_S3
    std::shared_ptr<Aws::S3::S3Client> s3_client;
#endif

};

using BatchParquetFileSourcePtr = std::shared_ptr<BatchParquetFileSource>;
}


