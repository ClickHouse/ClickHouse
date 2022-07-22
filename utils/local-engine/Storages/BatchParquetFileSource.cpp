#include "BatchParquetFileSource.h"

#include <IO/ReadBufferFromFile.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ArrowParquetBlockInputFormat.h>
#include <Poco/URI.h>
#include <Disks/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <IO/ReadBufferFromAzureBlobStorage.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadSettings.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3Settings.h>

using namespace DB;

namespace local_engine
{
#if USE_AWS_S3

std::shared_ptr<Aws::S3::S3Client> getClient(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        config.getString(config_prefix + ".region", ""),
        context->getRemoteHostFilter(), context->getGlobalContext()->getSettingsRef().s3_max_redirects);

    S3::URI uri(Poco::URI(config.getString(config_prefix + ".endpoint")));
//    if (uri.key.back() != '/')
//        throw Exception("S3 path must ends with '/', but '" + uri.key + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);

    client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 10000);
    client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 5000);
    client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
    client_configuration.endpointOverride = uri.endpoint;

    client_configuration.retryStrategy
        = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.getUInt(config_prefix + ".retry_attempts", 10));

    return S3::ClientFactory::instance().create(
        client_configuration,
        uri.is_virtual_hosted_style,
        config.getString(config_prefix + ".access_key_id", ""),
        config.getString(config_prefix + ".secret_access_key", ""),
        config.getString(config_prefix + ".server_side_encryption_customer_key_base64", ""),
        {},
        config.getBool(config_prefix + ".use_environment_credentials", config.getBool("s3.use_environment_credentials", false)),
        config.getBool(config_prefix + ".use_insecure_imds_request", config.getBool("s3.use_insecure_imds_request", false)));
}
#endif

BatchParquetFileSource::BatchParquetFileSource(FilesInfoPtr files, const DB::Block & sample, const DB::ContextPtr & context_)
    : SourceWithProgress(sample), files_info(files), header(sample), context(context_)
{
}
DB::Chunk BatchParquetFileSource::generate()
{
    while (!finished_generate)
    {
        /// Open file lazily on first read. This is needed to avoid too many open files from different streams.
        if (!reader)
        {
            auto current_file = files_info->next_file_to_read.fetch_add(1);
            if (current_file >= files_info->files.size())
            {
                finished_generate = true;
                return {};
            }

            current_path = files_info->files[current_file];

            read_buf = getReadBufferFromFileURI(current_path);
            ProcessorPtr format = std::make_shared<local_engine::ArrowParquetBlockInputFormat>(*read_buf, header, DB::FormatSettings());
            //            auto format = DB::ParquetBlockInputFormat::getParquetFormat(*read_buf, header);
            QueryPipelineBuilder builder;
            builder.init(Pipe(format));
            pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

            reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        }

        Chunk chunk;
        if (reader->pull(chunk))
        {
            return chunk;
        }

        /// Close file prematurely if stream was ended.
        reader.reset();
        pipeline.reset();
        read_buf.reset();
    }

    return {};
}
std::unique_ptr<ReadBuffer> BatchParquetFileSource::getReadBufferFromFileURI(const String & file)
{
    Poco::URI file_uri(file);
    const auto & schema = file_uri.getScheme();
    if (schema == "file")
    {
        return getReadBufferFromLocal(file_uri.getPath());
    }
#if USE_AWS_S3
    else if (schema == "s3")
    {
        return getReadBufferFromS3(file_uri.getHost(), file_uri.getPath().substr(1));
    }
#endif
#if USE_AZURE_BLOB_STORAGE
    else if (schema == "wasb" || schema == "wasbs")
    {
        return getReadBufferFromBlob(file_uri.getPath().substr(1));
    }
#endif
    else
    {
        throw std::runtime_error("unsupported schema " + schema);
    }
}
std::unique_ptr<ReadBuffer> BatchParquetFileSource::getReadBufferFromLocal(const String & file)
{
    std::unique_ptr<ReadBuffer> read_buffer;

    struct stat file_stat
    {
    };

    /// Check if file descriptor allows random reads (and reading it twice).
    if (0 != stat(file.c_str(), &file_stat))
        throw std::runtime_error("Cannot stat file " + file);

    if (S_ISREG(file_stat.st_mode))
        read_buffer = std::make_unique<ReadBufferFromFilePRead>(file);
    else
        read_buffer = std::make_unique<ReadBufferFromFile>(file);
    return read_buffer;
}

#if USE_AWS_S3
std::unique_ptr<ReadBuffer> BatchParquetFileSource::getReadBufferFromS3(const String &bucket, const String &key)
{
    if (!s3_client)
    {
        s3_client = getClient(context->getConfigRef(), "s3", context);
    }
    return std::make_unique<ReadBufferFromS3>(s3_client, bucket, key, 3, ReadSettings());
}
#endif

#if USE_AZURE_BLOB_STORAGE
std::unique_ptr<ReadBuffer> BatchParquetFileSource::getReadBufferFromBlob(const String & file)
{
    if (!blob_container_client)
    {
        blob_container_client = getAzureBlobContainerClient(context->getConfigRef(), "blob");
    }
    return std::make_unique<ReadBufferFromAzureBlobStorage>(blob_container_client, file, 5, 5, DBMS_DEFAULT_BUFFER_SIZE);
}
#endif

}
