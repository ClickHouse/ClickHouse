#include <memory>
#include <mutex>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Common/ErrorCodes.h>
#include "IO/ReadSettings.h"
#include <IO/ReadBufferFromAzureBlobStorage.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context_fwd.h>
#include <unistd.h>
#include <sys/stat.h>
#include <Poco/URI.h>
#include <IO/S3Common.h>
#include <IO/ReadBufferFromS3.h>
#include <Disks/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <Storages/StorageS3Settings.h>

#include <Poco/Logger.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class LocalFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit LocalFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) {}
    ~LocalFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        const String & file_path = file_uri.getPath();
        struct stat file_stat;
        if (stat(file_path.c_str(), &file_stat))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "file stat failed for {}", file_path);

        if (S_ISREG(file_stat.st_mode))
            read_buffer = std::make_unique<DB::ReadBufferFromFilePRead>(file_path);
        else
            read_buffer = std::make_unique<DB::ReadBufferFromFilePRead>(file_path);
        return read_buffer;
    }
};

class HDFSFileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit HDFSFileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) {}
    ~HDFSFileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        /// Need to set "hdfs.libhdfs3_conf" in global settings
        if (context->getConfigRef().getString("hdfs.libhdfs3_conf", "").empty())
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found hdfs.libhdfs3_conf");
        }
        std::string uriPath = "hdfs://" + file_uri.getHost();
        if (file_uri.getPort())
            uriPath += ":" + std::to_string(file_uri.getPort());
        read_buffer = std::make_unique<DB::ReadBufferFromHDFS>(
            uriPath, file_uri.getPath(), context->getGlobalContext()->getConfigRef());
        return read_buffer;
    }
};

#if USE_AWS_S3
class S3FileReadBufferBuilder : public ReadBufferBuilder
{
public:
    explicit S3FileReadBufferBuilder(DB::ContextPtr context_) : ReadBufferBuilder(context_) {}
    ~S3FileReadBufferBuilder() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info) override
    {
        Poco::URI file_uri(file_info.uri_file());
        auto client = getClient();
        std::unique_ptr<DB::ReadBuffer> readbuffer;
        readbuffer
            = std::make_unique<DB::ReadBufferFromS3>(client, file_uri.getHost(), file_uri.getPath().substr(1), 3, DB::ReadSettings());
        return readbuffer;
    }
private:
    std::shared_ptr<Aws::S3::S3Client> shared_client;

    std::shared_ptr<Aws::S3::S3Client> getClient()
    {
        if (shared_client)
            return shared_client;
        const auto & config = context->getConfigRef();
        String config_prefix = "s3";
        DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
            config.getString(config_prefix + ".region", ""),
            context->getRemoteHostFilter(),
            context->getGlobalContext()->getSettingsRef().s3_max_redirects);

        DB::S3::URI uri(Poco::URI(config.getString(config_prefix + ".endpoint")));

        client_configuration.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 10000);
        client_configuration.requestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 5000);
        client_configuration.maxConnections = config.getUInt(config_prefix + ".max_connections", 100);
        client_configuration.endpointOverride = uri.endpoint;

        client_configuration.retryStrategy
            = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.getUInt(config_prefix + ".retry_attempts", 10));

        shared_client = DB::S3::ClientFactory::instance().create(
            client_configuration,
            uri.is_virtual_hosted_style,
            config.getString(config_prefix + ".access_key_id", ""),
            config.getString(config_prefix + ".secret_access_key", ""),
            config.getString(config_prefix + ".server_side_encryption_customer_key_base64", ""),
            {},
            config.getBool(config_prefix + ".use_environment_credentials", config.getBool("s3.use_environment_credentials", false)),
            config.getBool(config_prefix + ".use_insecure_imds_request", config.getBool("s3.use_insecure_imds_request", false)));
        return shared_client;
    }
};
#endif

#if USE_AZURE_BLOB_STORAGE
class AzureBlobReadBuffer : public ReadBufferBuilder
{
public:
    explicit AzureBlobReadBuffer(DB::ContextPtr context_) : ReadBufferBuilder(context_) {}
    ~AzureBlobReadBuffer() override = default;

    std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info)
    {
        Poco::URI file_uri(file_info.uri_file());
        std::unique_ptr<DB::ReadBuffer> read_buffer;
        read_buffer = std::make_unique<DB::ReadBufferFromAzureBlobStorage>(getClient(), file_uri.getPath(), 5, 5, DBMS_DEFAULT_BUFFER_SIZE);
        return read_buffer;
    }
private:
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> shared_client;

    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> getClient()
    {
        if (shared_client)
            return shared_client;
        shared_client = DB::getAzureBlobContainerClient(context->getConfigRef(), "blob");
        return shared_client;
    }
};
#endif

void registerReadBufferBuildes(ReadBufferBuilderFactory & factory)
{
    LOG_TRACE(&Poco::Logger::get("ReadBufferBuilderFactory"), "+registerReadBufferBuildes");
    factory.registerBuilder("file", [](DB::ContextPtr context_) { return std::make_shared<LocalFileReadBufferBuilder>(context_); });
    factory.registerBuilder("hdfs", [](DB::ContextPtr context_) { return std::make_shared<HDFSFileReadBufferBuilder>(context_); });

#if USE_AWS_S3
    factory.registerBuilder("s3", [](DB::ContextPtr context_) { return std::make_shared<S3FileReadBufferBuilder>(context_); });
#endif

#if USE_AZURE_BLOB_STORAGE
    factory.registerBuilder("wasb", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
    factory.registerBuilder("wasbs", [](DB::ContextPtr context_) { return std::make_shared<AzureBlobReadBuffer>(context_); });
#endif
}

ReadBufferBuilderFactory & ReadBufferBuilderFactory::instance()
{
    static ReadBufferBuilderFactory instance;
    return instance;
}

ReadBufferBuilderPtr ReadBufferBuilderFactory::createBuilder(const String & schema, DB::ContextPtr context)
{
    auto it = builders.find(schema);
    if (it == builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found read buffer builder for {}", schema);
    return it->second(context);
}

void ReadBufferBuilderFactory::registerBuilder(const String & schema, NewBuilder newer)
{
    auto it = builders.find(schema);
    if (it != builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "readbuffer builder for {} has been registered", schema);
    builders[schema] = newer;
}

}
