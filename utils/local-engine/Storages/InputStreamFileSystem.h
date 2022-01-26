#pragma once

#include "duckdb.hpp"
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>

using namespace DB;

namespace local_engine
{
class InputStreamFileSystem;

class InputStreamFileHandle :  public duckdb::FileHandle
{
public:
    InputStreamFileHandle(::duckdb::FileSystem& fileSystem, int id)
        : FileHandle(fileSystem, std::to_string(id)),
        stream_id(id),
        offset_(0) {}

    int getStreamId()
    {
        return stream_id;
    }

    uint64_t offset() const {
        return offset_;
    }

    void setOffset(uint64_t newOffset) {
        offset_ = newOffset;
    }

protected:
    void Close() override
    {
        // do nothing
    }

private:
    int stream_id;
    uint64_t offset_;
};

class InputStreamFileSystem : public ::duckdb::FileSystem
{
public:
    InputStreamFileSystem() {}
    ~InputStreamFileSystem() override = default;

    std::unique_ptr<::duckdb::FileHandle> OpenFile(
        const std::string& path,
        uint8_t /*flags*/,
        ::duckdb::FileLockType /*lock = ::duckdb::FileLockType::NO_LOCK*/,
        ::duckdb::FileCompressionType /*compression =
          ::duckdb::FileCompressionType::UNCOMPRESSED*/
        ,
        ::duckdb::FileOpener* /*opener = nullptr*/) override {
        int stream_id = std::stoi(path);

        std::lock_guard<std::mutex> streams_lock(streamsMutex_);
        auto it = streams_.find(stream_id);
        if (it == streams_.end())
        {
            throw std::runtime_error("Unknown stream with ID " + path);
        }
        ++it->second.first;
        return std::make_unique<InputStreamFileHandle>(*this, stream_id);
    }

    std::unique_ptr<::duckdb::FileHandle> openStream(
        ReadBuffer & stream) {
        std::lock_guard<std::mutex> lock(streamsMutex_);
        auto stream_id = nextStreamId_++;
        streams_.emplace(
            std::make_pair(stream_id, std::make_pair(1, &stream)));
        return std::make_unique<InputStreamFileHandle>(*this, stream_id);
    }

    void Read(
        ::duckdb::FileHandle& handle,
        void* buffer,
        int64_t nr_bytes,
        uint64_t location) override {

        auto& stream_handle = dynamic_cast<InputStreamFileHandle&>(handle);
        auto it = streams_.find(stream_handle.getStreamId());
        if (it == streams_.end())
        {
            std::cout<<"Read cannot find stream ID "<<stream_handle.getStreamId()<<std::endl;
            it = streams_.find(stream_handle.getStreamId());
            if (it == streams_.end())
            {
                throw std::runtime_error("Unknown stream with ID " + std::to_string(stream_handle.getStreamId()));
            }
        }
        auto * read_buffer = dynamic_cast<SeekableReadBuffer *>(it->second.second);
        if (stream_handle.offset() != location)
        {
            read_buffer->seek(location, SEEK_SET);
        }
        read_buffer->read(static_cast<char *>(buffer), nr_bytes);
        stream_handle.setOffset(location + nr_bytes);
    }

    void CloseStream(int stream_id) {
        std::lock_guard<std::mutex> lock(streamsMutex_);
        auto it = streams_.find(stream_id);
        if (it == streams_.end())
        {
            throw std::runtime_error("Unknown stream with ID " + std::to_string(stream_id));
        }
        if (it->second.first == 1) {
            streams_.erase(it);
        } else {
            --it->second.first;
        }
    }

    void Write(
        ::duckdb::FileHandle& /*handle*/,
        void* /*buffer*/,
        int64_t /*nr_bytes*/,
        uint64_t /*location*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::Write");
    }

    int64_t Read(
        ::duckdb::FileHandle& /*handle*/,
        void* /*buffer*/,
        int64_t /*nr_bytes*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::Read");
    }

    int64_t Write(
        ::duckdb::FileHandle& /*handle*/,
        void* /*buffer*/,
        int64_t /*nr_bytes*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::Write");
    }

    int64_t GetFileSize(::duckdb::FileHandle& handle) override {
        auto& stream_handle = dynamic_cast<InputStreamFileHandle&>(handle);
        auto it = streams_.find(stream_handle.getStreamId());
        if (it == streams_.end())
        {
            std::cout<<"GetFileSize cannot find stream ID "<<stream_handle.getStreamId()<<std::endl;
            it = streams_.find(stream_handle.getStreamId());
            if (it == streams_.end())
            {
                throw std::runtime_error("Unknown stream with ID " + std::to_string(stream_handle.getStreamId()));
            }
        }
        if (auto * file_stream = dynamic_cast<ReadBufferFromFileDescriptor*>(it->second.second))
        {
            return file_stream->size();
        }
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::GetFileSize");
    }

    time_t GetLastModifiedTime(::duckdb::FileHandle& /*handle*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::GetLastModifiedTime");
    }

    void Truncate(::duckdb::FileHandle& /*handle*/, int64_t /*new_size*/)
        override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::Truncate");
    }

    bool DirectoryExists(const std::string& /*directory*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::DirectoryExists");
    }

    void CreateDirectory(const std::string& /*directory*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::CreateDirectory");
    }

    void RemoveDirectory(const std::string& /*directory*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::RemoveDirectory");
    }

    bool ListFiles(
        const std::string& /*directory*/,
        const std::function<void(std::string, bool)>& /*callback*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::ListFiles");
    }

    void MoveFile(const std::string& /*source*/, const std::string& /*target*/)
        override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::MoveFile");
    }

    bool FileExists(const std::string& /*filename*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::FileExists");
    }

    void RemoveFile(const std::string& /*filename*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::RemoveFile");
    }

    void FileSync(::duckdb::FileHandle& /*handle*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::FileSync");
    }

    std::vector<std::string> Glob(const std::string& /*path*/) override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::Glob");
    }

    virtual std::string GetName() const override {
        throw std::runtime_error("Unexpected call to InputStreamFileSystem::GetName");
    }
private:
    int nextStreamId_ = 1;
    // Maps stream ID to file handle counter and input stream
    std::unordered_map<
        int,
        std::pair<int, ReadBuffer *>>
        streams_;
    std::mutex streamsMutex_;
};
}
