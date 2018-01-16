#include <lz4.h>
#include <string.h>
#include <common/likely.h>
#include <common/Types.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedStream.h>
#include <IO/WriteHelpers.h>
#include <IO/LZ4_decompress_faster.h>
#include <IO/copyData.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/memcpySmall.h>
#include <common/unaligned.h>


/** for i in *.bin; do ./decompress_perf < $i > /dev/null; done
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int CANNOT_DECOMPRESS;
}

class FasterCompressedReadBufferBase
{
protected:
    ReadBuffer * compressed_in;

    /// If 'compressed_in' buffer has whole compressed block - then use it. Otherwise copy parts of data to 'own_compressed_buffer'.
    PODArray<char> own_compressed_buffer;
    /// Points to memory, holding compressed block.
    char * compressed_buffer = nullptr;

    LZ4::StreamStatistics stream_stat;
    LZ4::PerformanceStatistics perf_stat;

    size_t readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
    {
        if (compressed_in->eof())
            return 0;

        CityHash_v1_0_2::uint128 checksum;
        compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

        own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
        compressed_in->readStrict(&own_compressed_buffer[0], COMPRESSED_BLOCK_HEADER_SIZE);

        UInt8 method = own_compressed_buffer[0];    /// See CompressedWriteBuffer.h

        size_t & size_compressed = size_compressed_without_checksum;

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4) ||
            method == static_cast<UInt8>(CompressionMethodByte::ZSTD) ||
            method == static_cast<UInt8>(CompressionMethodByte::NONE))
        {
            size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
            size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);
        }
        else
            throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw Exception("Too large size_compressed. Most likely corrupted data.", ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

        /// Is whole compressed block located in 'compressed_in' buffer?
        if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE &&
            compressed_in->position() + size_compressed - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
        {
            compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
            compressed_buffer = compressed_in->position();
            compressed_in->position() += size_compressed;
        }
        else
        {
            own_compressed_buffer.resize(size_compressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
            compressed_buffer = &own_compressed_buffer[0];
            compressed_in->readStrict(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
        }

        return size_compressed + sizeof(checksum);
    }

    void decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
    {
        UInt8 method = compressed_buffer[0];    /// See CompressedWriteBuffer.h

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4))
        {
            //LZ4::statistics(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed, stat);
            LZ4::decompress(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_compressed_without_checksum, size_decompressed, perf_stat);
        }
        else
            throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

public:
    /// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
    FasterCompressedReadBufferBase(ReadBuffer * in = nullptr)
        : compressed_in(in), own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE)
    {
    }

    LZ4::StreamStatistics getStreamStatistics() const { return stream_stat; }
    LZ4::PerformanceStatistics getPerformanceStatistics() const { return perf_stat; }
};


class FasterCompressedReadBuffer : public FasterCompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
    size_t size_compressed = 0;

    bool nextImpl() override
    {
        size_t size_decompressed;
        size_t size_compressed_without_checksum;
        size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
        if (!size_compressed)
            return false;

        memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
        working_buffer = Buffer(&memory[0], &memory[size_decompressed]);

        decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

        return true;
    }

public:
    FasterCompressedReadBuffer(ReadBuffer & in_)
        : FasterCompressedReadBufferBase(&in_), BufferWithOwnMemory<ReadBuffer>(0)
    {
    }
};

}


int main(int, char **)
try
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    FasterCompressedReadBuffer decompressing_in(in);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    HashingWriteBuffer hashing_out(out);

    Stopwatch watch;
    copyData(decompressing_in, hashing_out);
    watch.stop();

    auto hash = hashing_out.getHash();

    double seconds = watch.elapsedSeconds();
    std::cerr << std::fixed << std::setprecision(3)
        << "Elapsed: " << seconds
        << ", " << formatReadableSizeWithBinarySuffix(in.count()) << " compressed"
        << ", " << formatReadableSizeWithBinarySuffix(decompressing_in.count()) << " decompressed"
        << ", ratio: " << static_cast<double>(decompressing_in.count()) / in.count()
        << ", " << formatReadableSizeWithBinarySuffix(in.count() / seconds) << "/sec. compressed"
        << ", " << formatReadableSizeWithBinarySuffix(decompressing_in.count() / seconds) << "/sec. decompressed"
        << ", checksum: " << hash.first << "_" << hash.second
        << "\n";

//    decompressing_in.getStatistics().print();

    LZ4::PerformanceStatistics perf_stat = decompressing_in.getPerformanceStatistics();

    for (size_t i = 0; i < LZ4::PerformanceStatistics::NUM_ELEMENTS; ++i)
    {
        const LZ4::PerformanceStatistics::Element & elem = perf_stat.data[i];

        std::cerr << "Variant " << i << ": "
            << "count: " << elem.count
            << ", mean ns/b: " << 1000000000.0 * elem.mean()
            << ", sigma ns/b: " << 1000000000.0 * elem.sigma()
            << "\n";
    }

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true);
    return DB::getCurrentExceptionCode();
}
