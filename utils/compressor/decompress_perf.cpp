#include <lz4.h>
#include <string.h>
#include <random>
#include <common/likely.h>
#include <common/Types.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedStream.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/memcpySmall.h>
#include <common/unaligned.h>


/** for i in *.bin; do ./decompress_perf < $i > /dev/null; done
  */


static void LZ4_wildCopy(UInt8 * dst, const UInt8 * src, UInt8 * dst_end)
{
    do
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));

        dst += 16;
        src += 16;
    } while (dst < dst_end);
}


void LZ4_decompress_faster(
                 const char * const source,
                 char * const dest,
                 size_t dest_size)
{
    const UInt8 * ip = (UInt8 *)source;
    UInt8 * op = (UInt8 *)dest;
    UInt8 * const output_end = op + dest_size;

    while (1)
    {
        size_t length;

        auto continue_read_length = [&]
        {
            unsigned s;
            do
            {
                s = *ip++;
                length += s;
            } while (unlikely(s == 255));
        };

        /// Get literal length.

        auto token = *ip++;
        length = token >> 4;
        if (length == 0x0F)
            continue_read_length();

        /// Copy literals.

        UInt8 * copy_end = op + length;
        LZ4_wildCopy(op, ip, copy_end);
        ip += length;
        op = copy_end;

        if (copy_end > output_end)
            return;

        /// Get match offset.

        size_t offset = unalignedLoad<UInt16>(ip);
        ip += 2;
        const UInt8 * match = op - offset;

        /// Get match length.

        length = token & 0x0F;
        if (length == 0x0F)
            continue_read_length();
        length += 4;

        /// Copy match within block, that produce overlapping pattern.

        copy_end = op + length;

        if (unlikely(offset < 16))
        {
            op[0] = match[0];
            op[1] = match[1];
            op[2] = match[2];
            op[3] = match[3];
            op[4] = match[4];
            op[5] = match[5];
            op[6] = match[6];
            op[7] = match[7];
            op[8] = match[8];
            op[9] = match[9];
            op[10] = match[10];
            op[11] = match[11];
            op[12] = match[12];
            op[13] = match[13];
            op[14] = match[14];
            op[15] = match[15];

            op += 16;

            /// 16 % N
            const unsigned shift[] = { 0, 0, 0, 1, 0, 1, 4, 2, 0, 7, 6, 5, 4, 3, 2, 1 };
            match += shift[offset];
        }

        LZ4_wildCopy(op, match, copy_end);
        op = copy_end;
    }
}


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
            own_compressed_buffer.resize(size_compressed);
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
            LZ4_decompress_faster(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed);
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

        memory.resize(size_decompressed + 15);
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

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true);
    return DB::getCurrentExceptionCode();
}
