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
#include <IO/copyData.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/memcpySmall.h>
#include <common/unaligned.h>


/** for i in *.bin; do ./decompress_perf < $i > /dev/null; done
  */

namespace LZ4
{

static void copy16(UInt8 * dst, const UInt8 * src)
{
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
}

static void wildCopy16(UInt8 * dst, const UInt8 * src, UInt8 * dst_end)
{
    do
    {
        copy16(dst, src);
        dst += 16;
        src += 16;
    } while (dst < dst_end);
}

static void copy8(UInt8 * dst, const UInt8 * src)
{
    memcpy(dst, src, 8);
}

static void wildCopy8(UInt8 * dst, const UInt8 * src, UInt8 * dst_end)
{
    do
    {
        copy8(dst, src);
        dst += 8;
        src += 8;
    } while (dst < dst_end);
}


static void copyOverlap16(UInt8 * op, const UInt8 *& match, const size_t offset)
{
    /// 4 % n.
    static constexpr int shift1[]
        = { 0,  1,  2,  1,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4  };

    /// 8 % n - 4 % n
    static constexpr int shift2[]
        = { 0,  0,  0,  1,  0, -1, -2, -3, -4,  4,  4,  4,  4,  4,  4,  4  };

    /// 16 % n - 8 % n
    static constexpr int shift3[]
        = { 0,  0,  0, -1,  0, -2,  2,  1,  0, -1, -2, -3, -4, -5,  -6, -7 };

    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    match += shift1[offset];
    memcpy(op + 4, match, 4);
    match += shift2[offset];
    memcpy(op + 8, match, 8);
    match += shift3[offset];
}


static void copyOverlap8(UInt8 * op, const UInt8 *& match, const size_t offset)
{
    /// 4 % n.
    static constexpr int shift1[] = {0, 1, 2, 1, 4, 4, 4, 4};

    /// 8 % n - 4 % n
    static constexpr int shift2[] = {0, 0, 0, 1, 0, -1, -2, -3};

    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    match += shift1[offset];
    memcpy(op + 4, match, 4);
    match += shift2[offset];
}


template <size_t N> void copy(UInt8 * dst, const UInt8 * src);
template <> void copy<8>(UInt8 * dst, const UInt8 * src) { copy8(dst, src); };
template <> void copy<16>(UInt8 * dst, const UInt8 * src) { copy16(dst, src); };

template <size_t N> void wildCopy(UInt8 * dst, const UInt8 * src, UInt8 * dst_end);
template <> void wildCopy<8>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy8(dst, src, dst_end); };
template <> void wildCopy<16>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy16(dst, src, dst_end); };

template <size_t N> void copyOverlap(UInt8 * op, const UInt8 *& match, const size_t offset);
template <> void copyOverlap<8>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap8(op, match, offset); };
template <> void copyOverlap<16>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap16(op, match, offset); };


template <size_t copy_amount>
void decompress(
     const char * const __restrict source,
     char * const __restrict dest,
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

        const unsigned token = *ip++;
        length = token >> 4;
        if (length == 0x0F)
            continue_read_length();

        /// Copy literals.

        UInt8 * copy_end = op + length;

        wildCopy<copy_amount>(op, ip, copy_end);

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

        if (unlikely(offset < copy_amount))
        {
            copyOverlap<copy_amount>(op, match, offset);
        }
        else
        {
            copy<copy_amount>(op, match);
            match += copy_amount;
        }

        op += copy_amount;

        copy<copy_amount>(op, match);
        if (length > copy_amount * 2)
            wildCopy<copy_amount>(op + copy_amount, match + copy_amount, copy_end);

        op = copy_end;
    }
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

    LZ4Stat stat;

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
            //LZ4::statistics(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed, stat);
            LZ4::decompress<8>(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed);
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

    LZ4Stat getStatistics() const { return stat; }
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

    decompressing_in.getStatistics().print();

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true);
    return DB::getCurrentExceptionCode();
}
