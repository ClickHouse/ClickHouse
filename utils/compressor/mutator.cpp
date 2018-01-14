#include <string.h>
#include <random>
#include <pcg_random.hpp>
#include <common/likely.h>
#include <common/Types.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedStream.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/PODArray.h>

/** Quick and dirty implementation of data scrambler.
  *
  * The task is to replace the data with pseudorandom values.
  * But with keeping some probability distributions
  *  and with maintaining the same compression ratio.
  *
  * The solution is to operate directly on compressed LZ4 stream.
  * The stream consists of independent compressed blocks.
  * Each block is a stream of "literals" and "matches".
  * Liteal is an instruction to literally put some following bytes,
  *  and match is an instruction to copy some bytes that was already seen before.
  *
  * We get literals and apply some scramble operation on it.
  * But we keep literal length and matches without changes.
  *
  * That's how we get pseudorandom data but with keeping
  *  all repetitive patterns and maintaining the same compression ratio.
  *
  * Actually, the compression ratio, if you decompress scrambled data and compress again
  *  become slightly worse, because LZ4 use simple match finder based on value of hash function,
  *  and it can find different matches due to collisions in hash function.
  *
  * Scramble operation replace literals with pseudorandom bytes,
  *  but with some heuristics to keep some sort of data structure.
  *
  * It's in question, is it scramble data enough and while is it safe to publish scrambled data.
  * In general, you should assume that it is not safe.
  */


#define ML_BITS  4
#define ML_MASK  ((1U<<ML_BITS)-1)
#define RUN_BITS (8-ML_BITS)
#define RUN_MASK ((1U<<RUN_BITS)-1)

#define MINMATCH 4
#define WILDCOPYLENGTH 8
#define LASTLITERALS 5


static UInt8 rand(pcg64 & generator, UInt8 min, UInt8 max)
{
    return min + generator() % (max + 1 - min);
}

static void mutate(pcg64 & generator, void * src, size_t length)
{
    UInt8 * pos = static_cast<UInt8 *>(src);
    UInt8 * end = pos + length;

    while (pos < end)
    {
        if (pos + strlen("https") <= end && 0 == memcmp(pos, "https", strlen("https")))
        {
            pos += strlen("https");
            continue;
        }

        if (pos + strlen("http") <= end && 0 == memcmp(pos, "http", strlen("http")))
        {
            pos += strlen("http");
            continue;
        }

        if (pos + strlen("www") <= end && 0 == memcmp(pos, "www", strlen("www")))
        {
            pos += strlen("www");
            continue;
        }

        if (*pos >= '1' && *pos <= '9')
            *pos = rand(generator, '1', '9');
        else if (*pos >= 'a' && *pos <= 'z')
            *pos = rand(generator, 'a', 'z');
        else if (*pos >= 'A' && *pos <= 'Z')
            *pos = rand(generator, 'A', 'Z');
        else if (*pos >= 0x80 && *pos <= 0xBF)
            *pos = rand(generator, *pos & 0xF0U, *pos | 0x0FU);
        else if (*pos == '\\')
            ++pos;

        ++pos;
    }

    pos = static_cast<UInt8 *>(src);
    while (pos < end)
    {
        if (pos + 3 <= end
            && isAlphaASCII(pos[0])
            && !isAlphaASCII(pos[1]) && pos[1] != '\\' && pos[1] >= 0x20
            && isAlphaASCII(pos[2]))
        {
            auto res = rand(generator, 0, 3);
            if (res == 2)
                std::swap(pos[0], pos[1]);
            if (res == 3)
                std::swap(pos[1], pos[2]);

            pos += 3;
        }
        else if (pos + 5 <= end
            && pos[0] >= 0xC0 && pos[0] <= 0xDF && pos[1] >= 0x80 && pos[1] <= 0xBF
            && pos[2] >= 0x20 && pos[2] < 0x80 && !isAlphaASCII(pos[2])
            && pos[3] >= 0xC0 && pos[0] <= 0xDF && pos[4] >= 0x80 && pos[4] <= 0xBF)
        {
            auto res = rand(generator, 0, 3);
            if (res == 2)
            {
                std::swap(pos[1], pos[2]);
                std::swap(pos[0], pos[1]);
            }
            if (res == 3)
            {
                std::swap(pos[3], pos[2]);
                std::swap(pos[4], pos[3]);
            }

            pos += 5;
        }
        else
            ++pos;
    }
}


static void LZ4_copy8(void* dst, const void* src)
{
    memcpy(dst,src,8);
}

/* customized variant of memcpy, which can overwrite up to 8 bytes beyond dstEnd */
static void LZ4_wildCopy(void* dstPtr, const void* srcPtr, void* dstEnd)
{
    UInt8* d = (UInt8*)dstPtr;
    const UInt8* s = (const UInt8*)srcPtr;
    UInt8* const e = (UInt8*)dstEnd;

    do { LZ4_copy8(d,s); d+=8; s+=8; } while (d<e);
}


static UInt16 LZ4_read16(const void* memPtr)
{
    UInt16 val; memcpy(&val, memPtr, sizeof(val)); return val;
}


static void LZ4_write32(void* memPtr, UInt32 value)
{
    memcpy(memPtr, &value, sizeof(value));
}


int LZ4_decompress_mutate(
                 char* const source,
                 char* const dest,
                 int outputSize)
{
    pcg64 generator;

    /* Local Variables */
    UInt8* ip = (UInt8*) source;

    UInt8* op = (UInt8*) dest;
    UInt8* const oend = op + outputSize;
    UInt8* cpy;

    const unsigned dec32table[] = {0, 1, 2, 1, 4, 4, 4, 4};
    const int dec64table[] = {0, 0, 0, -1, 0, 1, 2, 3};

    /* Main Loop : decode sequences */
    while (1) {
        size_t length;
        const UInt8* match;
        size_t offset;

        /* get literal length */
        unsigned const token = *ip++;
        if ((length=(token>>ML_BITS)) == RUN_MASK) {
            unsigned s;
            do {
                s = *ip++;
                length += s;
            } while (s==255);
        }

        /* copy literals */
        cpy = op+length;
        if (cpy>oend-WILDCOPYLENGTH)
        {
            if (cpy != oend) goto _output_error;       /* Error : block decoding must stop exactly there */
            mutate(generator, ip, length);
            memcpy(op, ip, length);
            ip += length;
            op += length;
            break;     /* Necessarily EOF, due to parsing restrictions */
        }
        mutate(generator, ip, cpy - op);
        LZ4_wildCopy(op, ip, cpy);
        ip += length; op = cpy;

        /* get offset */
        offset = LZ4_read16(ip); ip+=2;
        match = op - offset;
        LZ4_write32(op, (UInt32)offset);   /* costs ~1%; silence an msan warning when offset==0 */

        /* get matchlength */
        length = token & ML_MASK;
        if (length == ML_MASK) {
            unsigned s;
            do {
                s = *ip++;
                length += s;
            } while (s==255);
        }
        length += MINMATCH;

        /* copy match within block */
        cpy = op + length;
        if (unlikely(offset<8)) {
            const int dec64 = dec64table[offset];
            op[0] = match[0];
            op[1] = match[1];
            op[2] = match[2];
            op[3] = match[3];
            match += dec32table[offset];
            memcpy(op+4, match, 4);
            match -= dec64;
        } else { LZ4_copy8(op, match); match+=8; }
        op += 8;

        if (unlikely(cpy>oend-12)) {
            UInt8* const oCopyLimit = oend-(WILDCOPYLENGTH-1);
            if (cpy > oend-LASTLITERALS) goto _output_error;    /* Error : last LASTLITERALS bytes must be literals (uncompressed) */
            if (op < oCopyLimit) {
                LZ4_wildCopy(op, match, oCopyLimit);
                match += oCopyLimit - op;
                op = oCopyLimit;
            }
            while (op<cpy) *op++ = *match++;
        } else {
            LZ4_copy8(op, match);
            if (length>16) LZ4_wildCopy(op+8, match+8, cpy);
        }
        op=cpy;   /* correction */
    }

    return (int) (((const char*)ip)-source);   /* Nb of input bytes read */

    /* Overflow error detected */
_output_error:
    return (int) (-(((const char*)ip)-source))-1;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int CANNOT_DECOMPRESS;
}

class MutatingCompressedReadBufferBase
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
            if (LZ4_decompress_mutate(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed) < 0)
                throw Exception("Cannot LZ4_decompress_fast", ErrorCodes::CANNOT_DECOMPRESS);
        }
        else
            throw Exception("Unknown compression method: " + toString(method), ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

public:
    /// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
    MutatingCompressedReadBufferBase(ReadBuffer * in = nullptr)
        : compressed_in(in), own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE)
    {
    }
};


class MutatingCompressedReadBuffer : public MutatingCompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
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

        memory.resize(size_decompressed);
        working_buffer = Buffer(&memory[0], &memory[size_decompressed]);

        decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

        return true;
    }

public:
    MutatingCompressedReadBuffer(ReadBuffer & in_)
        : MutatingCompressedReadBufferBase(&in_), BufferWithOwnMemory<ReadBuffer>(0)
    {
    }
};

}


int main(int, char **)
try
{
    DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
    DB::MutatingCompressedReadBuffer mutating_in(in);
    DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    DB::copyData(mutating_in, out);

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true);
    return DB::getCurrentExceptionCode();
}
