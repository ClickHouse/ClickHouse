/// Tool to compute the same hash as ClickHouse's getHashOfLoadedBinary() from a file.
/// Useful for cross-compilation when you can't execute the target binary.
///
/// NOTE: This file is intentionally standalone with no ClickHouse dependencies.
/// We cannot reuse src/Common/SipHash.h because it depends on ClickHouse-specific
/// headers (base/types.h, base/extended_types.h, base/unaligned.h, etc.) that
/// require the full ClickHouse build environment. This tool must be compilable
/// with just the host compiler during cross-compilation.

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>

#include <elf.h>


inline uint64_t rotl64(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}


/// Standalone SipHash implementation (SipHash 2-4 variant with 128-bit output).
/// This is a simplified version of ClickHouse's SipHash, producing identical results.
class SipHash
{
private:
    uint64_t v0;
    uint64_t v1;
    uint64_t v2;
    uint64_t v3;
    uint64_t cnt;
    union
    {
        uint64_t current_word;
        uint8_t current_bytes[8];
    };

    void sipround()
    {
        v0 += v1; v1 = rotl64(v1, 13); v1 ^= v0; v0 = rotl64(v0, 32);
        v2 += v3; v3 = rotl64(v3, 16); v3 ^= v2;
        v0 += v3; v3 = rotl64(v3, 21); v3 ^= v0;
        v2 += v1; v1 = rotl64(v1, 17); v1 ^= v2; v2 = rotl64(v2, 32);
    }

    void finalize()
    {
        current_bytes[7] = static_cast<uint8_t>(cnt);

        v3 ^= current_word;
        sipround();
        sipround();
        v0 ^= current_word;

        v2 ^= 0xff;
        sipround();
        sipround();
        sipround();
        sipround();
    }

public:
    SipHash()
    {
        v0 = 0x736f6d6570736575ULL;
        v1 = 0x646f72616e646f6dULL;
        v2 = 0x6c7967656e657261ULL;
        v3 = 0x7465646279746573ULL;
        cnt = 0;
        current_word = 0;
    }

    void update(const char * data, uint64_t size)
    {
        const char * end = data + size;

        if (cnt & 7)
        {
            while ((cnt & 7) && data < end)
            {
                current_bytes[cnt & 7] = static_cast<uint8_t>(*data);
                ++data;
                ++cnt;
            }

            if (cnt & 7)
                return;

            v3 ^= current_word;
            sipround();
            sipround();
            v0 ^= current_word;
        }

        cnt += static_cast<uint64_t>(end - data);

        while (data + 8 <= end)
        {
            memcpy(&current_word, data, 8);

            v3 ^= current_word;
            sipround();
            sipround();
            v0 ^= current_word;

            data += 8;
        }

        current_word = 0;
        switch (end - data)
        {
            case 7: current_bytes[6] = static_cast<uint8_t>(data[6]); [[fallthrough]];
            case 6: current_bytes[5] = static_cast<uint8_t>(data[5]); [[fallthrough]];
            case 5: current_bytes[4] = static_cast<uint8_t>(data[4]); [[fallthrough]];
            case 4: current_bytes[3] = static_cast<uint8_t>(data[3]); [[fallthrough]];
            case 3: current_bytes[2] = static_cast<uint8_t>(data[2]); [[fallthrough]];
            case 2: current_bytes[1] = static_cast<uint8_t>(data[1]); [[fallthrough]];
            case 1: current_bytes[0] = static_cast<uint8_t>(data[0]); [[fallthrough]];
            case 0: break;
            default: break;
        }
    }

    void update(uint64_t x)
    {
        update(reinterpret_cast<const char *>(&x), sizeof(x));
    }

    void get128(uint64_t & lo, uint64_t & hi)
    {
        finalize();
        lo = v0 ^ v1;
        hi = v2 ^ v3;
    }
};


template <typename Ehdr, typename Phdr>
static bool hashExecutableSegments(std::istream & in, SipHash & hash)
{
    Ehdr ehdr;
    in.seekg(0);
    if (!in.read(reinterpret_cast<char *>(&ehdr), sizeof(ehdr)))
    {
        std::cerr << "Failed to read ELF header\n";
        return false;
    }

    for (size_t i = 0; i < ehdr.e_phnum; ++i)
    {
        Phdr phdr;
        in.seekg(static_cast<std::streamoff>(ehdr.e_phoff + i * ehdr.e_phentsize));
        if (!in.read(reinterpret_cast<char *>(&phdr), sizeof(phdr)))
        {
            std::cerr << "Failed to read program header\n";
            return false;
        }

        if (phdr.p_type == PT_LOAD && (phdr.p_flags & PF_X))
        {
            std::cerr << "Found executable segment: offset=0x" << std::hex << phdr.p_offset
                      << ", filesz=0x" << phdr.p_filesz << std::dec
                      << " (" << phdr.p_filesz << " bytes)\n";

            uint64_t filesz = phdr.p_filesz;
            hash.update(filesz);

            in.seekg(static_cast<std::streamoff>(phdr.p_offset));
            std::vector<char> buffer(8 * 1024 * 1024);
            uint64_t remaining = filesz;

            while (remaining > 0)
            {
                size_t to_read = std::min(remaining, static_cast<uint64_t>(buffer.size()));
                if (!in.read(buffer.data(), static_cast<std::streamsize>(to_read)))
                {
                    std::cerr << "Failed to read segment data\n";
                    return false;
                }
                hash.update(buffer.data(), to_read);
                remaining -= to_read;
            }
        }
    }

    return true;
}


static bool hashElfFile(std::istream & in, SipHash & hash)
{
    unsigned char e_ident[EI_NIDENT];
    if (!in.read(reinterpret_cast<char *>(e_ident), EI_NIDENT))
    {
        std::cerr << "Failed to read ELF ident\n";
        return false;
    }

    if (memcmp(e_ident, ELFMAG, SELFMAG) != 0)
    {
        std::cerr << "Not an ELF file\n";
        return false;
    }

    if (e_ident[EI_CLASS] == ELFCLASS64)
        return hashExecutableSegments<Elf64_Ehdr, Elf64_Phdr>(in, hash);
    else
        return hashExecutableSegments<Elf32_Ehdr, Elf32_Phdr>(in, hash);
}


int main(int argc, char ** argv)
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <elf-file>\n"
                  << "\nComputes the same hash as 'clickhouse hash-binary' but from a file.\n"
                  << "Useful for cross-compilation when you can't execute the target binary.\n";
        return 1;
    }

    std::ifstream file(argv[1], std::ios::binary);
    if (!file)
    {
        std::cerr << "Failed to open file: " << argv[1] << "\n";
        return 1;
    }

    SipHash hash;
    if (!hashElfFile(file, hash))
        return 1;

    uint64_t lo;
    uint64_t hi;
    hash.get128(lo, hi);

    /// Output format matches ClickHouse's getHexUIntUppercase for UInt128
    std::cout << std::uppercase << std::hex << std::setfill('0')
              << std::setw(16) << hi << std::setw(16) << lo;
    return 0;
}
