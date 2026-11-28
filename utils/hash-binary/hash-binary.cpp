/// Tool to compute the same hash as ClickHouse's getHashOfLoadedBinary() from a file.
/// Useful for cross-compilation when you can't execute the target binary.
/// With --write-section, also writes the hash in place into the section '.clickhouse.hash' reserved at link time.
///
/// NOTE: This file is intentionally standalone with no ClickHouse dependencies.
/// We cannot reuse src/Common/SipHash.h because it depends on ClickHouse-specific
/// headers (base/types.h, base/extended_types.h, wide_integer_impl.h, etc.) that
/// pull in boost and other dependencies not available during cross-compilation.
/// This tool must be compilable with just the host compiler.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <elf.h>
#include <fcntl.h>
#include <unistd.h>


namespace
{

uint64_t rotl64(uint64_t value, unsigned int count)
{
    return (value << count) | (value >> (64U - count));
}


/// The whole algorithm is built on wrapping addition of 64-bit words, so the check for unsigned
/// integer overflow (enabled in UBSan builds, which build this tool natively) has to be switched off.
#if defined(__clang__)
#pragma clang attribute push (__attribute__((__no_sanitize__("unsigned-integer-overflow"))), apply_to = function)
#endif

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

        v2 ^= 0xffU;
        sipround();
        sipround();
        sipround();
        sipround();
    }

public:
    SipHash()
        : v0(0x736f6d6570736575ULL)
        , v1(0x646f72616e646f6dULL)
        , v2(0x6c7967656e657261ULL)
        , v3(0x7465646279746573ULL)
        , cnt(0)
        , current_word(0)
    {
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
            default: break;
        }
    }

    void update(uint64_t value)
    {
        update(reinterpret_cast<const char *>(&value), sizeof(value));
    }

    void get128(uint64_t & lo, uint64_t & hi)
    {
        finalize();
        lo = v0 ^ v1;
        hi = v2 ^ v3;
    }
};

#if defined(__clang__)
#pragma clang attribute pop
#endif


template <typename Ehdr, typename Phdr>
bool hashExecutableSegments(std::istream & in, SipHash & hash)
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
        in.seekg(static_cast<std::streamoff>(ehdr.e_phoff + i * static_cast<size_t>(ehdr.e_phentsize)));
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

            const uint64_t filesz = phdr.p_filesz;
            hash.update(filesz);

            in.seekg(static_cast<std::streamoff>(phdr.p_offset));
            std::vector<char> buffer(8UL * 1024UL * 1024UL);
            uint64_t remaining = filesz;

            while (remaining > 0)
            {
                const size_t to_read = std::min(remaining, static_cast<uint64_t>(buffer.size()));
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


/// Writes `value` into the existing non-allocated section `name` of the same size, without changing anything else in the file.
template <typename Ehdr, typename Shdr>
bool writeSectionInPlace(std::fstream & file, const std::string & name, const std::string & value)
{
    Ehdr ehdr;
    file.seekg(0);
    if (!file.read(reinterpret_cast<char *>(&ehdr), sizeof(ehdr)) || ehdr.e_shoff == 0 || ehdr.e_shentsize != sizeof(Shdr))
    {
        std::cerr << "Failed to read section headers\n";
        return false;
    }

    auto read_section_header = [&](uint64_t index, Shdr & shdr)
    {
        file.seekg(static_cast<std::streamoff>(ehdr.e_shoff + index * sizeof(Shdr)));
        return static_cast<bool>(file.read(reinterpret_cast<char *>(&shdr), sizeof(shdr)));
    };

    /// With 0xff00 sections or more, the counts are stored in the first section header.
    Shdr first;
    if (!read_section_header(0, first))
    {
        std::cerr << "Failed to read section headers\n";
        return false;
    }
    const uint64_t section_count = ehdr.e_shnum ? ehdr.e_shnum : first.sh_size;
    const uint64_t names_index = ehdr.e_shstrndx == SHN_XINDEX ? first.sh_link : ehdr.e_shstrndx;

    Shdr names_header;
    if (names_index >= section_count || !read_section_header(names_index, names_header))
    {
        std::cerr << "Failed to read section names\n";
        return false;
    }
    std::string names(names_header.sh_size, '\0');
    file.seekg(static_cast<std::streamoff>(names_header.sh_offset));
    if (!file.read(names.data(), static_cast<std::streamsize>(names.size())))
    {
        std::cerr << "Failed to read section names\n";
        return false;
    }

    for (uint64_t i = 0; i < section_count; ++i)
    {
        Shdr shdr;
        if (!read_section_header(i, shdr))
        {
            std::cerr << "Failed to read section header\n";
            return false;
        }
        if (shdr.sh_name >= names.size() || std::strcmp(names.c_str() + shdr.sh_name, name.c_str()) != 0)
            continue;

        /// The hash covers only the executable segments: a non-allocated section is not part of any segment.
        if (shdr.sh_type != SHT_PROGBITS || (shdr.sh_flags & SHF_ALLOC) || shdr.sh_size != value.size())
        {
            std::cerr << "Section " << name << " must be non-allocated PROGBITS of " << value.size() << " bytes\n";
            return false;
        }

        file.seekp(static_cast<std::streamoff>(shdr.sh_offset));
        if (!file.write(value.data(), static_cast<std::streamsize>(value.size())) || !file.flush())
        {
            std::cerr << "Failed to write section " << name << "\n";
            return false;
        }
        return true;
    }

    std::cerr << "Section " << name << " not found\n";
    return false;
}


bool hashElfFile(std::istream & in, SipHash & hash)
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

}


int main(int argc, char ** argv)
{
    try
    {
        const bool write_section = argc == 3 && std::string(argv[1]) == "--write-section";
        if (argc != 2 && !write_section)
        {
            std::cerr << "Usage: " << argv[0] << " [--write-section] <elf-file>\n"
                      << "\nComputes the same hash as 'clickhouse hash-binary' but from a file.\n"
                      << "Useful for cross-compilation when you can't execute the target binary.\n"
                      << "--write-section also writes the hash into the section '.clickhouse.hash' of the file, in place.\n";
            return 1;
        }
        const char * path = argv[argc - 1];

        /// With stdout or stderr closed, the file could get that descriptor, and the output would be written into the file.
        if (fcntl(STDOUT_FILENO, F_GETFD) == -1 || fcntl(STDERR_FILENO, F_GETFD) == -1)
        {
            std::cerr << "stdout and stderr must be open\n";
            return 1;
        }

        std::fstream file(path, write_section ? (std::ios::in | std::ios::out | std::ios::binary) : (std::ios::in | std::ios::binary));
        if (!file)
        {
            std::cerr << "Failed to open file: " << path << "\n";
            return 1;
        }

        SipHash hash;
        if (!hashElfFile(file, hash))
            return 1;

        uint64_t lo = 0;
        uint64_t hi = 0;
        hash.get128(lo, hi);

        /// Output format matches ClickHouse's getHexUIntUppercase for UInt128
        std::ostringstream hex; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        hex << std::uppercase << std::hex << std::setfill('0')
            << std::setw(16) << hi << std::setw(16) << lo;
        if (!hex.good())
        {
            std::cerr << "Failed to format the hash\n";
            return 1;
        }

        if (write_section)
        {
            file.clear();
            unsigned char e_ident[EI_NIDENT];
            file.seekg(0);
            if (!file.read(reinterpret_cast<char *>(e_ident), EI_NIDENT))
                return 1;
            const bool written = e_ident[EI_CLASS] == ELFCLASS64
                ? writeSectionInPlace<Elf64_Ehdr, Elf64_Shdr>(file, ".clickhouse.hash", hex.str())
                : writeSectionInPlace<Elf32_Ehdr, Elf32_Shdr>(file, ".clickhouse.hash", hex.str());
            if (!written)
                return 1;
        }

        /// Flush explicitly: an error in the implicit flush at exit would be ignored.
        std::cout << hex.str() << std::flush;
        if (!std::cout.good())
        {
            std::cerr << "Failed to write the hash to stdout\n";
            return 1;
        }
        return 0;
    }
    catch (const std::exception & e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
