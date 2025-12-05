/// Tool to compute the same hash as ClickHouse's getHashOfLoadedBinary() from a file.
/// Useful for cross-compilation when you can't execute the target binary.

#include <cstring>
#include <fstream>
#include <iostream>

#include <elf.h>

#include <Common/SipHash.h>


namespace
{

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

            const UInt64 filesz = phdr.p_filesz;
            hash.update(filesz);

            in.seekg(static_cast<std::streamoff>(phdr.p_offset));
            std::vector<char> buffer(8UL * 1024UL * 1024UL);
            UInt64 remaining = filesz;

            while (remaining > 0)
            {
                const size_t to_read = std::min(remaining, static_cast<UInt64>(buffer.size()));
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

        const auto result = hash.get128();
        std::cout << getHexUIntUppercase(result);
        return 0;
    }
    catch (const std::exception & e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
