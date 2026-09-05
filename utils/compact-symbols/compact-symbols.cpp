#include <Common/CompactSymbols.h>

#include <bit>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <elf.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace
{

bool rangeWithin(uint64_t offset, uint64_t size, uint64_t total)
{
    return offset <= total && size <= total - offset;
}

template <typename T>
T byteSwap(T value)
{
    static_assert(std::is_unsigned_v<T>);
    if constexpr (sizeof(T) == 2)
        return __builtin_bswap16(value);
    else if constexpr (sizeof(T) == 4)
        return __builtin_bswap32(value);
    else if constexpr (sizeof(T) == 8)
        return __builtin_bswap64(value);
    else
        static_assert(sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8);
}

template <typename T>
T elfValue(T value, bool little_endian)
{
    static_assert(std::is_unsigned_v<T>);
    if (little_endian == (std::endian::native == std::endian::little))
        return value;
    return byteSwap(value);
}

class MappedFile
{
public:
    explicit MappedFile(const char * file_name)
    {
        fd = open(file_name, O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            throw std::runtime_error(std::string("Cannot open input ELF: ") + strerror(errno));

        struct stat file_stat
        {
        };
        if (fstat(fd, &file_stat) != 0)
        {
            int error = errno;
            [[maybe_unused]] int close_result = close(fd);
            fd = -1;
            throw std::runtime_error(std::string("Cannot stat input ELF: ") + strerror(error));
        }
        if (file_stat.st_size <= 0)
        {
            [[maybe_unused]] int close_result = close(fd);
            fd = -1;
            throw std::runtime_error("Input ELF has an invalid size");
        }
        size = static_cast<size_t>(file_stat.st_size);

        void * mapped = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped == MAP_FAILED)
        {
            int error = errno;
            [[maybe_unused]] int close_result = close(fd);
            fd = -1;
            throw std::runtime_error(std::string("Cannot mmap input ELF: ") + strerror(error));
        }
        data = static_cast<const char *>(mapped);
    }

    ~MappedFile()
    {
        if (data)
            munmap(const_cast<char *>(data), size);
        if (fd >= 0)
            [[maybe_unused]] int close_result = close(fd);
    }

    MappedFile(const MappedFile &) = delete;
    MappedFile & operator=(const MappedFile &) = delete;

    const char * begin() const { return data; }
    size_t fileSize() const { return size; }

private:
    int fd = -1;
    const char * data = nullptr;
    size_t size = 0;
};

std::vector<DB::CompactSymbols::Symbol> readSymbols(const MappedFile & file)
{
    if (file.fileSize() < sizeof(Elf64_Ehdr))
        throw std::runtime_error("Input is too small to be an ELF file");

    const auto & header = *reinterpret_cast<const Elf64_Ehdr *>(file.begin());
    if (memcmp(header.e_ident, ELFMAG, SELFMAG) != 0)
        throw std::runtime_error("Input does not have ELF magic");
    if (header.e_ident[EI_CLASS] != ELFCLASS64)
        throw std::runtime_error("Only ELF64 inputs are supported");
    if (header.e_ident[EI_DATA] != ELFDATA2LSB && header.e_ident[EI_DATA] != ELFDATA2MSB)
        throw std::runtime_error("Input ELF has an unsupported byte order");

    bool little_endian = header.e_ident[EI_DATA] == ELFDATA2LSB;
    uint64_t section_headers_offset = elfValue(header.e_shoff, little_endian);
    uint16_t section_count = elfValue(header.e_shnum, little_endian);
    uint16_t section_header_size = elfValue(header.e_shentsize, little_endian);
    if (section_header_size != sizeof(Elf64_Shdr))
        throw std::runtime_error("Input ELF has an unsupported section header size");
    if (!section_count || !rangeWithin(section_headers_offset, static_cast<uint64_t>(section_count) * section_header_size, file.fileSize()))
        throw std::runtime_error("Input ELF section headers are out of bounds");

    const auto * section_headers = reinterpret_cast<const Elf64_Shdr *>(file.begin() + section_headers_offset);
    const Elf64_Shdr * symbol_table = nullptr;
    for (uint16_t index = 0; index < section_count; ++index)
    {
        if (elfValue(section_headers[index].sh_type, little_endian) == SHT_SYMTAB)
        {
            symbol_table = &section_headers[index];
            break;
        }
    }
    if (!symbol_table)
        throw std::runtime_error("Input ELF has no .symtab section");

    uint32_t string_table_index = elfValue(symbol_table->sh_link, little_endian);
    if (string_table_index >= section_count)
        throw std::runtime_error("Input ELF .symtab has an invalid string table link");
    const auto & string_table = section_headers[string_table_index];
    if (elfValue(string_table.sh_type, little_endian) != SHT_STRTAB)
        throw std::runtime_error("Input ELF .symtab does not link to a string table");

    uint64_t symbol_offset = elfValue(symbol_table->sh_offset, little_endian);
    uint64_t symbol_size = elfValue(symbol_table->sh_size, little_endian);
    uint64_t symbol_entry_size = elfValue(symbol_table->sh_entsize, little_endian);
    uint64_t strings_offset = elfValue(string_table.sh_offset, little_endian);
    uint64_t strings_size = elfValue(string_table.sh_size, little_endian);
    if (symbol_entry_size != sizeof(Elf64_Sym) || symbol_size % symbol_entry_size != 0)
        throw std::runtime_error("Input ELF has an invalid .symtab entry size");
    if (!rangeWithin(symbol_offset, symbol_size, file.fileSize()) || !rangeWithin(strings_offset, strings_size, file.fileSize()))
        throw std::runtime_error("Input ELF symbol or string table is out of bounds");

    const auto * symbols = reinterpret_cast<const Elf64_Sym *>(file.begin() + symbol_offset);
    const char * strings = file.begin() + strings_offset;
    size_t symbol_count = static_cast<size_t>(symbol_size / symbol_entry_size);

    std::vector<DB::CompactSymbols::Symbol> result;
    result.reserve(symbol_count / 2);
    for (size_t index = 0; index < symbol_count; ++index)
    {
        const auto & symbol = symbols[index];
        uint32_t name_offset = elfValue(symbol.st_name, little_endian);
        uint64_t address = elfValue(symbol.st_value, little_endian);
        uint64_t size = elfValue(symbol.st_size, little_endian);

        if (!name_offset || !address || !size || name_offset >= strings_size)
            continue;

        const char * name = strings + name_offset;
        size_t available = static_cast<size_t>(strings_size - name_offset);
        const void * terminator = memchr(name, '\0', available);
        if (!terminator || terminator == name)
            continue;
        size_t name_size = static_cast<const char *>(terminator) - name;
        result.push_back({address, size, std::string_view(name, name_size)});
    }
    return result;
}

void writeFile(const char * file_name, std::span<const char> data)
{
    int fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        throw std::runtime_error(std::string("Cannot open output blob: ") + strerror(errno));

    size_t written = 0;
    while (written < data.size())
    {
        ssize_t result = write(fd, data.data() + written, data.size() - written);
        if (result < 0 && errno == EINTR)
            continue;
        if (result <= 0)
        {
            int error = errno;
            [[maybe_unused]] int close_result = close(fd);
            throw std::runtime_error(std::string("Cannot write output blob: ") + strerror(error));
        }
        written += static_cast<size_t>(result);
    }
    if (close(fd) != 0)
        throw std::runtime_error(std::string("Cannot close output blob: ") + strerror(errno));
}

}

int main(int argc, char ** argv)
{
    if (argc != 3)
    {
        std::cerr << "Usage: compact-symbols <input-elf> <output-blob>\n";
        return 1;
    }

    try
    {
        MappedFile input(argv[1]);
        std::vector<DB::CompactSymbols::Symbol> symbols = readSymbols(input);
        std::vector<char> encoded = DB::CompactSymbols::encode(symbols);
        DB::CompactSymbols::Reader reader(std::string_view(encoded.data(), encoded.size()));
        writeFile(argv[2], encoded);
        std::cout << "Wrote " << encoded.size() << " bytes for " << reader.addressCount() << " symbols and " << reader.nameCount()
                  << " unique names\n";
        return 0;
    }
    catch (const std::exception & exception)
    {
        std::cerr << "compact-symbols: " << exception.what() << '\n';
        return 1;
    }
}
