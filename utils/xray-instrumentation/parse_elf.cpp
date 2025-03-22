#include <iostream>
#include <fstream>
#include <elf.h>
#include <fcntl.h>
#include <unistd.h>
#include <libelf.h>
#include <gelf.h>
#include <cstring>
#include <vector>
#include <optional>
#include <unordered_map>


// TODO: missing some fields
struct XRayInstrEntry {
    uint64_t sled;     // XRay sled address (место вставки инструментации)
    uint64_t entry;    // Function address (адрес точки входа в функцию)
};

using FunctionAddressMap = std::unordered_map<int32_t, uint64_t>;
using FunctionAddressReverseMap = std::unordered_map<uint64_t, int32_t>;

FunctionAddressMap FunctionAddresses;
FunctionAddressReverseMap FunctionIds;


std::vector<unsigned char> readXRayInstrMap(const char *binary_path) {
    std::vector<unsigned char> data;

    // Opening ELF-binary/Открытие ELF-файла
    int fd = open(binary_path, O_RDONLY);
    if (fd == -1) {
        std::cerr << "Ошибка при открытии файла\n";
        return data;
    }

    // Init libelf/Инициализация libelf
    if (elf_version(EV_CURRENT) == EV_NONE) {
        std::cerr << "Ошибка при инициализации libelf\n";
        close(fd);
        return data;
    }

    Elf *elf = elf_begin(fd, ELF_C_READ, nullptr);
    if (!elf) {
        std::cerr << "Ошибка при открытии ELF-файла\n";
        close(fd);
        return data;
    }

    GElf_Ehdr header;
    if (gelf_getehdr(elf, &header) == nullptr) {
        std::cerr << "Ошибка при чтении заголовка ELF\n";
        elf_end(elf);
        close(fd);
        return data;
    }

    // Search for xray_instr_map section / Поиск секции xray_instr_map
    Elf_Scn *section = nullptr;
    Elf_Data *elf_data = nullptr;
    for (size_t i = 0; i < header.e_shnum; ++i) {
        section = elf_getscn(elf, i);
        GElf_Shdr shdr;
        if (gelf_getshdr(section, &shdr) == nullptr) {
            continue;
        }

        char *section_name = elf_strptr(elf, header.e_shstrndx, shdr.sh_name);
        if (section_name && strcmp(section_name, "xray_instr_map") == 0) {
            elf_data = elf_getdata(section, nullptr);
            if (elf_data) {
                data.assign((unsigned char *)elf_data->d_buf,
                            (unsigned char *)elf_data->d_buf + elf_data->d_size);
            }
            break;
        }
    }

    if (data.empty()) {
        std::cerr << "Xray_instr_map section not found\n";
    }

    // Closing ELF
    elf_end(elf);
    close(fd);
    return data;
}

std::vector<XRayInstrEntry> parseXRayInstrMap(const unsigned char *buf, size_t size) {
    std::vector<XRayInstrEntry> entries;

    // Проходим по секции 16-байтовыми шагами (т.к. каждая запись состоит из 2 quad-слов)
    for (size_t i = 0; i < size; i += 16) {
        if (i + 16 > size) break; // Защита от выхода за границы буфера

        XRayInstrEntry entry;
        std::memcpy(&entry.sled, buf + i, sizeof(uint64_t));
        std::memcpy(&entry.entry, buf + i + sizeof(uint64_t), sizeof(uint64_t));

        entries.push_back(entry);
    }

    // Выводим полученные entry points и sled'ы
    std::cout << "=== XRay Instrumentation Map ===\n";
    for (const auto &entry : entries) {
        std::cout << "SLED: 0x" << std::hex << entry.sled 
                  << "  ENTRY: 0x" << entry.entry << std::dec << "\n";
    }
    return entries;
}


std::optional<int32_t> getFunctionId(uint64_t Addr) {
    auto I = FunctionIds.find(Addr);
    if (I != FunctionIds.end())
      return I->second;
    return std::nullopt;
}

std::optional<uint64_t> getFunctionAddr(int32_t FuncId) {
  auto I = FunctionAddresses.find(FuncId);
  if (I != FunctionAddresses.end())
    return I->second;
  return std::nullopt;
}


void readSymbolTable(const char *binary_path, std::unordered_map<uint64_t, std::string> &symbolMap) {
    int fd = open(binary_path, O_RDONLY);
    if (fd == -1) {
        std::cerr << "Ошибка при открытии файла\n";
        return;
    }

    Elf *elf = elf_begin(fd, ELF_C_READ, nullptr);
    if (!elf) {
        std::cerr << "Ошибка при открытии ELF\n";
        close(fd);
        return;
    }

    GElf_Ehdr header;
    if (!gelf_getehdr(elf, &header)) {
        std::cerr << "Ошибка при чтении заголовка ELF\n";
        elf_end(elf);
        close(fd);
        return;
    }

    Elf_Scn *section = nullptr;
    while ((section = elf_nextscn(elf, section)) != nullptr) {
        GElf_Shdr shdr;
        if (!gelf_getshdr(section, &shdr)) continue;

        char *section_name = elf_strptr(elf, header.e_shstrndx, shdr.sh_name);
        if (!section_name) continue;

        // Search for .symtab (or .dynsym, если бинарник стрипнут)
        if (strcmp(section_name, ".symtab") == 0 || strcmp(section_name, ".dynsym") == 0) {
            Elf_Data *data = elf_getdata(section, nullptr);
            if (!data) continue;

            size_t numSymbols = shdr.sh_size / shdr.sh_entsize;
            for (size_t i = 0; i < numSymbols; i++) {
                GElf_Sym sym;
                if (!gelf_getsym(data, i, &sym)) continue;

                if (sym.st_value == 0 || GELF_ST_TYPE(sym.st_info) != STT_FUNC) continue;

                char *symName = elf_strptr(elf, shdr.sh_link, sym.st_name);
                if (symName) {
                    symbolMap[sym.st_value] = symName;
                }
            }
        }
    }

    elf_end(elf);
    close(fd);
}


int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cerr << "Использование: " << argv[0] << " <elf-файл>\n";
        return 1;
    }

    std::unordered_map<uint64_t, std::string> symbolMap;
    readSymbolTable(argv[1], symbolMap);



    // Read section from elf / Читаем секцию из ELF-файла
    std::vector<unsigned char> xray_data = readXRayInstrMap(argv[1]);
    auto entries = parseXRayInstrMap(xray_data.data(), xray_data.size());

    std::cout << "=== XRay Function Mapping ===\n";
    for (const auto &entry : entries) {
        auto it = symbolMap.find(entry.entry);
        if (it != symbolMap.end()) {
            std::cout << "SLED: 0x" << std::hex << entry.sled 
                    << "  ENTRY: 0x" << entry.entry 
                    << "  FUNCTION: " << it->second << "\n";
        } else {
            std::cout << "SLED: 0x" << std::hex << entry.sled 
                    << "  ENTRY: 0x" << entry.entry 
                    << "  FUNCTION: <unknown>\n";
        }
    }

    return 0;
}
