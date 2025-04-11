#include "llvm/Object/Binary.h"
#include "llvm/Object/ObjectFile.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/XRay/InstrumentationMap.h"
#include "llvm/DebugInfo/Symbolize/Symbolize.h"
#include "llvm/Support/Path.h"
#include "llvm/Support/TargetSelect.h"

using namespace llvm;
using namespace llvm::object;
using namespace llvm::xray;
using namespace llvm::symbolize;

//Note: right now this is a CLI programm for testing purposes, 
//later will be changed to save the results in some struct or maybe system table

//Takes path to the elf-binary file(that should contain xray_instr_map section), 
// and gets mapping of functionIDs to the addresses, then resolves IDs into human-readable names
int main(int argc, char **argv) {
    // Check if the binary file path is provided as an argument
    if (argc < 2) {
        errs() << "Usage: " << argv[0] << " <binary>\n";
        return 1;
    }

    // Get the binary file path from command-line arguments
    StringRef BinaryPath = argv[1];

    // Load the XRay instrumentation map from the binary
    auto InstrMapOrErr = loadInstrumentationMap(BinaryPath);
    if (!InstrMapOrErr) {
        errs() << "Failed to load instrumentation map: "
               << toString(InstrMapOrErr.takeError()) << "\n";
        return 1;
    }
    auto &InstrMap = *InstrMapOrErr;

    // Retrieve the mapping of function IDs to addresses
    auto FunctionAddresses = InstrMap.getFunctionAddresses();

    // Initialize the LLVM symbolizer to resolve function names
    LLVMSymbolizer Symbolizer;

    // Iterate over all instrumented functions
    for (const auto &[FuncID, Addr] : FunctionAddresses) {
        // Create a SectionedAddress structure to hold the function address
        object::SectionedAddress ModuleAddress;
        ModuleAddress.Address = Addr;
        ModuleAddress.SectionIndex = object::SectionedAddress::UndefSection;

        // Default function name if symbolization fails
        std::string FunctionName = "<unknown>";
        std::string BinaryPathStr = BinaryPath.str();

        // Attempt to symbolize the function address (resolve its name)
        if (auto ResOrErr = Symbolizer.symbolizeCode(BinaryPathStr, ModuleAddress)) {
            auto &DI = *ResOrErr;
            if (DI.FunctionName != DILineInfo::BadString)
                FunctionName = DI.FunctionName;
        }

        // Print function ID, its address, and resolved name
        outs() << "Function ID: " << FuncID
               << ", Address: 0x" << Twine::utohexstr(Addr)
               << ", Name: " << FunctionName << "\n";
    }

    return 0;
}
