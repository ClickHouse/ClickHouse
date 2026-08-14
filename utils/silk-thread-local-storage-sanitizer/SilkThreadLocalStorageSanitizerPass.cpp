#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/ADT/Twine.h>
#include <llvm/Support/ErrorHandling.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Module.h>
#include <llvm/Passes/PassBuilder.h>
#if __has_include(<llvm/Plugins/PassPlugin.h>)
#include <llvm/Plugins/PassPlugin.h>
#else
#include <llvm/Passes/PassPlugin.h>
#endif

namespace
{

constexpr llvm::StringLiteral hook_name = "silk_thread_local_storage_sanitizer_access_hook";
constexpr llvm::StringLiteral storage_symbol = "FiberLocalStorageThreadStorage";

constexpr llvm::StringLiteral excluded_modules[] =
{
    "contrib/silk",
    "contrib/jemalloc",
    "contrib/llvm-project/libcxxabi",
    "src/Common/SilkThreadLocalStorageSanitizer.cpp",
};

bool isExcludedModule(const llvm::Module & module)
{
    llvm::StringRef path = module.getSourceFileName();
    for (llvm::StringRef excluded : excluded_modules)
        if (path.contains(excluded))
            return true;
    return false;
}


/// A weak no-op ({ return; }) definition of the hook.
/// If we added a declaration of the hook instead, non-clickhouse binaries
/// (that do not link against SilkThreadLocalStorageSanitizer)
/// such as protoc or grpc_cpp_plugin would fail at the linking stage.
llvm::Function * createWeakDefinitionOfHook(llvm::Module & module, llvm::PointerType * ptr)
{
    if (module.getNamedValue(hook_name))
        llvm::report_fatal_error(llvm::Twine(hook_name) + " already exists in " + module.getSourceFileName());

    llvm::LLVMContext & context = module.getContext();
    auto * hook = llvm::Function::Create(
        llvm::FunctionType::get(llvm::Type::getVoidTy(context), {ptr, ptr}, false),
        llvm::GlobalValue::WeakAnyLinkage, hook_name, module);
    llvm::IRBuilder<>(llvm::BasicBlock::Create(context, "", hook)).CreateRetVoid();
    return hook;
}

bool injectAccessChecks(llvm::Module & module)
{
    llvm::PointerType * ptr = llvm::PointerType::getUnqual(module.getContext());
    llvm::Function * intrinsic = llvm::Intrinsic::getDeclarationIfExists(&module, llvm::Intrinsic::threadlocal_address, {ptr});
    if (!intrinsic)
        return false;

    llvm::Function * hook = nullptr;
    llvm::DenseMap<const llvm::GlobalValue *, llvm::Constant *> variable_names;
    for (llvm::User * user : intrinsic->users())
    {
        auto * access = llvm::cast<llvm::CallInst>(user);
        const auto * variable = llvm::cast<llvm::GlobalValue>(access->getArgOperand(0));

        /// Skip FiberLocalStorageThreadStorage.
        /// The same fiber benignly touches multiple FiberLocalStorageThreadStorage instances
        /// as on each context switch we do FiberLocalStorage::swap.
        if (variable->getName() == storage_symbol)
            continue;

        if (!hook)
            hook = createWeakDefinitionOfHook(module, ptr);

        /// Create hook call.
        llvm::IRBuilder<> builder(access->getParent(), std::next(access->getIterator()));
        llvm::Constant *& name_string = variable_names[variable];
        if (!name_string)
            name_string = builder.CreateGlobalString(variable->getName());
        builder.CreateCall(hook, {access, name_string});
    }
    return hook != nullptr;
}

/// Create a silk_thread_local_storage_sanitizer_access_hook call after each TLS address computation
/// (as well as a weak no-op definition of the hook).
struct SilkThreadLocalStorageSanitizer : llvm::PassInfoMixin<SilkThreadLocalStorageSanitizer>
{
    llvm::PreservedAnalyses run(llvm::Module & module, llvm::ModuleAnalysisManager &)
    {
        if (isExcludedModule(module))
            return llvm::PreservedAnalyses::all();

        bool injected = injectAccessChecks(module);
        return injected ? llvm::PreservedAnalyses::none() : llvm::PreservedAnalyses::all();
    }
};

}

extern "C" LLVM_ATTRIBUTE_WEAK llvm::PassPluginLibraryInfo llvmGetPassPluginInfo()
{
    return {LLVM_PLUGIN_API_VERSION, "SilkThreadLocalStorageSanitizer", LLVM_VERSION_STRING,
        [](llvm::PassBuilder & pass_builder)
        {
            pass_builder.registerPipelineStartEPCallback(
                [](llvm::ModulePassManager & manager, llvm::OptimizationLevel) { manager.addPass(SilkThreadLocalStorageSanitizer()); });
        }};
}
