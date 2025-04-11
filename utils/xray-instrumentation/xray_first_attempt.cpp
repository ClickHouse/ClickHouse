#include <cstdio>
#include <chrono>
#include <thread>
#include <mutex>
#include <xray/xray_interface.h>


//This is a simple custom handler for XRay, that just sleeps and prints out the id of the function
//Later will implement more custom handlers that do smth more meaningful(TBD)

// Global mutex to ensure thread-safe logging -- this is probably not the best way to do this though
std::mutex log_mutex;

// Hook function that is executed before entering an instrumented function
[[clang::xray_never_instrument]] void logEntry(int32_t FuncId, XRayEntryType Type) {
    static thread_local bool in_hook = false;

    // Prevent re-entry into the hook and only process function entry events
    if (in_hook || Type != XRayEntryType::ENTRY)
        return;
    
    in_hook = true; 
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        printf("Function ID %d entered. Sleeping for 1s...\n", FuncId);
    }

    // Simulate work by sleeping for 1 second
    std::this_thread::sleep_for(std::chrono::seconds(1));
    in_hook = false;
}

// Mark functions as always instrumented by XRay
[[clang::xray_always_instrument]] void foo() {
    printf("Executing foo()\n");
}

[[clang::xray_always_instrument]] void bar() {
    printf("Executing bar()\n");
}

int main() {
    // Enable XRay and set up function patching
    __xray_set_handler(logEntry);
    __xray_patch();

    printf("Calling foo()\n");
    foo();

    printf("Calling bar()\n");
    bar();

    // Disable patching after execution
    __xray_unpatch();

    return 0;
}
