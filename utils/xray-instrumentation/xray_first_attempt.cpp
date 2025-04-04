#include <xray/xray_interface.h>
#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>

std::mutex log_mutex;

[[clang::xray_always_instrument]] void test_function() {
    std::lock_guard<std::mutex> lock(log_mutex);
    std::cout << "Inside test_function()\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

int main() {
    test_function();
    return 0;
}
