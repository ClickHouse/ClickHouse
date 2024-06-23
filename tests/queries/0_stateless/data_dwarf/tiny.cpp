#include <thread>
#include <iostream>
#include <chrono>
using namespace std;

// Just an arbitrary program. We don't run it, just need its debug symbols.

int main() {
    thread t([]{
        this_thread::sleep_for(chrono::seconds(1));
        throw "hi";
    });
    this_thread::sleep_for(chrono::seconds(10));
    cout<<"unreachable\n";
}
