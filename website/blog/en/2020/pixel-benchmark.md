---
title: 'Running ClickHouse on an Android phone'
image: 'https://blog-images.clickhouse.tech/en/2020/pixel-benchmark/main.jpg'
date: '2020-07-16'
author: '[Alexander Kuzmenkov](https://github.com/akuzm)'
tags: ['Android', 'benchmark', 'experiment']
---


This is a brief description of my experiments with building ClickHouse on Android. If this is your first time hearing about ClickHouse, it is a suriprisingly fast columnar SQL DBMS for real-time reporting. It's normally used in AdTech and the like, deployed on clusters of hundreds of machines, holding up to petabytes of data. But ClickHouse is straightforward to use on a smaller scale as well &mdash; you laptop will do, and don't be surprised if you are able to process several gigabytes of data per second on this hardware. There is another kind of small-scale, though pretty powerful, platforms, that is ubiquitous now &mdash; smartphones. The conclusion inevitably follows: you must be able to run ClickHouse on your smartphone as well. It's also that I can't help but chuckle at the idea of setting up a high performance mobile OLAP cluster using a dozen of phones. Or also at the idea of seeing the nostalgic `Segmentation fault (core dumped)` on the lovely OLED screen, but I digress. Let's get it going.

## First cheap attempt

I heard somewhere that Android uses the Linux kernel, and I can already run familiar UNIX-like shell and tools using [Termux](https://termux.com/). And ClickHouse already supports ARM platform and even publishes a binary built for 64-bit ARM. This binary also doesn't have a lot of dependencies &mdash; only a pretty old version of `glibc`. Maybe I can just download a ClickHouse binary from CI to the phone and run it?

Turns out it's not that simple.

* The first thing we'll see after trying to run is an absurd error message: `./clickhouse: file is not found`. But it's right there! `strace` helps: what cannot be found is `/lib64/ld-linux-x86-64.so.2`, a linker specified in the ClickHouse binary. The linker, in this case, is a system program that initially loads the application binary and its dependencies before passing control to the application. Android uses a different linker located by another path, this is why we get the error. This problem can be overcome if we call the linker explicitly, e.g. `/system/bin/linker64 $(readlink -f ./clickhouse)`.

* Immediately we encounter another problem: the linker complains that the binary has a wrong type `ET_EXEC`. What does this mean? Android binaries must support dynamic relocation, so that they can be loaded at any address, probably for ASLR purposes. ClickHouse binaries do not normally use position-independent code, because we have measured that it gives a small performance penalty of about 1%. After tweaking compilation and linking flags to include `-fPIC` as much as possible, and battling some really weird linker errors, we finally arrive at a relocatable binary that has a correct type `ET_DYN`.

* But it only gets worse. Now it complains about TLS section offset being wrong. After reading some mail archives where I could barely understand a word, I concluded that Android uses some different layout of memory for the section of the executable that holds thread-local variables, and `clang` from Android toolchain is patched to account for this. After that, I had to accept I won't be able to use familiar tools, and reluctantly turned to the Android toolchain.

## Using the Android toolchain

Surprisingly, it's rather simple to set up. Our build system uses CMake and already supports cross-compilation &mdash; we have CI configurations that cross-compile for Mac, AArch64 Linux and FreeBSD. Android NDK also has integration with CMake and a [manual](https://developer.android.com/ndk/guides/cmake) on how to set it up. Download the Android NDK, add some flags to your `cmake` invocation: `DCMAKE_TOOLCHAIN_FILE=~/android-ndk-r21d/build/cmake/android.toolchain.cmake -DANDROID_ABI=arm64-v8a -DANDROID_PLATFORM=28`, and you're done. It (almost) builds. What obstacles do we have this time?

* Our `glibc` compatibility layer has a lot of compilation errors. It borrows `musl` code to provide functions that are absent from older versions of `glibc`, so that we can run the same binary on a wide range of distros. Being heavily dependent on system headers, it runs into all kinds of differences between Linux and Android, such as the limited scope of `pthread` support or just subtly different API variants. Thankfully we're building for a particular version of Android, so we can just disable this and use all needed functions straight from the system `libc`.
* Some third-party libraries and our CMake files are broken in various unimaginative ways. Just disable everything we can and fix everything we can't. 
* Some of our code uses `#if defined(__linux__)` to check for Linux platform. This doesn't always work, because Android also exports `__linux__` but there are some API differences.
* `std::filesystem` is still not fully supported in NDK r21. The support went into r22 that is scheduled for Q3 2020, but I want it right now... Good that we bundle our own forks of `libcxx` and `libcxxabi` to reduce dependencies, and they are fresh enough to fully support C++20. After enabling them, everything works.
* Weird twenty-screens errors in `std::map<int>` or something like that, that are also resolved by using our `libcxx`.

## On the device

At last, we have a binary we can actually run. Copy it to  the phone, `chmod +x`, `./clickhouse server --config-path db/config.xml`, run some queries, it works!

<img src="https://blog-images.clickhouse.tech/en/2020/pixel-benchmark/segfault.png" width="40%"/>

Feels so good to see my favorite message.

It's a full-fledged development environment here in Termux, let's install `gdb` and attach it to see where the segfault happens. Run `gdb clickhouse --ex run '--config-path ....'`, wait for it to lauch for a minute, only to see how Android kills Termux becase it is out of memory. Are 4 GB of RAM not enough, after all? Looking at the `clickhouse` binary, its size is a whoppping 1.1 GB. The major part of the bloat is due to the fact that some of our computational code is heavily specialized for particular data types (mostly via C++ templates), and also the fact that we build and link a lot of third-party libraries statically. A non-essential part of the binary is debug symbols, which help to produce good stack traces in error messages. We can remove them with `strip -s ./clickhouse` right here on the phone, and after that, the size becomes more manageable, about 400 MB. Finally we can run `gdb` and see that the segfault is somewhere in `unw_backtrace`:

```
Thread 60 "ConfigReloader" received signal SIGSEGV, Segmentation fault.                         
[Switching to LWP 21873]                        
0x000000556a73f740 in ?? ()          

(gdb) whe 20                                    
#0  0x000000556a73f740 in ?? ()                 
#1  0x000000556a744028 in ?? ()                 
#2  0x000000556a73e5a0 in ?? ()                 
#3  0x000000556a73d250 in unw_init_local ()     
#4  0x000000556a73deb8 in unw_backtrace ()      
#5  0x0000005562aabb54 in StackTrace::tryCapture() ()                                           
#6  0x0000005562aabb10 in StackTrace::StackTrace() ()                                           
#7  0x0000005562a8d73c in MemoryTracker::alloc(long) ()                                         
#8  0x0000005562a8db38 in MemoryTracker::alloc(long) ()                                         
#9  0x0000005562a8e8bc in CurrentMemoryTracker::alloc(long) ()                                  
#10 0x0000005562a8b88c in operator new[](unsigned long) ()                                      
#11 0x0000005569c35f08 in Poco::XML::NamePool::NamePool(unsigned long) ()                       
...
```

What is this function, and why do we need it? In this particular stack trace, we're out of memory, and about to throw an exception for that. `unw_backtrace` is called to produce a backtrace for the exception message. But there is another interesting context where we call it. Believe it or not, ClickHouse has a built-in `perf`-like sampling profiler that can save stack traces for CPU time and real time, and also memory allocations. The data is saved into a `system.trace_log` table, so you can build flame graphs for what your query was doing as simple as piping output of an SQL query into `flamegraph.pl`. This is an interesting feature, but what is relevant now is that it sends signals to all threads of the server to interrupt them at some random time and save their current backtraces, using the same `unw_backtrace` function that we know to segfault. We expect query profiler to be used in production environment, so it is enabled by default. After disabling it, we have a functioning ClickHouse server running on Android.

## Is your phone good enough?

There is a beaten genre of using data sets and queries of a varying degree of syntheticity to prove that a particular DBMS you work on has performance superior to other, less advanced, DBMSes. We've moved past that, and instead use the DBMS we love as a benchmark of hardware. For this benchmark we use a small 100M rows obfuscated data set from Yandex.Metrica, about 12 GB compressed, and some queries representative of Metrica dashboards. There is [this page](https://clickhouse.tech/benchmark/hardware/) with crowdsourced results for various cloud and traditional servers and even some laptops, but how do the phones compare? Let's find out. Following [the manual](https://clickhouse.tech/docs/en/operations/performance-test/) to download the necessary data to the phone and run the benchmark was pretty straightforward. One problem was that some queries can't run because they use too much memory and the server gets killed by Android, so I had to script around that. Also, I'm not sure how to reset a file system cache on Android, so the 'cold run' data is not correct. The results look pretty good:

<img src="https://blog-images.clickhouse.tech/en/2020/pixel-benchmark/compare.png" width="80%"/>

My phone is Google Pixel 3a, and it is only 5 times slower on average than my Dell XPS 15 work laptop. The queries where the data doesn't fit into memory and has to go to disk (the flash, I mean) are noticeably slower, up to 20 times, but mostly they don't complete because the server gets killed &mdash; it only has about 3 GB of memory available. Overall I think the results look pretty good for the phone. High-end models should be even more performant, reaching performance comparable to some smaller laptops.

## Conclusion

This was a rather enjoyable exercise. Running a server on your phone is a nice way to give a demo, so we should probably publish a Termux package for ClickHouse. For this, we have to debug and fix the `unw_backtrace` segfault (I have my fingers crossed that it will be gone after adding `-fno-omit-frame-pointer`), and also fix some quirks that are just commented out for now. Most of the changes required for the Android build are already merged into our master branch.

Building for Android turned out to be relatively simple &mdash; all these experiments and writing took me about four days, and it was the first time I ever did any Android-related programming. The NDK was simple to use, and our code was cross-platform enough so I only had to make minor modifications. If we didn't routinely build for AArch64 and had a hard dependency on SSE 4.2 or something, it would have been a different story.

But the most important takeout is that now you don't have to obsess over choosing a new phone &mdash; just benchmark it with ClickHouse.


_2020-07-16 [Alexander Kuzmenkov](https://github.com/akuzm)_
