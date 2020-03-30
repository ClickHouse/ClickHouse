## Build and test ClickHouse on various plaforms

Quick and dirty scripts.

Usage example:
```
./run-with-docker.sh ubuntu:bionic jobs/quick-build/run.sh
```

Another example, check build on ARM 64:
```
./prepare-docker-image-ubuntu.sh
./run-with-docker.sh multiarch/ubuntu-core:arm64-bionic jobs/quick-build/run.sh
```

Another example, check build on FreeBSD:
```
./prepare-vagrant-image-freebsd.sh
./run-with-vagrant.sh freebsd jobs/quick-build/run.sh
```

Look at `default_config` and `jobs/quick-build/run.sh`

Various possible options. We are not going to automate testing all of them.

#### CPU architectures:
- x86_64;
- AArch64;
- PowerPC64LE.

x86_64 is the main CPU architecture. We also have minimal support for AArch64 and PowerPC64LE.

#### Operating systems:
- Linux;
- FreeBSD.

We also target Mac OS X, but it's more difficult to test.
Linux is the main. FreeBSD is also supported as production OS.
Mac OS is intended only for development and have minimal support: client should work, server should just start.

#### Linux distributions:
For build:
- Ubuntu Bionic;
- Ubuntu Trusty.

For run:
- Ubuntu Hardy;
- CentOS 5

We should support almost any Linux to run ClickHouse. That's why we test also on old distributions.

#### How to obtain sources:
- use sources from local working copy;
- clone sources from github;
- download source tarball.

#### Compilers:
- gcc-7;
- gcc-8;
- clang-6;
- clang-svn.

#### Compiler installation:
- from OS packages;
- build from sources.

#### C++ standard library implementation:
- libc++;
- libstdc++ with C++11 ABI;
- libstdc++ with old ABI.

When building with clang, libc++ is used. When building with gcc, we choose libstdc++ with C++11 ABI.

#### Linkers:
- ldd;
- gold;

When building with clang on x86_64, ldd is used. Otherwise we use gold.

#### Build types:
- RelWithDebInfo;
- Debug;
- ASan;
- TSan.

#### Build types, extra:
- -g0 for quick build;
- enable test coverage;
- debug tcmalloc.

#### What to build:
- only `clickhouse` target;
- all targets;
- debian packages;

We also have intent to build RPM and simple tgz packages.

#### Where to get third-party libraries:
- from contrib directory (submodules);
- from OS packages.

The only production option is to use libraries from contrib directory.
Using libraries from OS packages is discouraged, but we also support this option.

#### Linkage types:
- static;
- shared;

Static linking is the only option for production usage.
We also have support for shared linking, but it is intended only for developers.

#### Make tools:
- make;
- ninja.

#### Installation options:
- run built `clickhouse` binary directly;
- install from packages.

#### How to obtain packages:
- build them;
- download from repository.

#### Sanity checks:
- check that clickhouse binary has no dependencies on unexpected shared libraries;
- check that source code have no style violations.

#### Tests:
- Functional tests;
- Integration tests;
- Unit tests;
- Simple sh/reference tests;
- Performance tests (note that they require predictable computing power);
- Tests for external dictionaries (should be moved to integration tests);
- Jepsen like tests for quorum inserts (not yet available in opensource).

#### Tests extra:
- Run functional tests with Valgrind.

#### Static analyzers:
- CppCheck;
- clang-tidy;
- Coverity.
