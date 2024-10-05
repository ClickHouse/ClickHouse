## chcache

#### Usage instructions

First, build the binary. If you don't have Rust toolchain, get one by installing `rustup` with your package manager, then enable the default toolchain:
```bash
rustup default stable
```

Then, build the `chcache`:
```bash
cargo build --release
```

Place this in your `~/.config/chcache/config.toml`
```toml
TBD
```

Go to your `~/src/clickhouse/cmake-build-debug` (only `cmake-build-debug` part is important) and run CMake like this, adjusting the path to `chcache` binary:
```bash
cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_BUILD_PATH_MAPPING=1 -DCMAKE_CXX_COMPILER_LAUNCHER=/home/thevar1able/src/chcache/chcache-rust/target/release/chcache-rust -DCMAKE_C_COMPILER_LAUNCHER=/home/thevar1able/src/chcache/chcache-rust/target/release/chcache-rust -DCOMPILER_CACHE=disabled ..
```
