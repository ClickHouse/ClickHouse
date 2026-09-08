CLANG_VERSION ?= 21
OUT_FOLDER := .
OPT_LEVEL := -Oz -O3

SRC_FILES := $(wildcard *.c)
WASM_FILES := $(patsubst %.c, $(OUT_FOLDER)/%.wasm, $(SRC_FILES))

# AssemblyScript compiler (https://www.assemblyscript.org).
# Install with: `npm install -g assemblyscript` (provides the `asc` binary).
ASSEMBLYSCRIPT_COMPILER ?= asc
ASSEMBLYSCRIPT_SRC_FILES := $(wildcard as_*.ts)
ASSEMBLYSCRIPT_WASM_FILES := $(patsubst %.ts, $(OUT_FOLDER)/%.wasm, $(ASSEMBLYSCRIPT_SRC_FILES))

.PHONY: all
all: wasm

.PHONY: wasm
wasm: $(OUT_FOLDER) $(WASM_FILES) $(ASSEMBLYSCRIPT_WASM_FILES)

$(OUT_FOLDER):
	mkdir -p $(OUT_FOLDER)

$(OUT_FOLDER)/%.o: %.c
	clang-$(CLANG_VERSION) -std=c23 --target=wasm32 -msimd128 -ffreestanding -nostdlib $(OPT_LEVEL) -c $< -o $@

$(OUT_FOLDER)/%.wasm: $(OUT_FOLDER)/%.o
	wasm-ld-$(CLANG_VERSION) --export-all --no-entry --lto-O3 --allow-undefined $< -o $@
	@size=$$(wc -c < $@); \
	if [ $$size -gt 10240 ]; then \
		echo "Error: $@ is $$size bytes, which exceeds the 10KB limit"; \
		rm $@; \
		exit 1; \
	fi

# Linked with an explicit maximum for its linear memory: the module-declared half of
# `WasmCompartment::getMaxLinearMemorySize` has no other coverage.
$(OUT_FOLDER)/small_memory_abi.wasm: $(OUT_FOLDER)/small_memory_abi.o
	wasm-ld-$(CLANG_VERSION) --export-all --no-entry --lto-O3 --allow-undefined --max-memory=196608 $< -o $@

# Linked with zero initial pages and an explicit maximum: the splitter's
# `getLinearMemorySize() == 0 ? getMaxLinearMemorySize() : ...` path has no other coverage. The
# guest grows its own memory, so it needs neither a shadow stack nor static data.
$(OUT_FOLDER)/growable_zero_page_abi.wasm: $(OUT_FOLDER)/growable_zero_page_abi.o
	wasm-ld-$(CLANG_VERSION) --export-all --no-entry --lto-O3 --allow-undefined -z stack-size=0 --initial-memory=0 --max-memory=196608 $< -o $@

$(OUT_FOLDER)/as_%.wasm: as_%.ts
	$(ASSEMBLYSCRIPT_COMPILER) $< --runtime incremental --exportRuntime --enable simd --disableWarning 112 -o $@
# WARNING AS112: Exchange of 'v128' values is not supported by all embeddings
# We support 128-bit integers, so allow export function with v128 in signature
