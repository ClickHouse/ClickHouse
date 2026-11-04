CLANG_VERSION ?= 21
OUT_FOLDER := .
OPT_LEVEL := -Oz -O3

SRC_FILES := $(wildcard *.c)
WASM_FILES := $(patsubst %.c, $(OUT_FOLDER)/%.wasm, $(SRC_FILES))

.PHONY: all
all: wasm

.PHONY: wasm
wasm: $(OUT_FOLDER) $(WASM_FILES)

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
