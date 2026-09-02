# Reports the size of the compiled parser, and - unless REPORT_ONLY is set - fails when it is over
# MAX_SIZE. Run by the `parser-size` test and, in reporting mode, after every link.
#
#   cmake -DWASM=<path> -DMAX_SIZE=<bytes> [-DREPORT_ONLY=ON] -P check_size.cmake

if (NOT DEFINED WASM OR NOT DEFINED MAX_SIZE)
    message (FATAL_ERROR "WASM and MAX_SIZE are both required")
endif ()

if (NOT EXISTS "${WASM}")
    message (FATAL_ERROR "${WASM} does not exist")
endif ()

file (SIZE "${WASM}" SIZE)

# What a browser actually negotiates. gzip is the floor every engine supports, so it is the one
# reported; the README has the brotli and zstd numbers alongside it. It is reported only, never
# asserted on: the ceiling is on the module itself, which is what the build controls.
find_program (GZIP gzip)
if (GZIP)
    execute_process (
        COMMAND "${GZIP}" -9 -c "${WASM}"
        OUTPUT_FILE "${WASM}.gz"
        RESULT_VARIABLE GZIP_RESULT)
    if (NOT GZIP_RESULT EQUAL 0)
        message (FATAL_ERROR "Could not compress ${WASM}: ${GZIP_RESULT}")
    endif ()
    file (SIZE "${WASM}.gz" COMPRESSED_SIZE)
    file (REMOVE "${WASM}.gz")
    set (GZIPPED " (${COMPRESSED_SIZE} gzipped)")
endif ()

if (SIZE GREATER MAX_SIZE)
    math (EXPR OVER "${SIZE} - ${MAX_SIZE}")
    set (COMPLAINT "${WASM} is ${SIZE} bytes${GZIPPED}, ${OVER} over the ${MAX_SIZE} byte ceiling")
    if (REPORT_ONLY)
        message (WARNING "${COMPLAINT}")
    else ()
        message (FATAL_ERROR "${COMPLAINT}.\n"
            "This is what a browser downloads, so growth here is not free. If it is intended, "
            "raise the corresponding MAX_SIZE_* in utils/wasm-parser/CMakeLists.txt in the same "
            "change, and update the table in utils/wasm-parser/README.md.")
    endif ()
else ()
    math (EXPR HEADROOM "${MAX_SIZE} - ${SIZE}")
    message (STATUS "${WASM}: ${SIZE} bytes${GZIPPED}, ${HEADROOM} under the ceiling")
endif ()
