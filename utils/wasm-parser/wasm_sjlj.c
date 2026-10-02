/// The `setjmp`/`longjmp` error boundary of the standalone WebAssembly parser.
///
/// This is the one translation unit of the module that is built on its own, outside the LTO unit,
/// and it is C rather than C++. Both are deliberate:
///
///   * LLVM lowers WebAssembly `setjmp`/`longjmp` onto the exception-handling proposal in a
///     codegen pass (`-mllvm -wasm-enable-sjlj`). Run on LTO-merged bitcode, that pass links
///     without complaint and produces a module whose `longjmp` escapes the boundary as an
///     uncaught WebAssembly exception, so the first query that reports its error by throwing
///     takes the module down. Run on a single translation unit, it is correct. Nothing else here
///     calls `setjmp`, and if anything ever does, the link fails on an undefined `setjmp` rather
///     than producing a module that is quietly broken.
///
///   * `-fvirtual-function-elimination` assumes the LTO unit is the whole program and replaces
///     vtable slots nothing calls with a trap. C code has no vtables and makes no virtual calls,
///     so keeping this file outside LTO cannot invalidate that assumption.
///
/// The frames in between - all of `src/Parsers`, LTO'd and optimized as before - need no part in
/// this: a WebAssembly exception unwinds through them on its own.

#include <wasm_sjlj.h>

#include <setjmp.h>
#include <string.h>

static jmp_buf recovery_point;
static int recovery_armed = 0;

/// Not a `std::string`: filling this in must not allocate, because an allocation failing here
/// would throw, and re-enter the path that is writing it.
static char recovery_message[1024];

int chParserProtectedCall(int (*body)(void * argument), void * argument)
{
    int result;

    if (setjmp(recovery_point) != 0)
        return CH_PARSER_THREW;

    recovery_armed = 1;
    result = body(argument);
    recovery_armed = 0;

    return result;
}

int chParserRecoveryArmed(void)
{
    return recovery_armed;
}

const char * chParserRecoveryMessage(void)
{
    return recovery_message;
}

void chParserLongjmp(const char * message)
{
    size_t length = strlen(message);
    if (length > sizeof(recovery_message) - 1)
        length = sizeof(recovery_message) - 1;
    memcpy(recovery_message, message, length);
    recovery_message[length] = 0;

    /// Disarm before jumping: this is the only path that can be re-entered.
    recovery_armed = 0;
    longjmp(recovery_point, 1);
}
