#pragma once

/// The error boundary of the standalone WebAssembly parser. See `wasm_sjlj.c` for why it is a
/// separate translation unit, and `wasm_runtime.cpp` for what arrives here.

#ifdef __cplusplus
extern "C"
{
#endif

/// What `chParserProtectedCall` returns when `body` threw instead of returning. `body` itself
/// only ever answers 1 or 0, so this cannot collide with a real answer.
#define CH_PARSER_THREW (-1)

/// Runs `body(argument)` with the boundary armed, and returns what it returned. If a
/// `DB::Exception` is thrown below, returns `CH_PARSER_THREW` instead, and
/// `chParserRecoveryMessage` holds its message.
int chParserProtectedCall(int (*body)(void * argument), void * argument);

/// Whether a `chParserProtectedCall` is currently on the stack, i.e. whether a throw can be
/// recovered from at all.
int chParserRecoveryArmed(void);

/// The message of the exception that ended the last `chParserProtectedCall`.
const char * chParserRecoveryMessage(void);

/// Records `message` and returns to the innermost armed `chParserProtectedCall`. Only valid
/// while `chParserRecoveryArmed` holds.
__attribute__((noreturn)) void chParserLongjmp(const char * message);

#ifdef __cplusplus
}
#endif
