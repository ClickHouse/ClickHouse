# SYSTEM Queries Tests for clickhouse-local

This directory contains tests for the fix that prevents clickhouse-local from throwing LOGICAL_ERROR on certain SYSTEM queries and instead throws UNSUPPORTED_METHOD with proper error messages.

## Problem Description

Previously, clickhouse-local would throw LOGICAL_ERROR when users tried to execute certain SYSTEM queries that are not supported in the local mode:

- `SYSTEM RELOAD CONFIG` - would throw "Can't reload config because config_reload_callback is not set. (LOGICAL_ERROR)"
- `SYSTEM STOP LISTEN HTTP` - would throw LOGICAL_ERROR
- `SYSTEM START LISTEN HTTP` - would throw LOGICAL_ERROR

## Solution

The fix adds proper checks for `Context::ApplicationType::LOCAL` in the `InterpreterSystemQuery` class and throws `UNSUPPORTED_METHOD` errors with descriptive messages instead of `LOGICAL_ERROR`.

## Test Files

### 03562_clickhouse_local_system_queries.sql
Basic test that verifies the three problematic SYSTEM queries now throw UNSUPPORTED_METHOD errors in clickhouse-local.

### 03563_system_queries_server_mode.sql
Test that verifies these SYSTEM queries still work properly in server mode (not affected by the changes).

### 03564_clickhouse_local_system_queries.sh
Shell script test that directly tests clickhouse-local behavior with SYSTEM queries.

### 03565_clickhouse_local_logical_error_fix.sql
Test that specifically verifies the old LOGICAL_ERROR behavior has been fixed.

### 03566_system_queries_comprehensive_test.sql
Comprehensive test that covers all aspects of the fix:
- Queries that should fail with UNSUPPORTED_METHOD in clickhouse-local
- Queries that should work in both clickhouse-local and server mode
- Verification of proper error messages
- Testing with different server types

## Integration Test

### test_clickhouse_local_system_queries/
Python integration test that verifies:
- clickhouse-local properly handles unsupported SYSTEM queries
- clickhouse-local properly handles supported SYSTEM queries
- Server mode continues to work correctly
- Error messages are specific and helpful

## Expected Behavior

### In clickhouse-local:
- `SYSTEM RELOAD CONFIG` → `UNSUPPORTED_METHOD` with message "SYSTEM RELOAD CONFIG query is not supported in clickhouse-local"
- `SYSTEM STOP LISTEN HTTP` → `UNSUPPORTED_METHOD` with message "SYSTEM STOP LISTEN HTTP query is not supported in clickhouse-local"
- `SYSTEM START LISTEN HTTP` → `UNSUPPORTED_METHOD` with message "SYSTEM START LISTEN HTTP query is not supported in clickhouse-local"
- Other SYSTEM queries (like `SYSTEM DROP DNS CACHE`) → Should continue to work

### In server mode:
- All SYSTEM queries should continue to work as before
- No regression in functionality

## Code Changes

The fix was implemented in `src/Interpreters/InterpreterSystemQuery.cpp`:

1. **RELOAD_CONFIG case**: Added check for `Context::ApplicationType::LOCAL`
2. **STOP_LISTEN case**: Added check for `Context::ApplicationType::LOCAL`
3. **START_LISTEN case**: Added check for `Context::ApplicationType::LOCAL`

Each case now throws `Exception::createDeprecated` with `ErrorCodes::UNSUPPORTED_METHOD` and a descriptive message mentioning clickhouse-local.

## Running the Tests

```bash
# Run the stateless tests
./tests/queries/0_stateless/03562_clickhouse_local_system_queries.sh

# Run the integration test
cd tests/integration/test_clickhouse_local_system_queries/
python3 test.py
``` 