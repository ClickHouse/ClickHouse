#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `mongodb` table function needs USE_MONGODB

# A MongoDB server that returned a malformed document in a cursor batch used to abort the ClickHouse server
# (`BSON_ASSERT` in `_mongoc_cursor_response_read` of the C driver). Now the query fails with an exception.
# The Python script starts a fake MongoDB server that answers `find` and `getMore` with hand-encoded replies.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 "$CUR_DIR"/05828_mongodb_malformed_cursor_batch.python
