#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# A `?` after ` :: ` globs the archive entries and is not folded into the key of an archive with spaces.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

archive="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05629 dir/a b.zip"

python3 -c "
import io, sys, zipfile
buffer = io.BytesIO()
with zipfile.ZipFile(buffer, 'w') as archive:
    archive.writestr('file1.csv', '42\n')
sys.stdout.buffer.write(buffer.getvalue())
" | ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3('$archive', 'test', 'testtest', 'RawBLOB', 'data String') SETTINGS s3_truncate_on_insert = 1 FORMAT RawBLOB"

for pattern in 'file1.csv' 'file*.csv' 'file?.csv'; do
    ${CLICKHOUSE_CLIENT} --query "SELECT '$pattern', x FROM s3('$archive :: $pattern', 'test', 'testtest', 'CSV', 'x UInt32')"
done
