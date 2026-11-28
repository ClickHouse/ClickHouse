#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A Parquet file written by ClickHouse 26.10 with 100 rows in one UInt64 column x: two ZSTD pages without a content size.
DATA='UEFSMRUEFcAMFbICTBXIARUAAAAotS/9BFhMBADiDCASELjeDfz////fmVVVVVVVxZYC/6JEhwoNCvSnz548d+rMifOmzZo0Z8qMCfOly5YsV6pMifKkyZIkR4oMCfKjx44cN2rMiPGixYoUJ0qMCPGhw4YMFypMiPCgwYIEBwoMCPDXt5d3VzcX99a2lnZWNhb21bWVdVU1FfXUtJR0dGNUAgADAQEAAKlTAn0VABW6ARXaASwVyAEVEBUGFQYcWAhjAAAAAAAAABgIAAAAAAAAAAAREQAAACi1L/0EWOgCAAcbgIBgQCgYDoiEYsFoOB6QiGRCqVgumIxmw+l4PqCQaEQqmU6olGrFarlesJhsRqvZbricbsfr+X7AoHBILBqPyKRyyWw6n9CodEqtWq/YrHbL7Xq/4LB4DAAAAAEAAFBQEnMmjAUcFQQZJQAQGRgBeBUMFsgBFvIOFoQFJtoCJggcWAhjAAAAAAAAABgIAAAAAAAAAAAREQAZLBUEFQAVAgAVABUQFQIAPAAAABUEGSxIBnNjaGVtYRUCABUEJQAYAXglHEysE0ASAAAAFsgBGRwZHCaMBRwVBBklABAZGAF4FQwWyAEW8g4WhAUm2gImCBxYCGMAAAAAAAAAGAgAAAAAAAAAABERABksFQQVABUCABUAFRAVAgA8AAAAFvIOFsgBJggWhAUAKEtDbGlja0hvdXNlIHZlcnNpb24gMjYuMTAuNSAoYnVpbGQgMjVlYTc5Zjc4NGUxMmRiNmQyZDFiYzczZDhmOWZkZGNlNWQ4NTUzYikZHBwAAADQAAAAUEFSMQ=='

# Window_Descriptor of both frames (bytes 26 and 223) patched to 0x90: a 256 MiB window. The page is in memory and its
# size is known, so it decodes without a window buffer.
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(x) FROM format(Parquet, base64Decode('$DATA'))"
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(x) FROM format(Parquet, overlay(overlay(base64Decode('$DATA'), '\\x90', 26), '\\x90', 223))"
