package test

import "os"

const defaultClickHouseVersion = "latest"

func GetClickHouseTestVersion() string {
	return GetEnv("CLICKHOUSE_VERSION", defaultClickHouseVersion)
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
