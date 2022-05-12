package clickhouse_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/clickhouse"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSummaryConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for summary collector", func(t *testing.T) {
		client := test.NewFakeClickhouseClient(make(map[string][]string))
		summaryCollector := clickhouse.NewSummaryCollector(&platform.ResourceManager{
			DbClient: client,
		})
		conf := summaryCollector.Configuration()
		require.Len(t, conf.Params, 1)
		require.IsType(t, config.IntParam{}, conf.Params[0])
		limit, ok := conf.Params[0].(config.IntParam)
		require.True(t, ok)
		require.False(t, limit.Required())
		require.Equal(t, limit.Name(), "row_limit")
		require.Equal(t, int64(20), limit.Value)
	})
}

func TestSummaryCollection(t *testing.T) {

	client := test.NewFakeClickhouseClient(make(map[string][]string))
	versionFrame := test.NewFakeDataFrame("version", []string{"version()"},
		[][]interface{}{
			{"22.1.3.7"},
		},
	)
	client.QueryResponses["SELECT version()"] = &versionFrame
	databasesFrame := test.NewFakeDataFrame("databases", []string{"name", "engine", "tables", "partitions", "parts", "disk_size"},
		[][]interface{}{
			{"tutorial", "Atomic", 2, 2, 2, "1.70 GiB"},
			{"default", "Atomic", 5, 5, 6, "1.08 GiB"},
			{"system", "Atomic", 11, 24, 70, "1.05 GiB"},
			{"INFORMATION_SCHEMA", "Memory", 0, 0, 0, "0.00 B"},
			{"covid19db", "Atomic", 0, 0, 0, "0.00 B"},
			{"information_schema", "Memory", 0, 0, 0, "0.00 B"}})

	client.QueryResponses["SELECT name, engine, tables, partitions, parts, formatReadableSize(bytes_on_disk) \"disk_size\" "+
		"FROM system.databases db LEFT JOIN ( SELECT database, uniq(table) \"tables\", uniq(table, partition) \"partitions\", "+
		"count() AS parts, sum(bytes_on_disk) \"bytes_on_disk\" FROM system.parts WHERE active GROUP BY database ) AS db_stats "+
		"ON db.name = db_stats.database ORDER BY bytes_on_disk DESC LIMIT 20"] = &databasesFrame

	summaryCollector := clickhouse.NewSummaryCollector(&platform.ResourceManager{
		DbClient: client,
	})

	t.Run("test default summary collection", func(t *testing.T) {
		bundle, errs := summaryCollector.Collect(config.Configuration{})
		require.Empty(t, errs)
		require.Len(t, bundle.Errors.Errors, 30)
		require.NotNil(t, bundle)
		require.Len(t, bundle.Frames, 2)
		// check version frame
		require.Contains(t, bundle.Frames, "version")
		require.Equal(t, []string{"version()"}, bundle.Frames["version"].Columns())
		checkFrame(t, bundle.Frames["version"], versionFrame.Rows)
		//check databases frame
		require.Contains(t, bundle.Frames, "databases")
		require.Equal(t, []string{"name", "engine", "tables", "partitions", "parts", "disk_size"}, bundle.Frames["databases"].Columns())
		checkFrame(t, bundle.Frames["databases"], databasesFrame.Rows)
		client.Reset()
	})

	t.Run("test summary collection with limit", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.IntParam{
					Value: 1,
					Param: config.NewParam("row_limit", "Limit rows on supported queries.", false),
				},
			},
		}
		bundle, errs := summaryCollector.Collect(conf)

		require.Empty(t, errs)
		require.Len(t, bundle.Errors.Errors, 31)
		require.NotNil(t, bundle)
		// databases will be absent due to limit
		require.Len(t, bundle.Frames, 1)
		// check version frame
		require.Contains(t, bundle.Frames, "version")
		require.Equal(t, []string{"version()"}, bundle.Frames["version"].Columns())
		checkFrame(t, bundle.Frames["version"], versionFrame.Rows)

		client.QueryResponses["SELECT name, engine, tables, partitions, parts, formatReadableSize(bytes_on_disk) \"disk_size\" "+
			"FROM system.databases db LEFT JOIN ( SELECT database, uniq(table) \"tables\", uniq(table, partition) \"partitions\", "+
			"count() AS parts, sum(bytes_on_disk) \"bytes_on_disk\" FROM system.parts WHERE active GROUP BY database ) AS db_stats "+
			"ON db.name = db_stats.database ORDER BY bytes_on_disk DESC LIMIT 1"] = &databasesFrame
		bundle, errs = summaryCollector.Collect(conf)
		require.Empty(t, errs)
		require.Len(t, bundle.Errors.Errors, 30)
		require.NotNil(t, bundle)
		require.Len(t, bundle.Frames, 2)
		require.Contains(t, bundle.Frames, "version")
		//check databases frame
		require.Contains(t, bundle.Frames, "databases")
		require.Equal(t, []string{"name", "engine", "tables", "partitions", "parts", "disk_size"}, bundle.Frames["databases"].Columns())
		// this will parse as our mock client does not read statement (specifically the limit clause) when called with execute
		checkFrame(t, bundle.Frames["databases"], databasesFrame.Rows)
	})
}
