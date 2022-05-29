package clickhouse_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/clickhouse"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSystemConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for system db collector", func(t *testing.T) {
		client := test.NewFakeClickhouseClient(make(map[string][]string))
		systemDbCollector := clickhouse.NewSystemDatabaseCollector(&platform.ResourceManager{
			DbClient: client,
		})
		conf := systemDbCollector.Configuration()
		require.Len(t, conf.Params, 3)
		// check first param
		require.IsType(t, config.StringListParam{}, conf.Params[0])
		includeTables, ok := conf.Params[0].(config.StringListParam)
		require.True(t, ok)
		require.False(t, includeTables.Required())
		require.Equal(t, includeTables.Name(), "include_tables")
		require.Nil(t, includeTables.Values)
		// check second param
		require.IsType(t, config.StringListParam{}, conf.Params[1])
		excludeTables, ok := conf.Params[1].(config.StringListParam)
		require.True(t, ok)
		require.False(t, excludeTables.Required())
		require.Equal(t, "exclude_tables", excludeTables.Name())
		require.Equal(t, []string{"licenses", "distributed_ddl_queue", "query_thread_log", "query_log", "asynchronous_metric_log", "zookeeper", "aggregate_function_combinators", "collations", "contributors", "data_type_families", "formats", "graphite_retentions", "numbers", "numbers_mt", "one", "parts_columns", "projection_parts", "projection_parts_columns", "table_engines", "time_zones", "zeros", "zeros_mt"}, excludeTables.Values)
		// check third param
		require.IsType(t, config.IntParam{}, conf.Params[2])
		rowLimit, ok := conf.Params[2].(config.IntParam)
		require.True(t, ok)
		require.False(t, rowLimit.Required())
		require.Equal(t, "row_limit", rowLimit.Name())
		require.Equal(t, int64(100000), rowLimit.Value)
	})
}

func TestSystemDbCollect(t *testing.T) {

	diskFrame := test.NewFakeDataFrame("disks", []string{"name", "path", "free_space", "total_space", "keep_free_space", "type"},
		[][]interface{}{
			{"default", "/var/lib/clickhouse", 1729659346944, 1938213220352, "", "local"},
		},
	)
	clusterFrame := test.NewFakeDataFrame("clusters", []string{"cluster", "shard_num", "shard_weight", "replica_num", "host_name", "host_address", "port", "is_local", "user", "default_database", "errors_count", "slowdowns_count", "estimated_recovery_time"},
		[][]interface{}{
			{"events", 1, 1, 1, "dalem-local-clickhouse-blue-1", "192.168.144.2", 9000, 1, "default", "", 0, 0, 0},
			{"events", 2, 1, 1, "dalem-local-clickhouse-blue-2", "192.168.144.4", 9000, 1, "default", "", 0, 0, 0},
			{"events", 3, 1, 1, "dalem-local-clickhouse-blue-3", "192.168.144.3", 9000, 1, "default", "", 0, 0, 0},
		},
	)
	userFrame := test.NewFakeDataFrame("users", []string{"name", "id", "storage", "auth_type", "auth_params", "host_ip", "host_names", "host_names_regexp", "host_names_like"},
		[][]interface{}{
			{"default", "94309d50-4f52-5250-31bd-74fecac179db,users.xml,plaintext_password", "sha256_password", []string{"::0"}, []string{}, []string{}, []string{}},
		},
	)

	dbTables := map[string][]string{
		clickhouse.SystemDatabase: {"disks", "clusters", "users"},
	}
	client := test.NewFakeClickhouseClient(dbTables)

	client.QueryResponses["SELECT * FROM system.disks LIMIT 100000"] = &diskFrame
	client.QueryResponses["SELECT * FROM system.clusters LIMIT 100000"] = &clusterFrame
	client.QueryResponses["SELECT * FROM system.users LIMIT 100000"] = &userFrame
	systemDbCollector := clickhouse.NewSystemDatabaseCollector(&platform.ResourceManager{
		DbClient: client,
	})

	t.Run("test default system db collection", func(t *testing.T) {
		diagSet, err := systemDbCollector.Collect(config.Configuration{})
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 3)
		// disks frame
		require.Equal(t, "disks", diagSet.Frames["disks"].Name())
		require.Equal(t, diskFrame.ColumnNames, diagSet.Frames["disks"].Columns())
		checkFrame(t, diagSet.Frames["disks"], diskFrame.Rows)
		// clusters frame
		require.Equal(t, "clusters", diagSet.Frames["clusters"].Name())
		require.Equal(t, clusterFrame.ColumnNames, diagSet.Frames["clusters"].Columns())
		checkFrame(t, diagSet.Frames["clusters"], clusterFrame.Rows)
		// users frame
		require.Equal(t, "users", diagSet.Frames["users"].Name())
		require.Equal(t, userFrame.ColumnNames, diagSet.Frames["users"].Columns())
		checkFrame(t, diagSet.Frames["users"], userFrame.Rows)
		client.Reset()
	})

	t.Run("test when we pass an includes", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					// nil means include everything
					Values: []string{"disks"},
					Param:  config.NewParam("include_tables", "Exclusion", false),
				},
			},
		}
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 1)
		// disks frame
		require.Equal(t, "disks", diagSet.Frames["disks"].Name())
		require.Equal(t, diskFrame.ColumnNames, diagSet.Frames["disks"].Columns())
		checkFrame(t, diagSet.Frames["disks"], diskFrame.Rows)
		client.Reset()
	})

	// test excludes
	t.Run("test when we pass an excludes", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					Values: []string{"disks"},
					Param:  config.NewParam("exclude_tables", "Exclusion", false),
				},
			},
		}
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 2)
		// clusters frame
		require.Equal(t, "clusters", diagSet.Frames["clusters"].Name())
		require.Equal(t, clusterFrame.ColumnNames, diagSet.Frames["clusters"].Columns())
		checkFrame(t, diagSet.Frames["clusters"], clusterFrame.Rows)
		// users frame
		require.Equal(t, "users", diagSet.Frames["users"].Name())
		require.Equal(t, userFrame.ColumnNames, diagSet.Frames["users"].Columns())
		checkFrame(t, diagSet.Frames["users"], userFrame.Rows)
		client.Reset()
	})

	// test includes which isn't in the list
	t.Run("test when we pass an invalid includes", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					// nil means include everything
					Values: []string{"disks", "invalid"},
					Param:  config.NewParam("include_tables", "Exclusion", false),
				},
			},
		}
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 1)
		require.Equal(t, diagSet.Errors.Error(), "some tables specified in the include_tables are not in the "+
			"system database and will not be exported: [invalid]")
		require.Len(t, diagSet.Frames, 1)
		// disks frame
		require.Equal(t, "disks", diagSet.Frames["disks"].Name())
		require.Equal(t, diskFrame.ColumnNames, diagSet.Frames["disks"].Columns())
		checkFrame(t, diagSet.Frames["disks"], diskFrame.Rows)
		client.Reset()
	})

	t.Run("test when we use a table with excluded fields", func(t *testing.T) {
		excludeDefault := clickhouse.ExcludeColumns
		client.QueryResponses["SELECT * EXCEPT(keep_free_space,type) FROM system.disks LIMIT 100000"] = &diskFrame
		clickhouse.ExcludeColumns = map[string][]string{
			"disks": {"keep_free_space", "type"},
		}
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					// nil means include everything
					Values: []string{"disks"},
					Param:  config.NewParam("include_tables", "Exclusion", false),
				},
			},
		}
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 1)
		// disks frame
		require.Equal(t, "disks", diagSet.Frames["disks"].Name())
		require.Equal(t, []string{"name", "path", "free_space", "total_space"}, diagSet.Frames["disks"].Columns())
		eDiskFrame := test.NewFakeDataFrame("disks", []string{"name", "path", "free_space", "total_space"},
			[][]interface{}{
				{"default", "/var/lib/clickhouse", 1729659346944, 1938213220352},
			},
		)
		checkFrame(t, diagSet.Frames["disks"], eDiskFrame.Rows)
		clickhouse.ExcludeColumns = excludeDefault
		client.Reset()
	})

	t.Run("test with a low row limit", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.IntParam{
					Value: 1,
					Param: config.NewParam("row_limit", "Maximum number of rows to collect from any table. Negative values mean unlimited.", false),
				},
			},
		}
		client.QueryResponses["SELECT * FROM system.disks LIMIT 1"] = &diskFrame
		client.QueryResponses["SELECT * FROM system.clusters LIMIT 1"] = &clusterFrame
		client.QueryResponses["SELECT * FROM system.users LIMIT 1"] = &userFrame
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 3)
		// clusters frame
		require.Equal(t, "clusters", diagSet.Frames["clusters"].Name())
		require.Equal(t, clusterFrame.ColumnNames, diagSet.Frames["clusters"].Columns())
		lClusterFrame := test.NewFakeDataFrame("clusters", []string{"cluster", "shard_num", "shard_weight", "replica_num", "host_name", "host_address", "port", "is_local", "user", "default_database", "errors_count", "slowdowns_count", "estimated_recovery_time"},
			[][]interface{}{
				{"events", 1, 1, 1, "dalem-local-clickhouse-blue-1", "192.168.144.2", 9000, 1, "default", "", 0, 0, 0},
			})
		checkFrame(t, diagSet.Frames["clusters"], lClusterFrame.Rows)
		client.Reset()
	})

	t.Run("test with a negative low row limit", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.IntParam{
					Value: -23,
					Param: config.NewParam("row_limit", "Maximum number of rows to collect from any table. Negative values mean unlimited.", false),
				},
			},
		}
		client.QueryResponses["SELECT * FROM system.clusters"] = &clusterFrame
		client.QueryResponses["SELECT * FROM system.disks"] = &diskFrame
		client.QueryResponses["SELECT * FROM system.users"] = &userFrame
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 3)
		// disks frame
		require.Equal(t, "disks", diagSet.Frames["disks"].Name())
		require.Equal(t, diskFrame.ColumnNames, diagSet.Frames["disks"].Columns())
		checkFrame(t, diagSet.Frames["disks"], diskFrame.Rows)
		// clusters frame
		require.Equal(t, "clusters", diagSet.Frames["clusters"].Name())
		require.Equal(t, clusterFrame.ColumnNames, diagSet.Frames["clusters"].Columns())
		checkFrame(t, diagSet.Frames["clusters"], clusterFrame.Rows)
		// users frame
		require.Equal(t, "users", diagSet.Frames["users"].Name())
		require.Equal(t, userFrame.ColumnNames, diagSet.Frames["users"].Columns())
		checkFrame(t, diagSet.Frames["users"], userFrame.Rows)
		client.Reset()
	})

	t.Run("test that includes overrides excludes", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					// nil means include everything
					Values: []string{"disks"},
					Param:  config.NewParam("exclude_tables", "Excluded", false),
				},
				config.StringListParam{
					// nil means include everything
					Values: []string{"disks", "clusters", "users"},
					Param:  config.NewParam("include_tables", "Included", false),
				},
			},
		}
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 3)
		client.Reset()
	})

	t.Run("test banned", func(t *testing.T) {
		bannedDefault := clickhouse.BannedTables
		clickhouse.BannedTables = []string{"disks"}
		diagSet, err := systemDbCollector.Collect(config.Configuration{})
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 2)
		require.Contains(t, diagSet.Frames, "users")
		require.Contains(t, diagSet.Frames, "clusters")
		clickhouse.BannedTables = bannedDefault
		client.Reset()
	})

	t.Run("test banned unless included", func(t *testing.T) {
		bannedDefault := clickhouse.BannedTables
		clickhouse.BannedTables = []string{"disks"}
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					// nil means include everything
					Values: []string{"disks", "clusters", "users"},
					Param:  config.NewParam("include_tables", "Included", false),
				},
			},
		}
		diagSet, err := systemDbCollector.Collect(conf)
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 3)
		require.Contains(t, diagSet.Frames, "disks")
		require.Contains(t, diagSet.Frames, "users")
		require.Contains(t, diagSet.Frames, "clusters")
		clickhouse.BannedTables = bannedDefault
		client.Reset()
	})

	t.Run("tables are ordered if configured", func(t *testing.T) {
		defaultOrderBy := clickhouse.OrderBy
		clickhouse.OrderBy = map[string]data.OrderBy{
			"clusters": {
				Column: "shard_num",
				Order:  data.Desc,
			},
		}
		client.QueryResponses["SELECT * FROM system.clusters ORDER BY shard_num DESC LIMIT 100000"] = &clusterFrame
		diagSet, err := systemDbCollector.Collect(config.Configuration{})
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Errors.Errors, 0)
		require.Len(t, diagSet.Frames, 3)
		clickhouse.OrderBy = defaultOrderBy
		oClusterFrame := test.NewFakeDataFrame("clusters", []string{"cluster", "shard_num", "shard_weight", "replica_num", "host_name", "host_address", "port", "is_local", "user", "default_database", "errors_count", "slowdowns_count", "estimated_recovery_time"},
			[][]interface{}{
				{"events", 3, 1, 1, "dalem-local-clickhouse-blue-3", "192.168.144.3", 9000, 1, "default", "", 0, 0, 0},
				{"events", 2, 1, 1, "dalem-local-clickhouse-blue-2", "192.168.144.4", 9000, 1, "default", "", 0, 0, 0},
				{"events", 1, 1, 1, "dalem-local-clickhouse-blue-1", "192.168.144.2", 9000, 1, "default", "", 0, 0, 0},
			},
		)
		checkFrame(t, diagSet.Frames["clusters"], oClusterFrame.Rows)
		client.Reset()
	})

}

func checkFrame(t *testing.T, frame data.Frame, rows [][]interface{}) {
	i := 0
	for {
		values, ok, err := frame.Next()
		require.Nil(t, err)
		if !ok {
			break
		}
		require.ElementsMatch(t, rows[i], values)
		i += 1
	}
	require.Equal(t, i, len(rows))
}
