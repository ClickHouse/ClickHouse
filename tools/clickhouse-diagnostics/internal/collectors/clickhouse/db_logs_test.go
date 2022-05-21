package clickhouse_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/clickhouse"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDbLogsConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for summary collector", func(t *testing.T) {
		client := test.NewFakeClickhouseClient(make(map[string][]string))
		dbLogsCollector := clickhouse.NewDBLogsCollector(&platform.ResourceManager{
			DbClient: client,
		})
		conf := dbLogsCollector.Configuration()
		require.Len(t, conf.Params, 1)
		require.IsType(t, config.IntParam{}, conf.Params[0])
		rowLimit, ok := conf.Params[0].(config.IntParam)
		require.True(t, ok)
		require.False(t, rowLimit.Required())
		require.Equal(t, rowLimit.Name(), "row_limit")
		require.Equal(t, int64(100000), rowLimit.Value)
	})
}

func TestDbLogsCollect(t *testing.T) {
	client := test.NewFakeClickhouseClient(make(map[string][]string))
	dbLogsCollector := clickhouse.NewDBLogsCollector(&platform.ResourceManager{
		DbClient: client,
	})
	queryLogColumns := []string{"type", "event_date", "event_time", "event_time_microseconds",
		"query_start_time", "query_start_time_microseconds", "query_duration_ms", "read_rows", "read_bytes", "written_rows", "written_bytes",
		"result_rows", "result_bytes", "memory_usage", "current_database", "query", "formatted_query", "normalized_query_hash",
		"query_kind", "databases", "tables", "columns", "projections", "views", "exception_code", "exception", "stack_trace",
		"is_initial_query", "user", "query_id", "address", "port", "initial_user", "initial_query_id", "initial_address", "initial_port",
		"initial_query_start_time", "initial_query_start_time_microseconds", "interface", "os_user", "client_hostname", "client_name",
		"client_revision", "client_version_major", "client_version_minor", "client_version_patch", "http_method", "http_user_agent",
		"http_referer", "forwarded_for", "quota_key", "revision", "log_comment", "thread_ids", "ProfileEvents", "Settings",
		"used_aggregate_functions", "used_aggregate_function_combinators", "used_database_engines", "used_data_type_families",
		"used_dictionaries", "used_formats", "used_functions", "used_storages", "used_table_functions"}
	queryLogFrame := test.NewFakeDataFrame("queryLog", queryLogColumns,
		[][]interface{}{
			{"QueryStart", "2021-12-13", "2021-12-13 12:53:20", "2021-12-13 12:53:20.590579", "2021-12-13 12:53:20", "2021-12-13 12:53:20.590579", "0", "0", "0", "0", "0", "0", "0", "0", "default", "SELECT DISTINCT arrayJoin(extractAll(name, '[\\w_]{2,}')) AS res FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.table_engines UNION ALL SELECT name FROM system.formats UNION ALL SELECT name FROM system.table_functions UNION ALL SELECT name FROM system.data_type_families UNION ALL SELECT name FROM system.merge_tree_settings UNION ALL SELECT name FROM system.settings UNION ALL SELECT cluster FROM system.clusters UNION ALL SELECT macro FROM system.macros UNION ALL SELECT policy_name FROM system.storage_policies UNION ALL SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate UNION ALL SELECT name FROM system.databases LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.tables LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.dictionaries LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.columns LIMIT 10000) WHERE notEmpty(res)", "", "6666026786019643712", "Select", "['system']", "['system.aggregate_function_combinators','system.clusters','system.columns','system.data_type_families','system.databases','system.dictionaries','system.formats','system.functions','system.macros','system.merge_tree_settings','system.settings','system.storage_policies','system.table_engines','system.table_functions','system.tables']", "['system.aggregate_function_combinators.name','system.clusters.cluster','system.columns.name','system.data_type_families.name','system.databases.name','system.dictionaries.name','system.formats.name','system.functions.is_aggregate','system.functions.name','system.macros.macro','system.merge_tree_settings.name','system.settings.name','system.storage_policies.policy_name','system.table_engines.name','system.table_functions.name','system.tables.name']", "[]", "[]", "0", "", "", "1", "default", "3b5feb6d-3086-4718-adb2-17464988ff12", "::ffff:127.0.0.1", "50920", "default", "3b5feb6d-3086-4718-adb2-17464988ff12", "::ffff:127.0.0.1", "50920", "2021-12-13 12:53:30", "2021-12-13 12:53:30.590579", "1", "", "", "ClickHouse client", "54450", "21", "11", "0", "0", "", "", "", "", "54456", "", "[]", "{}", "{'load_balancing':'random','max_memory_usage':'10000000000'}", "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]"},
			{"QueryFinish", "2021-12-13", "2021-12-13 12:53:30", "2021-12-13 12:53:30.607292", "2021-12-13 12:53:30", "2021-12-13 12:53:30.590579", "15", "4512", "255694", "0", "0", "4358", "173248", "4415230", "default", "SELECT DISTINCT arrayJoin(extractAll(name, '[\\w_]{2,}')) AS res FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.table_engines UNION ALL SELECT name FROM system.formats UNION ALL SELECT name FROM system.table_functions UNION ALL SELECT name FROM system.data_type_families UNION ALL SELECT name FROM system.merge_tree_settings UNION ALL SELECT name FROM system.settings UNION ALL SELECT cluster FROM system.clusters UNION ALL SELECT macro FROM system.macros UNION ALL SELECT policy_name FROM system.storage_policies UNION ALL SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate UNION ALL SELECT name FROM system.databases LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.tables LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.dictionaries LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.columns LIMIT 10000) WHERE notEmpty(res)", "", "6666026786019643712", "Select", "['system']", "['system.aggregate_function_combinators','system.clusters','system.columns','system.data_type_families','system.databases','system.dictionaries','system.formats','system.functions','system.macros','system.merge_tree_settings','system.settings','system.storage_policies','system.table_engines','system.table_functions','system.tables']", "['system.aggregate_function_combinators.name','system.clusters.cluster','system.columns.name','system.data_type_families.name','system.databases.name','system.dictionaries.name','system.formats.name','system.functions.is_aggregate','system.functions.name','system.macros.macro','system.merge_tree_settings.name','system.settings.name','system.storage_policies.policy_name','system.table_engines.name','system.table_functions.name','system.tables.name']", "[]", "[]", "0", "", "", "1", "default", "3b5feb6d-3086-4718-adb2-17464988ff12", "::ffff:127.0.0.1", "50920", "default", "3b5feb6d-3086-4718-adb2-17464988ff12", "::ffff:127.0.0.1", "50920", "2021-12-13 12:53:30", "2021-12-13 12:53:30.590579", "1", "", "", "ClickHouse client", "54450", "21", "11", "0", "0", "", "", "", "", "54456", "", "[95298,95315,95587,95316,95312,95589,95318,95586,95588,95585]", "{'Query':1,'SelectQuery':1,'ArenaAllocChunks':41,'ArenaAllocBytes':401408,'FunctionExecute':62,'NetworkSendElapsedMicroseconds':463,'NetworkSendBytes':88452,'SelectedRows':4512,'SelectedBytes':255694,'RegexpCreated':6,'ContextLock':411,'RWLockAcquiredReadLocks':190,'RealTimeMicroseconds':49221,'UserTimeMicroseconds':19811,'SystemTimeMicroseconds':2817,'SoftPageFaults':1128,'OSCPUWaitMicroseconds':127,'OSCPUVirtualTimeMicroseconds':22624,'OSWriteBytes':12288,'OSWriteChars':13312}", "{'load_balancing':'random','max_memory_usage':'10000000000'}", "[]", "[]", "[]", "[]", "[]", "[]", "['concat','notEmpty','extractAll']", "[]", "[]"},
			{"QueryStart", "2021-12-13", "2021-12-13 13:02:53", "2021-12-13 13:02:53.419528", "2021-12-13 13:02:53", "2021-12-13 13:02:53.419528", "0", "0", "0", "0", "0", "0", "0", "0", "default", "SELECT DISTINCT arrayJoin(extractAll(name, '[\\w_]{2,}')) AS res FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.table_engines UNION ALL SELECT name FROM system.formats UNION ALL SELECT name FROM system.table_functions UNION ALL SELECT name FROM system.data_type_families UNION ALL SELECT name FROM system.merge_tree_settings UNION ALL SELECT name FROM system.settings UNION ALL SELECT cluster FROM system.clusters UNION ALL SELECT macro FROM system.macros UNION ALL SELECT policy_name FROM system.storage_policies UNION ALL SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate UNION ALL SELECT name FROM system.databases LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.tables LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.dictionaries LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.columns LIMIT 10000) WHERE notEmpty(res)", "", "6666026786019643712", "Select", "['system']", "['system.aggregate_function_combinators','system.clusters','system.columns','system.data_type_families','system.databases','system.dictionaries','system.formats','system.functions','system.macros','system.merge_tree_settings','system.settings','system.storage_policies','system.table_engines','system.table_functions','system.tables']", "['system.aggregate_function_combinators.name','system.clusters.cluster','system.columns.name','system.data_type_families.name','system.databases.name','system.dictionaries.name','system.formats.name','system.functions.is_aggregate','system.functions.name','system.macros.macro','system.merge_tree_settings.name','system.settings.name','system.storage_policies.policy_name','system.table_engines.name','system.table_functions.name','system.tables.name']", "[]", "[]", "0", "", "", "1", "default", "351b58e4-6128-47d4-a7b8-03d78c1f84c6", "::ffff:127.0.0.1", "50968", "default", "351b58e4-6128-47d4-a7b8-03d78c1f84c6", "::ffff:127.0.0.1", "50968", "2021-12-13 13:02:53", "2021-12-13 13:02:53.419528", "1", "", "", "ClickHouse client", "54450", "21", "11", "0", "0", "", "", "", "", "54456", "", "[]", "{}", "{'load_balancing':'random','max_memory_usage':'10000000000'}", "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]"},
			{"QueryFinish", "2021-12-13", "2021-12-13 13:02:56", "2021-12-13 13:02:56.437115", "2021-12-13 13:02:56", "2021-12-13 13:02:56.419528", "16", "4629", "258376", "0", "0", "4377", "174272", "4404694", "default", "SELECT DISTINCT arrayJoin(extractAll(name, '[\\w_]{2,}')) AS res FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.table_engines UNION ALL SELECT name FROM system.formats UNION ALL SELECT name FROM system.table_functions UNION ALL SELECT name FROM system.data_type_families UNION ALL SELECT name FROM system.merge_tree_settings UNION ALL SELECT name FROM system.settings UNION ALL SELECT cluster FROM system.clusters UNION ALL SELECT macro FROM system.macros UNION ALL SELECT policy_name FROM system.storage_policies UNION ALL SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate UNION ALL SELECT name FROM system.databases LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.tables LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.dictionaries LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.columns LIMIT 10000) WHERE notEmpty(res)", "", "6666026786019643712", "Select", "['system']", "['system.aggregate_function_combinators','system.clusters','system.columns','system.data_type_families','system.databases','system.dictionaries','system.formats','system.functions','system.macros','system.merge_tree_settings','system.settings','system.storage_policies','system.table_engines','system.table_functions','system.tables']", "['system.aggregate_function_combinators.name','system.clusters.cluster','system.columns.name','system.data_type_families.name','system.databases.name','system.dictionaries.name','system.formats.name','system.functions.is_aggregate','system.functions.name','system.macros.macro','system.merge_tree_settings.name','system.settings.name','system.storage_policies.policy_name','system.table_engines.name','system.table_functions.name','system.tables.name']", "[]", "[]", "0", "", "", "1", "default", "351b58e4-6128-47d4-a7b8-03d78c1f84c6", "::ffff:127.0.0.1", "50968", "default", "351b58e4-6128-47d4-a7b8-03d78c1f84c6", "::ffff:127.0.0.1", "50968", "2021-12-13 13:02:53", "2021-12-13 13:02:53.419528", "1", "", "", "ClickHouse client", "54450", "21", "11", "0", "0", "", "", "", "", "54456", "", "[95298,95318,95315,95316,95312,95588,95589,95586,95585,95587]", "{'Query':1,'SelectQuery':1,'ArenaAllocChunks':41,'ArenaAllocBytes':401408,'FunctionExecute':62,'NetworkSendElapsedMicroseconds':740,'NetworkSendBytes':88794,'SelectedRows':4629,'SelectedBytes':258376,'ContextLock':411,'RWLockAcquiredReadLocks':194,'RealTimeMicroseconds':52469,'UserTimeMicroseconds':17179,'SystemTimeMicroseconds':4218,'SoftPageFaults':569,'OSCPUWaitMicroseconds':303,'OSCPUVirtualTimeMicroseconds':25087,'OSWriteBytes':12288,'OSWriteChars':12288}", "{'load_balancing':'random','max_memory_usage':'10000000000'}", "[]", "[]", "[]", "[]", "[]", "[]", "['concat','notEmpty','extractAll']", "[]", "[]"},
		})

	client.QueryResponses["SELECT * FROM system.query_log ORDER BY event_time_microseconds ASC LIMIT 100000"] = &queryLogFrame

	textLogColumns := []string{"event_date", "event_time", "event_time_microseconds", "microseconds", "thread_name", "thread_id", "level", "query_id", "logger_name", "message", "revision", "source_file", "source_line"}
	textLogFrame := test.NewFakeDataFrame("textLog", textLogColumns,
		[][]interface{}{
			{"2022-02-03", "2022-02-03 16:17:47", "2022-02-03 16:37:17.056950", "56950", "clickhouse-serv", "68947", "Information", "", "DNSCacheUpdater", "Update period 15 seconds", "54458", "../src/Interpreters/DNSCacheUpdater.cpp; void DB::DNSCacheUpdater::start()", "46"},
			{"2022-02-03", "2022-02-03 16:27:47", "2022-02-03 16:37:27.057022", "57022", "clickhouse-serv", "68947", "Information", "", "Application", "Available RAM: 62.24 GiB; physical cores: 8; logical cores: 16.", "54458", "../programs/server/Server.cpp; virtual int DB::Server::main(const std::vector<std::string> &)", "1380"},
			{"2022-02-03", "2022-02-03 16:37:47", "2022-02-03 16:37:37.057484", "57484", "clickhouse-serv", "68947", "Information", "", "Application", "Listening for http://[::1]:8123", "54458", "../programs/server/Server.cpp; virtual int DB::Server::main(const std::vector<std::string> &)", "1444"},
			{"2022-02-03", "2022-02-03 16:47:47", "2022-02-03 16:37:47.057527", "57527", "clickhouse-serv", "68947", "Information", "", "Application", "Listening for native protocol (tcp): [::1]:9000", "54458", "../programs/server/Server.cpp; virtual int DB::Server::main(const std::vector<std::string> &)", "1444"},
		})

	client.QueryResponses["SELECT * FROM system.text_log ORDER BY event_time_microseconds ASC LIMIT 100000"] = &textLogFrame

	// skip query_thread_log frame - often it doesn't exist anyway unless enabled
	t.Run("test default db logs collection", func(t *testing.T) {
		bundle, errs := dbLogsCollector.Collect(config.Configuration{})
		require.Empty(t, errs)
		require.NotNil(t, bundle)
		require.Len(t, bundle.Frames, 2)
		require.Contains(t, bundle.Frames, "text_log")
		require.Contains(t, bundle.Frames, "query_log")
		require.Len(t, bundle.Errors.Errors, 1)
		// check query_log frame
		require.Contains(t, bundle.Frames, "query_log")
		require.Equal(t, queryLogColumns, bundle.Frames["query_log"].Columns())
		checkFrame(t, bundle.Frames["query_log"], queryLogFrame.Rows)
		//check text_log frame
		require.Contains(t, bundle.Frames, "text_log")
		require.Equal(t, textLogColumns, bundle.Frames["text_log"].Columns())
		checkFrame(t, bundle.Frames["text_log"], textLogFrame.Rows)
		client.Reset()
	})

	t.Run("test db logs collection with limit", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.IntParam{
					Value: 1,
					Param: config.NewParam("row_limit", "Maximum number of log rows to collect. Negative values mean unlimited", false),
				},
			},
		}
		bundle, err := dbLogsCollector.Collect(conf)
		require.Empty(t, err)
		require.NotNil(t, bundle)
		require.Len(t, bundle.Frames, 0)
		require.Len(t, bundle.Errors.Errors, 3)
		// populate client
		client.QueryResponses["SELECT * FROM system.query_log ORDER BY event_time_microseconds ASC LIMIT 1"] = &queryLogFrame
		client.QueryResponses["SELECT * FROM system.text_log ORDER BY event_time_microseconds ASC LIMIT 1"] = &textLogFrame
		bundle, err = dbLogsCollector.Collect(conf)
		require.Empty(t, err)
		require.Len(t, bundle.Frames, 2)
		require.Len(t, bundle.Errors.Errors, 1)
		require.Contains(t, bundle.Frames, "text_log")
		require.Contains(t, bundle.Frames, "query_log")
		// check query_log frame
		require.Contains(t, bundle.Frames, "query_log")
		require.Equal(t, queryLogColumns, bundle.Frames["query_log"].Columns())
		checkFrame(t, bundle.Frames["query_log"], queryLogFrame.Rows[:1])
		//check text_log frame
		require.Contains(t, bundle.Frames, "text_log")
		require.Equal(t, textLogColumns, bundle.Frames["text_log"].Columns())
		checkFrame(t, bundle.Frames["text_log"], textLogFrame.Rows[:1])
		client.Reset()
	})
}
