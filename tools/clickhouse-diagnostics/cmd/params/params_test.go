package params_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/cmd/params"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"os"
	"sort"
	"testing"
)

var conf = map[string]config.Configuration{
	"config": {
		Params: []config.ConfigParam{
			config.StringParam{
				Value:      "",
				Param:      config.NewParam("directory", "A directory", false),
				AllowEmpty: true,
			},
		},
	},
	"system": {
		Params: []config.ConfigParam{
			config.StringListParam{
				// nil means include everything
				Values: nil,
				Param:  config.NewParam("include_tables", "Include tables", false),
			},
			config.StringListParam{
				Values: []string{"distributed_ddl_queue", "query_thread_log", "query_log", "asynchronous_metric_log", "zookeeper"},
				Param:  config.NewParam("exclude_tables", "Excluded tables", false),
			},
			config.IntParam{
				Value: 100000,
				Param: config.NewParam("row_limit", "Max rows", false),
			},
		},
	},
	"reader": {
		Params: []config.ConfigParam{
			config.StringOptions{
				Value:   "csv",
				Options: []string{"csv"},
				Param:   config.NewParam("format", "Format of imported files", false),
			},
			config.BoolParam{
				Value: true,
				Param: config.NewParam("collect_archives", "Collect archives", false),
			},
		},
	},
}

func TestNewParamMap(t *testing.T) {
	// test each of the types via NewParamMap - one with each type. the keys here can represent anything e.g. a collector name
	t.Run("test param map correctly converts types", func(t *testing.T) {
		paramMap := params.NewParamMap(conf)
		require.Len(t, paramMap, 3)
		// check config
		require.Contains(t, paramMap, "config")
		require.Len(t, paramMap["config"], 1)
		require.Contains(t, paramMap["config"], "directory")
		require.IsType(t, params.CliParam{}, paramMap["config"]["directory"])
		require.Equal(t, "A directory", paramMap["config"]["directory"].Description)
		require.Equal(t, "", *(paramMap["config"]["directory"].Value.(*string)))
		require.Equal(t, "", paramMap["config"]["directory"].Default)
		require.Equal(t, params.String, paramMap["config"]["directory"].Type)
		// check system
		require.Contains(t, paramMap, "system")
		require.Len(t, paramMap["system"], 3)
		require.IsType(t, params.CliParam{}, paramMap["system"]["include_tables"])

		require.Equal(t, "Include tables", paramMap["system"]["include_tables"].Description)
		var value []string
		require.Equal(t, &value, paramMap["system"]["include_tables"].Value)
		require.Equal(t, value, paramMap["system"]["include_tables"].Default)
		require.Equal(t, params.StringList, paramMap["system"]["include_tables"].Type)

		require.Equal(t, "Excluded tables", paramMap["system"]["exclude_tables"].Description)
		require.IsType(t, params.CliParam{}, paramMap["system"]["exclude_tables"])
		require.Equal(t, &value, paramMap["system"]["exclude_tables"].Value)
		require.Equal(t, []string{"distributed_ddl_queue", "query_thread_log", "query_log", "asynchronous_metric_log", "zookeeper"}, paramMap["system"]["exclude_tables"].Default)
		require.Equal(t, params.StringList, paramMap["system"]["exclude_tables"].Type)

		require.Equal(t, "Max rows", paramMap["system"]["row_limit"].Description)
		require.IsType(t, params.CliParam{}, paramMap["system"]["row_limit"])
		var iValue int64
		require.Equal(t, &iValue, paramMap["system"]["row_limit"].Value)
		require.Equal(t, int64(100000), paramMap["system"]["row_limit"].Default)
		require.Equal(t, params.Integer, paramMap["system"]["row_limit"].Type)

		// check reader
		require.Contains(t, paramMap, "reader")
		require.Len(t, paramMap["reader"], 2)
		require.IsType(t, params.CliParam{}, paramMap["reader"]["format"])
		require.Equal(t, "Format of imported files", paramMap["reader"]["format"].Description)
		require.IsType(t, params.CliParam{}, paramMap["reader"]["format"])
		oValue := params.StringOptionsVar{
			Options: []string{"csv"},
			Value:   "csv",
		}
		require.Equal(t, &oValue, paramMap["reader"]["format"].Value)
		require.Equal(t, "csv", paramMap["reader"]["format"].Default)
		require.Equal(t, params.StringOptionsList, paramMap["reader"]["format"].Type)

		require.IsType(t, params.CliParam{}, paramMap["reader"]["collect_archives"])
		require.Equal(t, "Collect archives", paramMap["reader"]["collect_archives"].Description)
		require.IsType(t, params.CliParam{}, paramMap["reader"]["collect_archives"])
		var bVar bool
		require.Equal(t, &bVar, paramMap["reader"]["collect_archives"].Value)
		require.Equal(t, true, paramMap["reader"]["collect_archives"].Default)
		require.Equal(t, params.Boolean, paramMap["reader"]["collect_archives"].Type)

	})

}

//  test GetConfigParam
func TestConvertParamsToConfig(t *testing.T) {
	paramMap := params.NewParamMap(conf)
	t.Run("test we can convert a param map back to a config", func(t *testing.T) {
		cParam := params.ConvertParamsToConfig(paramMap)
		// these will not be equal as we have some information loss e.g. allowEmpty
		//require.Equal(t, conf, cParam)
		// deep equality
		for name := range conf {
			require.Equal(t, len(conf[name].Params), len(cParam[name].Params))
			// sort both consistently
			sort.Slice(conf[name].Params, func(i, j int) bool {
				return conf[name].Params[i].Name() < conf[name].Params[j].Name()
			})
			sort.Slice(cParam[name].Params, func(i, j int) bool {
				return cParam[name].Params[i].Name() < cParam[name].Params[j].Name()
			})
			for i, param := range conf[name].Params {
				require.Equal(t, param.Required(), cParam[name].Params[i].Required())
				require.Equal(t, param.Name(), cParam[name].Params[i].Name())
				require.Equal(t, param.Description(), cParam[name].Params[i].Description())
			}
		}
	})
}

// create via NewParamMap and add to command AddParamMapToCmd - check contents
func TestAddParamMapToCmd(t *testing.T) {
	paramMap := params.NewParamMap(conf)
	t.Run("test we can add hidden params to a command", func(t *testing.T) {
		testComand := &cobra.Command{
			Use:   "test",
			Short: "Run a test",
			Long:  `Longer description`,
			Run: func(cmd *cobra.Command, args []string) {
				os.Exit(0)
			},
		}
		params.AddParamMapToCmd(paramMap, testComand, "collector", true)
		// check we get an error on one which doesn't exist
		_, err := testComand.Flags().GetString("collector.config.random")
		require.NotNil(t, err)
		// check getting incorrect type
		_, err = testComand.Flags().GetString("collector.system.include_tables")
		require.NotNil(t, err)

		// check existence of all flags
		directory, err := testComand.Flags().GetString("collector.config.directory")
		require.Nil(t, err)
		require.Equal(t, "", directory)

		includeTables, err := testComand.Flags().GetStringSlice("collector.system.include_tables")
		require.Nil(t, err)
		require.Equal(t, []string{}, includeTables)

		excludeTables, err := testComand.Flags().GetStringSlice("collector.system.exclude_tables")
		require.Nil(t, err)
		require.Equal(t, []string{"distributed_ddl_queue", "query_thread_log", "query_log", "asynchronous_metric_log", "zookeeper"}, excludeTables)

		rowLimit, err := testComand.Flags().GetInt64("collector.system.row_limit")
		require.Nil(t, err)
		require.Equal(t, int64(100000), rowLimit)

		format, err := testComand.Flags().GetString("collector.reader.format")
		require.Nil(t, err)
		require.Equal(t, "csv", format)

		collectArchives, err := testComand.Flags().GetBool("collector.reader.collect_archives")
		require.Nil(t, err)
		require.Equal(t, true, collectArchives)
	})
}

// test StringOptionsVar
func TestStringOptionsVar(t *testing.T) {

	t.Run("test we can set", func(t *testing.T) {
		format := params.StringOptionsVar{
			Options: []string{"csv", "tsv", "native"},
			Value:   "csv",
		}
		require.Equal(t, "csv", format.String())
		err := format.Set("tsv")
		require.Nil(t, err)
		require.Equal(t, "tsv", format.String())
	})

	t.Run("test set invalid", func(t *testing.T) {
		format := params.StringOptionsVar{
			Options: []string{"csv", "tsv", "native"},
			Value:   "csv",
		}
		require.Equal(t, "csv", format.String())
		err := format.Set("random")
		require.NotNil(t, err)
		require.Equal(t, "random is not included in options: [csv tsv native]", err.Error())
	})
}

// test StringSliceOptionsVar
func TestStringSliceOptionsVar(t *testing.T) {

	t.Run("test we can set", func(t *testing.T) {
		formats := params.StringSliceOptionsVar{
			Options: []string{"csv", "tsv", "native", "qsv"},
			Values:  []string{"csv", "tsv"},
		}
		require.Equal(t, "[csv,tsv]", formats.String())
		err := formats.Set("tsv,native")
		require.Nil(t, err)
		require.Equal(t, "[tsv,native]", formats.String())
	})

	t.Run("test set invalid", func(t *testing.T) {
		formats := params.StringSliceOptionsVar{
			Options: []string{"csv", "tsv", "native", "qsv"},
			Values:  []string{"csv", "tsv"},
		}
		require.Equal(t, "[csv,tsv]", formats.String())
		err := formats.Set("tsv,random")
		require.NotNil(t, err)
		require.Equal(t, "[random] are not included in options: [csv tsv native qsv]", err.Error())
		err = formats.Set("msv,random")
		require.NotNil(t, err)
		require.Equal(t, "[msv random] are not included in options: [csv tsv native qsv]", err.Error())
	})

}
