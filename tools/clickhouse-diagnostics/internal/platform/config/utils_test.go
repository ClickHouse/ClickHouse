package config_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadStringListValues(t *testing.T) {

	t.Run("can find a string list param", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					// nil means include everything
					Values: nil,
					Param:  config.NewParam("include_tables", "Specify list of tables to collect", false),
				},
				config.StringListParam{
					Values: []string{"licenses", "settings"},
					Param:  config.NewParam("exclude_tables", "Specify list of tables not to collect", false),
				},
			},
		}
		excludeTables, err := config.ReadStringListValues(conf, "exclude_tables")
		require.Nil(t, err)
		require.Equal(t, []string{"licenses", "settings"}, excludeTables)
	})

}

func TestReadStringValue(t *testing.T) {

	t.Run("can find a string param", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringListParam{
					// nil means include everything
					Values: nil,
					Param:  config.NewParam("include_tables", "Specify list of tables to collect", false),
				},
				config.StringParam{
					Value: "/tmp/dump",
					Param: config.NewParam("directory", "Specify a directory", false),
				},
			},
		}
		directory, err := config.ReadStringValue(conf, "directory")
		require.Nil(t, err)
		require.Equal(t, "/tmp/dump", directory)
	})

}

func TestReadIntValue(t *testing.T) {
	t.Run("can find an integer param", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.IntParam{
					// nil means include everything
					Value: 10000,
					Param: config.NewParam("row_limit", "Max Rows to collect", false),
				},
				config.StringListParam{
					// nil means include everything
					Values: nil,
					Param:  config.NewParam("include_tables", "Specify list of tables to collect", false),
				},
				config.StringParam{
					Value: "/tmp/dump",
					Param: config.NewParam("directory", "Specify a directory", false),
				},
			},
		}
		rowLimit, err := config.ReadIntValue(conf, "row_limit")
		require.Nil(t, err)
		require.Equal(t, int64(10000), rowLimit)
	})

}

func TestReadBoolValue(t *testing.T) {
	t.Run("can find a boolean param", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.BoolParam{
					// nil means include everything
					Value: true,
					Param: config.NewParam("compress", "Compress data", false),
				},
				config.StringListParam{
					// nil means include everything
					Values: nil,
					Param:  config.NewParam("include_tables", "Specify list of tables to collect", false),
				},
				config.StringParam{
					Value: "/tmp/dump",
					Param: config.NewParam("directory", "Specify a directory", false),
				},
			},
		}

		compress, err := config.ReadBoolValue(conf, "compress")
		require.Nil(t, err)
		require.True(t, compress)
	})
}

func TestReadStringOptionsValue(t *testing.T) {
	t.Run("can find a string value in a list of options", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringOptions{
					Param:      config.NewParam("format", "List of formats", false),
					Options:    []string{"csv", "tsv", "binary", "json", "ndjson"},
					Value:      "csv",
					AllowEmpty: false,
				},
			},
		}
		format, err := config.ReadStringOptionsValue(conf, "format")
		require.Nil(t, err)
		require.Equal(t, "csv", format)
	})

	t.Run("errors on invalid value", func(t *testing.T) {
		conf := config.Configuration{
			Params: []config.ConfigParam{
				config.StringOptions{
					Param:      config.NewParam("format", "List of formats", false),
					Options:    []string{"csv", "tsv", "binary", "json", "ndjson"},
					Value:      "random",
					AllowEmpty: false,
				},
			},
		}
		format, err := config.ReadStringOptionsValue(conf, "format")
		require.Equal(t, "random is not a valid option in [csv tsv binary json ndjson] for the the parameter format", err.Error())
		require.Equal(t, "", format)
	})
}
