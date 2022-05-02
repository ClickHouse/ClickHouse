package clickhouse_test

import (
	"encoding/xml"
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/clickhouse"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path"
	"testing"
)

func TestConfigConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for config collector", func(t *testing.T) {
		configCollector := clickhouse.NewConfigCollector(&platform.ResourceManager{})
		conf := configCollector.Configuration()
		require.Len(t, conf.Params, 1)
		// check first param
		require.IsType(t, config.StringParam{}, conf.Params[0])
		directory, ok := conf.Params[0].(config.StringParam)
		require.True(t, ok)
		require.False(t, directory.Required())
		require.Equal(t, directory.Name(), "directory")
		require.Equal(t, "", directory.Value)
	})
}

func TestConfigCollect(t *testing.T) {
	configCollector := clickhouse.NewConfigCollector(&platform.ResourceManager{})

	t.Run("test default file collector configuration", func(t *testing.T) {
		diagSet, err := configCollector.Collect(config.Configuration{})
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		// we won't be able to collect the default configs preprocessed and default - even if clickhouse is installed
		// these directories should not be readable under any permissions these tests are unrealistically executed!
		// note: we may also pick up configs from a local clickhouse process - we thus allow a len >=2 but don't check this
		// as its non-deterministic
		require.GreaterOrEqual(t, len(diagSet.Frames), 2)
		// check default key
		require.Contains(t, diagSet.Frames, "default")
		require.Equal(t, diagSet.Frames["default"].Name(), "/etc/clickhouse-server/")
		require.Equal(t, diagSet.Frames["default"].Columns(), []string{"config"})
		// collection will have failed
		checkFrame(t, diagSet.Frames["default"], nil)
		// check preprocessed key
		require.Contains(t, diagSet.Frames, "preprocessed")
		require.Equal(t, diagSet.Frames["preprocessed"].Name(), "/var/lib/clickhouse/preprocessed_configs")
		require.Equal(t, diagSet.Frames["preprocessed"].Columns(), []string{"config"})
		// min of 2 - might be more if a local installation of clickhouse is running
		require.GreaterOrEqual(t, len(diagSet.Errors.Errors), 2)
	})

	t.Run("test configuration when specified", func(t *testing.T) {
		// create some test files
		tempDir := t.TempDir()
		confDir := path.Join(tempDir, "conf")
		// create an includes file
		includesDir := path.Join(tempDir, "includes")
		err := os.MkdirAll(includesDir, os.ModePerm)
		require.Nil(t, err)
		includesPath := path.Join(includesDir, "random.xml")
		includesFile, err := os.Create(includesPath)
		require.Nil(t, err)
		xmlWriter := io.Writer(includesFile)
		enc := xml.NewEncoder(xmlWriter)
		enc.Indent("  ", "    ")
		xmlConfig := data.XmlConfig{
			XMLName: xml.Name{},
			Clickhouse: data.XmlLoggerConfig{
				XMLName:  xml.Name{},
				ErrorLog: "/var/log/clickhouse-server/clickhouse-server.err.log",
				Log:      "/var/log/clickhouse-server/clickhouse-server.log",
			},
			IncludeFrom: "",
		}
		err = enc.Encode(xmlConfig)
		require.Nil(t, err)
		// create 5 temporary config files - length is 6 for the included file
		rows := make([][]interface{}, 6)
		for i := 0; i < 5; i++ {
			if i == 4 {
				// set the includes for the last doc
				xmlConfig.IncludeFrom = includesPath
			}
			// we want to check hierarchies are walked so create a simple folder for each file
			fileDir := path.Join(confDir, fmt.Sprintf("%d", i))
			err := os.MkdirAll(fileDir, os.ModePerm)
			require.Nil(t, err)
			filepath := path.Join(fileDir, fmt.Sprintf("random-%d.xml", i))
			row := make([]interface{}, 1)
			row[0] = data.XmlConfigFile{Path: filepath}
			rows[i] = row
			xmlFile, err := os.Create(filepath)
			require.Nil(t, err)
			// write a little xml so its valid
			xmlConfig := xmlConfig
			xmlWriter := io.Writer(xmlFile)
			enc := xml.NewEncoder(xmlWriter)
			enc.Indent("  ", "    ")
			err = enc.Encode(xmlConfig)
			require.Nil(t, err)
		}
		diagSet, err := configCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value: confDir,
					Param: config.NewParam("directory", "File locations", false),
				},
			},
		})
		require.Nil(t, err)
		require.NotNil(t, diagSet)
		require.Len(t, diagSet.Frames, 1)
		require.Contains(t, diagSet.Frames, "user_specified")
		require.Equal(t, diagSet.Frames["user_specified"].Name(), confDir)
		require.Equal(t, diagSet.Frames["user_specified"].Columns(), []string{"config"})
		iConf := make([]interface{}, 1)
		iConf[0] = data.XmlConfigFile{Path: includesPath, Included: true}
		rows[5] = iConf
		checkFrame(t, diagSet.Frames["user_specified"], rows)
	})
}
