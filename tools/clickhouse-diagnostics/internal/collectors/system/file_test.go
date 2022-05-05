package system_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/system"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFileConfiguration(t *testing.T) {
	t.Run("correct configuration is returned for file collector", func(t *testing.T) {
		fileCollector := system.NewFileCollector(&platform.ResourceManager{})
		conf := fileCollector.Configuration()
		require.Len(t, conf.Params, 1)
		require.IsType(t, config.StringParam{}, conf.Params[0])
		filePattern, ok := conf.Params[0].(config.StringParam)
		require.True(t, ok)
		require.True(t, filePattern.Required())
		require.Equal(t, filePattern.Name(), "file_pattern")
		require.Equal(t, "", filePattern.Value)
	})
}

func TestFileCollect(t *testing.T) {

	t.Run("test filter patterns work", func(t *testing.T) {
		fileCollector := system.NewFileCollector(&platform.ResourceManager{})
		bundle, err := fileCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      "../../../testdata/**/*.xml",
					Param:      config.NewParam("file_pattern", "Glob based pattern to specify files for collection", true),
					AllowEmpty: false,
				},
			},
		})
		require.Nil(t, err)
		require.Nil(t, bundle.Errors.Errors)
		checkFileBundle(t, bundle,
			[]string{"../../../testdata/configs/include/xml/server-include.xml",
				"../../../testdata/configs/include/xml/user-include.xml",
				"../../../testdata/configs/xml/config.xml",
				"../../../testdata/configs/xml/users.xml",
				"../../../testdata/configs/xml/users.d/default-password.xml",
				"../../../testdata/configs/yandex_xml/config.xml",
				"../../../testdata/docker/admin.xml",
				"../../../testdata/docker/custom.xml"})
	})

	t.Run("invalid file patterns are detected", func(t *testing.T) {
		fileCollector := system.NewFileCollector(&platform.ResourceManager{})
		bundle, err := fileCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      "",
					Param:      config.NewParam("file_pattern", "Glob based pattern to specify files for collection", true),
					AllowEmpty: false,
				},
			},
		})
		require.NotNil(t, err)
		require.Equal(t, "parameter file_pattern is invalid - file_pattern cannot be empty", err.Error())
		require.Equal(t, &data.DiagnosticBundle{}, bundle)
	})

	t.Run("check empty matches are reported", func(t *testing.T) {
		fileCollector := system.NewFileCollector(&platform.ResourceManager{})
		bundle, err := fileCollector.Collect(config.Configuration{
			Params: []config.ConfigParam{
				config.StringParam{
					Value:      "../../../testdata/**/*.random",
					Param:      config.NewParam("file_pattern", "Glob based pattern to specify files for collection", true),
					AllowEmpty: false,
				},
			},
		})
		require.Nil(t, err)
		require.Nil(t, bundle.Frames)
		require.Len(t, bundle.Errors.Errors, 1)
		require.Equal(t, "0 files match glob pattern", bundle.Errors.Errors[0].Error())
	})

}

func checkFileBundle(t *testing.T, bundle *data.DiagnosticBundle, expectedFiles []string) {
	require.NotNil(t, bundle)
	require.Nil(t, bundle.Errors.Errors)
	require.Len(t, bundle.Frames, 1)
	require.Contains(t, bundle.Frames, "collection")
	dirFrame, ok := bundle.Frames["collection"].(data.FileFrame)
	require.True(t, ok)
	require.Equal(t, []string{"files"}, dirFrame.Columns())
	i := 0
	for {
		values, ok, err := dirFrame.Next()
		require.Nil(t, err)
		if !ok {
			break
		}
		require.Len(t, values, 1)
		file, ok := values[0].(data.SimpleFile)
		require.True(t, ok)
		require.Contains(t, expectedFiles, file.FilePath())
		i += 1
	}
	require.Equal(t, len(expectedFiles), i)
}
