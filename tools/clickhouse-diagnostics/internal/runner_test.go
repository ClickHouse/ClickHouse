package internal_test

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/collectors"
	_ "github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/clickhouse"
	_ "github.com/ClickHouse/clickhouse-diagnostics/internal/collectors/system"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/outputs"
	_ "github.com/ClickHouse/clickhouse-diagnostics/internal/outputs/file"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/config"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/test"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
)

func TestMain(m *testing.M) {
	// create a ClickHouse container
	ctx := context.Background()
	cwd, err := os.Getwd()

	if err != nil {
		// can't test without container
		panic(err)
	}
	// for now, we test against a hardcoded database-server version but we should make this a property
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", test.GetClickHouseTestVersion()),
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
		BindMounts: map[string]string{
			"/etc/clickhouse-server/config.d/custom.xml": path.Join(cwd, "../testdata/docker/custom.xml"),
			"/etc/clickhouse-server/users.d/admin.xml":   path.Join(cwd, "../testdata/docker/admin.xml"),
		},
	}
	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		// can't test without container
		panic(err)
	}

	p, _ := clickhouseContainer.MappedPort(ctx, "9000")

	os.Setenv("CLICKHOUSE_DB_PORT", p.Port())
	defer clickhouseContainer.Terminate(ctx) //nolint
	os.Exit(m.Run())
}

// Execute a full default capture, with simple output, and check if a bundle is produced and it's not empty
func TestCapture(t *testing.T) {
	tmrDir := t.TempDir()
	port, err := strconv.ParseUint(os.Getenv("CLICKHOUSE_DB_PORT"), 10, 16)
	if err != nil {
		t.Fatal("Unable to read port value from environment")
	}
	// test a simple output exists
	_, err = outputs.GetOutputByName("simple")
	require.Nil(t, err)
	// this relies on the simple out not changing its params - test will likely fail if so
	outputConfig := config.Configuration{
		Params: []config.ConfigParam{
			config.StringParam{
				Value: tmrDir,
				Param: config.NewParam("directory", "Directory in which to create dump. Defaults to the current directory.", false),
			},
			config.StringOptions{
				Value:   "csv",
				Options: []string{"csv"},
				Param:   config.NewParam("format", "Format of exported files", false),
			},
			config.BoolParam{
				Value: true,
				Param: config.NewParam("skip_archive", "Don't compress output to an archive", false),
			},
		},
	}
	// test default collectors
	collectorNames := collectors.GetCollectorNames(true)
	// grab all configs - only default will be used because of collectorNames
	collectorConfigs, err := collectors.BuildConfigurationOptions()
	require.Nil(t, err)
	conf := internal.NewRunConfiguration("random", "localhost", uint16(port), "", "", "simple", outputConfig, collectorNames, collectorConfigs)
	internal.Capture(conf)
	outputDir := path.Join(tmrDir, "random")
	_, err = os.Stat(outputDir)
	require.Nil(t, err)
	require.True(t, !os.IsNotExist(err))
	files, err := ioutil.ReadDir(outputDir)
	require.Nil(t, err)
	require.Len(t, files, 1)
	outputDir = path.Join(outputDir, files[0].Name())
	// check we have a folder per collector i.e. collectorNames + diag_trace
	files, err = ioutil.ReadDir(outputDir)
	require.Nil(t, err)
	require.Len(t, files, len(collectorNames)+1)
	expectedFolders := append(collectorNames, "diag_trace")
	for _, file := range files {
		require.True(t, file.IsDir())
		utils.Contains(expectedFolders, file.Name())
	}
	// we don't test the specific collector outputs but make sure something was written to system
	systemFolder := path.Join(outputDir, "system")
	files, err = ioutil.ReadDir(systemFolder)
	require.Nil(t, err)
	require.Greater(t, len(files), 0)
	// test diag_trace
	diagFolder := path.Join(outputDir, "diag_trace")
	files, err = ioutil.ReadDir(diagFolder)
	require.Nil(t, err)
	require.Equal(t, 1, len(files))
	require.FileExists(t, path.Join(diagFolder, "errors.csv"))
}
