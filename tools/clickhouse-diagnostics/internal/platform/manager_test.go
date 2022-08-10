package platform_test

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
		fmt.Println("unable to read current directory", err)
		os.Exit(1)
	}
	// for now, we test against a hardcoded database-server version but we should make this a property
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", test.GetClickHouseTestVersion()),
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
		BindMounts: map[string]string{
			"/etc/clickhouse-server/config.d/custom.xml": path.Join(cwd, "../../testdata/docker/custom.xml"),
			"/etc/clickhouse-server/users.d/admin.xml":   path.Join(cwd, "../../testdata/docker/admin.xml"),
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

func TestConnect(t *testing.T) {
	mappedPort, err := strconv.Atoi(os.Getenv("CLICKHOUSE_DB_PORT"))
	if err != nil {
		t.Fatal("Unable to read port value from environment")
	}
	t.Run("can only connect once", func(t *testing.T) {
		// get before connection
		manager := platform.GetResourceManager()
		require.Nil(t, manager.DbClient)
		// init connection
		err = manager.Connect("localhost", uint16(mappedPort), "", "")
		require.Nil(t, err)
		require.NotNil(t, manager.DbClient)
		// try and re-fetch connection
		err = manager.Connect("localhost", uint16(mappedPort), "", "")
		require.NotNil(t, err)
		require.Equal(t, "connect can only be called once", err.Error())
	})

}

func TestGetResourceManager(t *testing.T) {
	t.Run("get resource manager", func(t *testing.T) {
		manager := platform.GetResourceManager()
		require.NotNil(t, manager)
		manager2 := platform.GetResourceManager()
		require.NotNil(t, manager2)
		require.Equal(t, &manager, &manager2)
	})

}
