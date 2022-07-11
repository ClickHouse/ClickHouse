package data_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNextMemoryFrame(t *testing.T) {
	t.Run("can iterate memory frame", func(t *testing.T) {
		columns := []string{"Filesystem", "Size", "Used", "Avail", "Use%", "Mounted on"}
		rows := [][]interface{}{
			{"sysfs", 0, 0, 0, 0, "/sys"},
			{"proc", 0, 0, 0, 0, "/proc"},
			{"udev", 33357840384, 0, 33357840384, 0, "/dev"},
			{"devpts", 0, 0, 0, 0, "/dev/pts"},
			{"tmpfs", 6682607616, 2228224, 6680379392, 1, "/run"},
			{"/dev/mapper/system-root", 1938213220352, 118136926208, 1721548947456, 7.000000000000001, "/"},
		}
		memoryFrame := data.NewMemoryFrame("disks", columns, rows)
		i := 0
		for {
			values, ok, err := memoryFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			require.ElementsMatch(t, values, rows[i])
			require.Len(t, values, 6)
			i += 1
		}
		require.Equal(t, 6, i)
	})

	t.Run("can iterate memory frame when empty", func(t *testing.T) {
		memoryFrame := data.NewMemoryFrame("test", []string{}, [][]interface{}{})
		i := 0
		for {
			_, ok, err := memoryFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
		}
		require.Equal(t, 0, i)
	})

	t.Run("can iterate memory frame when empty", func(t *testing.T) {
		memoryFrame := data.MemoryFrame{}
		i := 0
		for {
			_, ok, err := memoryFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
		}
		require.Equal(t, 0, i)
	})
}
