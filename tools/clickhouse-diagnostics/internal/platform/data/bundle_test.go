package data_test

import (
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBundleError(t *testing.T) {

	t.Run("can get a bundle error", func(t *testing.T) {
		errs := make([]error, 3)
		errs[0] = errors.New("Error 1")
		errs[1] = errors.New("Error 2")
		errs[2] = errors.New("Error 3")
		fErrors := data.FrameErrors{
			Errors: errs,
		}
		require.Equal(t, `Error 1
Error 2
Error 3`, fErrors.Error())

	})
}
