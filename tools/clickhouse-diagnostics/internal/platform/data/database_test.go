package data_test

import (
	"database/sql"
	"github.com/ClickHouse/clickhouse-diagnostics/internal/platform/data"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestString(t *testing.T) {
	t.Run("can order by asc", func(t *testing.T) {
		orderBy := data.OrderBy{
			Column: "created_at",
			Order:  data.Asc,
		}
		require.Equal(t, " ORDER BY created_at ASC", orderBy.String())
	})

	t.Run("can order by desc", func(t *testing.T) {
		orderBy := data.OrderBy{
			Column: "created_at",
			Order:  data.Desc,
		}
		require.Equal(t, " ORDER BY created_at DESC", orderBy.String())
	})

}

func TestNextDatabaseFrame(t *testing.T) {

	t.Run("can iterate sql rows", func(t *testing.T) {
		rowValues := [][]interface{}{
			{int64(1), "post_1", "hello"},
			{int64(2), "post_2", "world"},
			{int64(3), "post_3", "goodbye"},
			{int64(4), "post_4", "world"},
		}
		mockRows := sqlmock.NewRows([]string{"id", "title", "body"})
		for i := range rowValues {
			mockRows.AddRow(rowValues[i][0], rowValues[i][1], rowValues[i][2])
		}
		rows := mockRowsToSqlRows(mockRows)
		dbFrame, err := data.NewDatabaseFrame("test", rows)
		require.ElementsMatch(t, dbFrame.Columns(), []string{"id", "title", "body"})
		require.Nil(t, err)
		i := 0
		for {
			values, ok, err := dbFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			require.Len(t, values, 3)
			require.ElementsMatch(t, values, rowValues[i])
			i++
		}
		require.Equal(t, 4, i)
	})

	t.Run("can iterate empty sql rows", func(t *testing.T) {
		mockRows := sqlmock.NewRows([]string{"id", "title", "body"})
		rows := mockRowsToSqlRows(mockRows)
		dbFrame, err := data.NewDatabaseFrame("test", rows)
		require.ElementsMatch(t, dbFrame.Columns(), []string{"id", "title", "body"})
		require.Nil(t, err)
		i := 0
		for {
			_, ok, err := dbFrame.Next()
			require.Nil(t, err)
			if !ok {
				break
			}
			i++
		}
		require.Equal(t, 0, i)
	})
}

func mockRowsToSqlRows(mockRows *sqlmock.Rows) *sql.Rows {
	db, mock, _ := sqlmock.New()
	mock.ExpectQuery("select").WillReturnRows(mockRows)
	rows, _ := db.Query("select")
	return rows
}
