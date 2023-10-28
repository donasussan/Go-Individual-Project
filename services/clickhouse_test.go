package services

import (
	"datastream/database"
	"datastream/logs"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestConnectClickhouse(t *testing.T) {
	logs.InsForLogging()
	database.LoadDatabaseConfig("clickhouse")
	connector, err := ConnectClickhouse()
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if connector == nil {
		t.Error("Expected non-nil ClickHouseConnector, but got nil")
	}
}

func TestGetOutputFromClickHouse(t *testing.T) {
	logs.InsForLogging()

	tests := []struct {
		name             string
		query            string
		expectedColumns  int
		shouldCheckError bool
	}{
		{
			name:             "ValidQuery",
			query:            `SELECT * FROM ContactActivity;`,
			expectedColumns:  9,
			shouldCheckError: true,
		},
		{
			name:             "ErrorQuery",
			query:            "error_query",
			expectedColumns:  0,
			shouldCheckError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows, err := GetQueryResultFromClickhouse(test.query)

			if test.shouldCheckError {
				if err != nil {

				} else if test.expectedColumns == 0 {
					t.Errorf("Expected an error, but there was none.")
				}
			} else {

				for rows.Next() {
					columns, err := rows.Columns()
					if err != nil {
						t.Errorf("Failed to retrieve column information: %v", err)
						continue
					}

					if len(columns) != test.expectedColumns {
						t.Errorf("Expected %d columns, but got %d columns", test.expectedColumns, len(columns))
					}
				}
			}
		})
	}
}
