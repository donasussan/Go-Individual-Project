package services

import (
	"database/sql"
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func TestNewKafkaProducers(t *testing.T) {
	dataconfig := &config.KafkaConfig{
		Broker: "localhost:9092",
	}
	producer1, producer2, err := NewKafkaProducers(dataconfig)
	if err != nil {
		t.Errorf("Expected no error, but got an error: %v", err)
	}
	if producer1 == nil || producer2 == nil {
		t.Errorf("Expected non-nil producers, but got nil")
	}

	invalidConfig := &config.KafkaConfig{
		Broker:        "invalid-broker",
		ContactsTopic: "",
		ActivityTopic: "",
	}
	_, _, err = NewKafkaProducers(invalidConfig)
	if err == nil {
		t.Error("Expected an error, but got no error")
	}
}

func TestNewKafkaConsumer(t *testing.T) {
	logs.InsForLogging()

	validconfig := &config.KafkaConfig{
		Broker:        "localhost:9092",
		ContactsTopic: "",
		ActivityTopic: "",
	}
	consumer, err := NewKafkaConsumer(validconfig, "test-topic")
	if err != nil {
		t.Errorf("Expected no error, but got an error: %v", err)
	}
	if consumer == nil {
		t.Errorf("Expected a non-nil consumer, but got nil")
	}

	invalidConfig := &config.KafkaConfig{
		Broker:        "invalid-broker",
		ContactsTopic: "",
		ActivityTopic: "",
	}
	_, err = NewKafkaConsumer(invalidConfig, "test-topic")
	if err == nil {
		t.Error("Expected an error, but got no error")
	}
}

type DatabaseConnector interface {
	ConnectDatabase(string) (*sql.DB, error)
}

type MockDatabaseConnector struct {
	mock.Mock
}

func (m *MockDatabaseConnector) ConnectDatabase(databaseName string) (*sql.DB, error) {
	args := m.Called(databaseName)
	return args.Get(0).(*sql.DB), args.Error(1)
}
func TestGetOutputFromClickHouse(t *testing.T) {
	logs.InsForLogging()

	tests := []struct {
		name   string
		setup  func(*sql.DB)
		query  string
		assert func(*testing.T, *sql.Rows, error)
	}{
		{
			name: "Success",
			setup: func(db *sql.DB) {
				createTableQuery := `
                    CREATE TABLE IF NOT EXISTS mock_test_data
                    (
                        ContactsID String,
                        ActivityType UInt8
                    )
                `
				createViewQuery := `
                    CREATE VIEW IF NOT EXISTS ContactActivityTEST AS
                    SELECT
                        CAST(ContactsID AS String) AS ContactsID,
                        sum(CASE WHEN ActivityType = 1 THEN 1 ELSE 0 END) AS sent,
                        sum(CASE WHEN ActivityType = 3 THEN 1 ELSE 0 END) AS opened,
                        sum(CASE WHEN ActivityType = 4 THEN 1 ELSE 0 END) AS clicked,
                        sum(CASE WHEN ActivityType = 5 THEN 1 ELSE 0 END) AS abusive,
                        sum(CASE WHEN ActivityType = 6 THEN 1 ELSE 0 END) AS unsubscribe,
                        sum(CASE WHEN ActivityType = 7 THEN 1 ELSE 0 END) AS conversion,
                        sum(CASE WHEN ActivityType = 2 THEN 1 ELSE 0 END) AS bounce,
                        count(ActivityType) AS totalcount
                    FROM mock_test_data
                    GROUP BY ContactsID
                `
				insertQuery := `
                    INSERT INTO mock_test_data VALUES
                    ('contact1', 1),  
                    ('contact1', 3),  
                    ('contact2', 1),  
                    ('contact2', 4),  
                    ('contact2', 6),  
                    ('contact3', 1),  
                    ('contact3', 3),  
                    ('contact3', 4),  
                    ('contact3', 7); 
                `
				_, err := db.Exec(createTableQuery)
				if err != nil {
					t.Fatalf("Failed to create the table: %v", err)
				}

				_, err = db.Exec(createViewQuery)
				if err != nil {
					t.Fatalf("Failed to create the view: %v", err)
				}

				_, err = db.Exec(insertQuery)
				if err != nil {
					t.Fatalf("Failed to insert data into the table: %v", err)
				}
			},
			query: `SELECT * FROM ContactActivityTEST;`,
			assert: func(t *testing.T, rows *sql.Rows, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, rows)
			},
		},
		{
			name:  "ErrorQuery",
			setup: func(db *sql.DB) {},
			query: "error_query",
			assert: func(t *testing.T, rows *sql.Rows, err error) {
				assert.Error(t, err)
				assert.Nil(t, rows)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("Failed to create a database connection: %v", err)
			}
			defer db.Close()

			test.setup(db)

			rows, err := GetQueryResultFromClickhouse(test.query, db)
			test.assert(t, rows, err)
		})
	}
}

func TestInsertDataToSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open SQLite database: %v", err)
	}
	defer db.Close()

	createTableSQL := `CREATE TABLE ContactActivity (
		ContactsID varchar(250) NOT NULL,
		CampaignID int NOT NULL,
		ActivityType int NOT NULL,
		ActivityDate datetime NOT NULL
	);`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testCases := []struct {
		name      string
		messages  []string
		query     string
		expectErr bool
	}{
		{
			name: "Successful insertion",
			messages: []string{
				"('d1d7033fae6341ab94dcabe01ee9d2cc7f69235ee08acfec', 86, 1, '2023-09-01 00:00:00')",
			},
			query:     "INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType, ActivityDate) VALUES",
			expectErr: false,
		},
		{
			name:      "Error during query execution",
			messages:  []string{"message1", "message2"},
			query:     "INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType, ActivityDate) VALUES",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, message := range tc.messages {
				expectedQuery := tc.query + message

				if tc.expectErr {
					_, err := db.Exec(expectedQuery)
					if err == nil {
						t.Errorf("Expected an error but got none")
					}
				} else {
					_, err := db.Exec(expectedQuery)
					if err != nil {
						t.Errorf("Expected no error but got: %v", err)
					}
				}
			}
		})
	}
}
