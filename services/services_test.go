package services

import (
	"database/sql"
	"datastream/config"
	"datastream/logs"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestGetQueryResultFromClickhouse(t *testing.T) {
	results, err := GetQueryResultFromClickhouse()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Expected no error, but got an error: %v", err))
	}
	expectedCountries := []string{"USA", "UK"}
	if len(results) != len(expectedCountries) {
		logs.NewLog.Errorf(fmt.Sprintf("Expected %d results, but got %d", len(expectedCountries), len(results)))
	}
	for i, expectedCountry := range expectedCountries {
		if results[i].Country != expectedCountry {
			logs.NewLog.Error(fmt.Sprintf("Result at index %d: Expected country '%s', but got '%s'", i, expectedCountry, results[i].Country))
		}
	}
}
func TestSendMessageAndConsumeMessage(t *testing.T) {
	testConfig := &config.KafkaConfig{
		Broker: "localhost:9092",
	}
	topic := "test_topic"
	consumer, err := NewKafkaConsumer(testConfig, topic)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error creating Kafka consumer: %v", err))
	}
	producer1, _, err := NewKafkaProducers(testConfig)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error creating Kafka producer: %v", err))
	}
	messageToSend := "FileTest"
	err = SendMessage(producer1, topic, messageToSend)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error sending message: %v", err))
	}
	receivedMessages := ConsumeMessage(consumer, topic)
	expectedMessage := messageToSend

	if len(receivedMessages) != 1 {
		logs.NewLog.Error(fmt.Sprintf("Expected 1 message, but got %d messages", len(receivedMessages)))
	}
	if receivedMessages[0] != expectedMessage {
		logs.NewLog.Error(fmt.Sprintf("Expected message '%s', but got '%s'", expectedMessage, receivedMessages[0]))
	}
	consumer.Close()
	producer1.Close()
}
func TestConnectMysql(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}
func TestInsertContactDataToMySQL(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	createSchema(db)
	testMessages := []string{
		"(1, 'John', 'john@example.com', 'Details1', 1),",
		"(2, 'Jane', 'jane@example.com', 'Details2', 0),",
	}

	err = InsertContactDataToMySQL(testMessages)
	if err != nil {
		t.Fatal(err)
	}

	verifyData(t, db)
}

func createSchema(db *sql.DB) {
	schemaSQL := `
        CREATE TABLE Contacts (
            ID INT PRIMARY KEY,
            Name TEXT,
            Email TEXT,
            Details TEXT,
            Status INT
        );
    `
	_, err := db.Exec(schemaSQL)
	if err != nil {
		panic(err)
	}
}

func verifyData(t *testing.T, db *sql.DB) {
	rows, err := db.Query("SELECT * FROM Contacts")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

}
