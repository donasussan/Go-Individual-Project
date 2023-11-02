package services

import (
	"datastream/logs"
	"testing"
)

func TestInsertDataIntoTable_ValidDataWithConversion(t *testing.T) {
	logs.InsForLogging()
	db, err := EstablishMySQLConnection()
	if err != nil {
		t.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	values := []interface{}{"abcd", 2, 1, "2023-11-01"}

	keyValuePairs := make(map[string]interface{})
	keyValuePairs["ContactsID"] = values[0]
	keyValuePairs["CampaignID"] = values[1]
	keyValuePairs["ActivityType"] = values[2]
	keyValuePairs["ActivityDate"] = values[3]

	tableName := "ContactActivity"

	err = InsertDataIntoTable(db, tableName, keyValuePairs)

	if err != nil {
		t.Errorf("Expected no error, but got an error: %v", err)
	}
}
