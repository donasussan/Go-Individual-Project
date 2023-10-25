package config

import (
	"testing"
)

func TestMySQLConnector(t *testing.T) {
	config := MySQLConfig{
		Username: "root",
		Password: "root",
		Hostname: "localhost",
		Port:     "3306",
		DBName:   "dona_campaign",
	}

	connector := MySQLConnector{Config: config}

	db, err := connector.Connect()
	if err != nil {
		t.Errorf("Connect() returned an error: %v", err)
	}
	defer db.Close()

	err = connector.Close()
	if err != nil {
		t.Errorf("Close() returned an error: %v", err)
	}
}

func TestClickHouseConnector(t *testing.T) {
	config := ClickHouseConfig{
		Username: "default",
		Password: "dona1502",
		Hostname: "localhost",
		Port:     "9000",
		DBName:   "dona_campaign",
	}

	connector := ClickHouseConnector{Config: config}

	db, err := connector.Connect()
	if err != nil {
		t.Errorf("Connect() returned an error: %v", err)
	}
	defer db.Close()

	err = connector.Close()
	if err != nil {
		t.Errorf("Close() returned an error: %v", err)
	}
}

func TestMySQLConfig_GetDSN(t *testing.T) {
	config := MySQLConfig{
		Username: "testuser",
		Password: "testpassword",
		Hostname: "localhost",
		Port:     "3306",
		DBName:   "testdb",
	}

	dsn := config.GetDSN()
	expectedDSN := "testuser:testpassword@tcp(localhost:3306)/testdb"
	if dsn != expectedDSN {
		t.Errorf("GetDSN() returned %s, expected %s", dsn, expectedDSN)
	}
}

func TestMySQLConfig_GetConfig(t *testing.T) {
	config := MySQLConfig{
		Username: "testuser",
		Password: "testpassword",
		Hostname: "localhost",
		Port:     "3306",
		DBName:   "testdb",
	}

	expectedConfig := map[string]string{
		"Username": "testuser",
		"Password": "testpassword",
		"Hostname": "localhost",
		"Port":     "3306",
		"DBName":   "testdb",
	}
	configMap := config.GetConfig()

	for key, expectedValue := range expectedConfig {
		if configMap[key] != expectedValue {
			t.Errorf("GetConfig()[%s] returned %s, expected %s", key, configMap[key], expectedValue)
		}
	}
}
