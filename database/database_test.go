package database

// import (
// 	"datastream/config"
// 	"datastream/logs"
// 	"fmt"
// 	"os"
// 	"testing"
// )

// func TestLoadDatabaseConfig(t *testing.T) {
// 	os.Setenv("DB_USERNAME", "testuser")
// 	os.Setenv("DB_PASSWORD", "testpassword")
// 	os.Setenv("DB_HOST", "localhost")
// 	os.Setenv("DB_PORT", "3306")
// 	os.Setenv("DB_NAME", "testdb")

// 	mysqlConfig, err := LoadDatabaseConfig("mysql")
// 	if err != nil {
// 		logs.NewLog.Errorf(fmt.Sprintf("Expected no error, got %v", err))
// 	}
// 	expectedMySQLConfig := config.MySQLConfig{
// 		Username: "testuser",
// 		Password: "testpassword",
// 		Hostname: "localhost",
// 		Port:     "3306",
// 		DBName:   "testdb",
// 	}
// 	if mysqlConfig != expectedMySQLConfig {
// 		logs.NewLog.Errorf(fmt.Sprintf("Expected MySQL config %v, got %v", expectedMySQLConfig, mysqlConfig))
// 	}

// 	os.Setenv("KAFKA_BROKER", "kafka:9092")
// 	kafkaConfig, err := LoadDatabaseConfig("kafka")
// 	if err != nil {
// 		logs.NewLog.Errorf(fmt.Sprintf("Expected no error, got %v", err))
// 	}
// 	expectedKafkaConfig := config.KafkaConfig{
// 		Broker: "kafka:9092",
// 	}
// 	if kafkaConfig != expectedKafkaConfig {
// 		logs.NewLog.Errorf(fmt.Sprintf("Expected Kafka config %v, got %v", expectedKafkaConfig, kafkaConfig))
// 	}

// 	os.Setenv("CLICK_USERNAME", "clickuser")
// 	os.Setenv("CLICK_PASSWORD", "clickpassword")
// 	os.Setenv("CLICK_HOST", "clickhouse")
// 	os.Setenv("CLICK_PORT", "8123")
// 	os.Setenv("CLICK_DB_NAME", "clickdb")

// 	clickHouseConfig, err := LoadDatabaseConfig("clickhouse")
// 	if err != nil {
// 		logs.NewLog.Errorf(fmt.Sprintf("Expected no error, got %v", err))
// 	}
// 	expectedClickHouseConfig := config.ClickHouseConfig{
// 		Username: "clickuser",
// 		Password: "clickpassword",
// 		Hostname: "clickhouse",
// 		Port:     "8123",
// 		DBName:   "clickdb",
// 	}
// 	if clickHouseConfig != expectedClickHouseConfig {
// 		logs.NewLog.Errorf(fmt.Sprintf("Expected ClickHouse config %v, got %v", expectedClickHouseConfig, clickHouseConfig))
// 	}

// }
