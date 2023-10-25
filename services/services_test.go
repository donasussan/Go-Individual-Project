package services

import (
	"datastream/config"
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
