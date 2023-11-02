package services

import (
	"datastream/config"
	"datastream/logs"
	"testing"
)

func TestNewKafkaProducers(t *testing.T) {
	dataconfig := &config.KafkaConfig{
		Broker: "localhost:9092",
	}
	producer1, err := NewKafkaProducers(dataconfig)
	if err != nil {
		t.Errorf("Expected no error, but got an error: %v", err)
	}
	if producer1 == nil {
		t.Errorf("Expected non-nil producers, but got nil")
	}

	invalidConfig := &config.KafkaConfig{
		Broker:        "invalid-broker",
		ContactsTopic: "",
		ActivityTopic: "",
	}
	_, err = NewKafkaProducers(invalidConfig)
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

func TestSendMessage_Successful(t *testing.T) {
	kh, _ := NewKafkaHandler()
	topic := "newtopic"
	message := "Success, Kafka!"
	err1 := kh.SendMessage(topic, message)
	if err1 != nil {
		t.Errorf("Expected no error, but got: %v", err1)
	}
}

func TestSendMessage_FailedTopic(t *testing.T) {
	kh, _ := NewKafkaHandler()
	topic := "/invalidtopic/"
	message := "This should fail"
	err1 := kh.SendMessage(topic, message)
	if err1 == nil {
		t.Error("Expected an error, but got nil")
	}
}

func BenchmarkSendMessage(b *testing.B) {
	kh, _ := NewKafkaHandler()
	topic := "test_topic"
	message := "example_message"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := kh.SendMessage(topic, message)
		if err != nil {
			b.Fatalf("SendMessage failed: %v", err)
		}
	}
}
func BenchmarkConsumeMessage(b *testing.B) {

	kafkaHandler, _ := NewKafkaHandler()

	for i := 0; i < b.N; i++ {
		kafkaHandler.ConsumeMessage("contacts15")
	}
}
func TestConsumeMessage(t *testing.T) {
	logs.InsForLogging()
	kafkahandler, _ := NewKafkaHandler()
	testMessage := "Test message"

	topic := "test-topic"
	kafkahandler.SendMessage(topic, testMessage)
	actualMessage, err := kafkahandler.ConsumeMessage(topic)

	if err != nil {
		t.Errorf("Expected no error, but got an error: %v", err)
	}
	if actualMessage != testMessage {
		t.Errorf("Expected '%s', but got: %s", testMessage, actualMessage)
	}
}
