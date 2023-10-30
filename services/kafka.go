package services

import (
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
)

type KafkaHandler struct {
	producer1 sarama.SyncProducer
	consumer  sarama.Consumer
	Config    config.KafkaConfig
	offset    int64
}

func NewKafkaProducers(config *config.KafkaConfig) (sarama.SyncProducer, error) {
	producer1, err := sarama.NewSyncProducer([]string{config.Broker}, nil)
	if err != nil {
		return nil, err
	}

	return producer1, nil
}
func NewKafkaConsumer(config *config.KafkaConfig, topic string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer([]string{config.Broker}, nil)
	if err != nil {
		logs.NewLog.Error("Error creating new consumer")
		return nil, err
	}
	return consumer, err
}
func NewKafkaHandler() (*KafkaHandler, error) {
	configData, err := database.LoadDatabaseConfig("kafka")
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading Kafka database config: %v", err))
		return nil, err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Invalid database type: expected 'kafka'")
		return nil, errors.New("invalid database type: expected 'kafka'")
	}

	producer1, err := NewKafkaProducers(&kafkaConfig)
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka producers: %v", err))
		return nil, err
	}

	consumer, err := NewKafkaConsumer(&kafkaConfig, kafkaConfig.ContactsTopic)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return nil, err
	}

	return &KafkaHandler{
		producer1: producer1,
		consumer:  consumer,
		Config:    kafkaConfig,
		offset:    sarama.OffsetNewest,
	}, nil
}

func (kafkahandler *KafkaHandler) SendMessage(topic string, message string) error {
	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	_, _, err := kafkahandler.producer1.SendMessage(producerMessage)
	if err != nil {
		return err
	}
	return nil
}
func (kafkahandler *KafkaHandler) ConsumeMessage(topic string) (string, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false

	partitionConsumer, err := kafkahandler.consumer.ConsumePartition(topic, 0, kafkahandler.offset)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint("Error creating partition consumer: ", err))
		return "", err
	}
	defer partitionConsumer.Close()

	for msg := range partitionConsumer.Messages() {
		message := string(msg.Value)
		kafkahandler.offset = msg.Offset + 1
		return message, nil
	}

	return "", errors.New("0 messages received")
}

func (kafkahandler *KafkaHandler) Close() {
	kafkahandler.producer1.Close()
	kafkahandler.consumer.Close()
}
