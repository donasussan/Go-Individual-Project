package services

import (
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"errors"
	"fmt"
	"strings"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
)

func NewKafkaProducers(config *config.KafkaConfig) (sarama.SyncProducer, sarama.SyncProducer, error) {
	producer1, err := sarama.NewSyncProducer([]string{config.Broker}, nil)
	if err != nil {
		return nil, nil, err
	}
	producer2, err := sarama.NewSyncProducer([]string{config.Broker}, nil)
	if err != nil {
		return nil, nil, err
	}
	return producer1, producer2, nil
}
func NewKafkaConsumer(config *config.KafkaConfig, topic string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer([]string{config.Broker}, nil)
	if err != nil {
		logs.NewLog.Error("Error creating new consumer")
		return nil, err
	}
	return consumer, err
}
func KafkaConfigAndCreateConsumer() (sarama.Consumer, config.KafkaConfig, error) {
	configData, err := database.LoadDatabaseConfig("kafka")
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
		return nil, config.KafkaConfig{}, err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Error converting database config to *KafkaConfig")
		return nil, config.KafkaConfig{}, errors.New("error converting database config to *KafkaConfig")
	}
	consumer, err := NewKafkaConsumer(&kafkaConfig, kafkaConfig.ContactsTopic)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return nil, config.KafkaConfig{}, err
	}
	return consumer, kafkaConfig, nil
}

func KafkaConfigAndCreateProducers() (sarama.SyncProducer, sarama.SyncProducer, config.KafkaConfig, error) {
	configData, err := database.LoadDatabaseConfig("kafka")
	fmt.Println(configData)
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error loading Kafka database config: %v", err))
		return nil, nil, config.KafkaConfig{}, err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Invalid database type: expected 'kafka'")
		return nil, nil, config.KafkaConfig{}, errors.New("Invalid database type: expected 'kafka'")
	}
	producer1, producer2, err := NewKafkaProducers(&kafkaConfig)
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka producers: %v", err))
		return nil, nil, kafkaConfig, err
	}
	return producer1, producer2, kafkaConfig, nil
}
func SendMessage(producer sarama.SyncProducer, topic string, message string) error {
	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	_, _, err := producer.SendMessage(producerMessage)
	return err
}

func ConsumeMessage(consumer sarama.Consumer, topic string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		logs.NewLog.Error("Error creating partition consumer")
		return
	}
	defer partitionConsumer.Close()
	var messages []string
	messageCount := 0

	for msg := range partitionConsumer.Messages() {
		message := string(msg.Value)
		messages = append(messages, message)
		messageCount++

		if messageCount%100 == 0 {
			if strings.Contains(topic, "contacts") {
				err := InsertContactDataToMySQL(messages, "Contacts")
				messages = make([]string, 0)
				if err != nil {
					logs.NewLog.Errorf(fmt.Sprintf("Error inserting contact data into MySQL: %v", err))
				}
			} else {
				err := InsertActivityDataToMySQL(messages, "ContactActivity")
				messages = make([]string, 0)
				if err != nil {
					logs.NewLog.Errorf(fmt.Sprintf("Error inserting activity data into MySQL: %v", err))
				}
			}
		}
	}
}
