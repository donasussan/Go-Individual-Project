package database

import (
	"datastream/config"
	"datastream/logs"

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

func SendMessage(producer sarama.SyncProducer, topic string, message string) error {
	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	logs.NewLog.Info(topic)
	_, _, err := producer.SendMessage(producerMessage)
	return err
}

func NewKafkaConsumer(config *config.KafkaConfig, topic string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer([]string{config.Broker}, nil)
	if err != nil {
		logs.NewLog.Error("Error creating new consumer")
		return nil, nil
	}
	return consumer, nil
}
func ConsumeMessage(consumer sarama.Consumer, topic string) []string {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		logs.NewLog.Error("Error creating partition consumer")
		return nil
	}
	defer partitionConsumer.Close()
	var messages []string

	for msg := range partitionConsumer.Messages() {
		message := string(msg.Value)
		messages = append(messages, message)

		if message == "EOF" {
			break
		}
	}
	return messages
}
