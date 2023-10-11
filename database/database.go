package database

import (
	"datastream/config"
	"datastream/logs"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
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

	_, _, err := producer.SendMessage(producerMessage)
	return err
}

func NewKafkaConsumer(config *config.KafkaConfig, topic string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer([]string{config.Broker}, nil)
	if err != nil {
		return nil, err
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

func LoadDatabaseConfig(Database string) (config.DatabaseConfig, error) {
	if err := godotenv.Load(); err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error loading .env file: %v", err))
		return nil, err
	}

	switch Database {
	case "mysql":
		mysqlConfig := config.MySQLConfig{
			Username: os.Getenv("DB_USERNAME"),
			Password: os.Getenv("DB_PASSWORD"),
			Hostname: os.Getenv("DB_HOST"),
			Port:     os.Getenv("DB_PORT"),
			DBName:   os.Getenv("DB_NAME"),
		}
		return mysqlConfig, nil
	case "kafka":
		kafkaConfig := config.KafkaConfig{
			Broker: os.Getenv("KAFKA_BROKER"),
			Topic1: os.Getenv("KAFKA_TOPIC_CONTACTS"),
			Topic2: os.Getenv("KAFKA_TOPIC_CONTACT_ACTIVITY"),
		}
		return kafkaConfig, nil
	case "clickhouse":
		clickHouseConfig := config.ClickHouseConfig{
			Username: os.Getenv("CLICK_USERNAME"),
			Password: os.Getenv("CLICK_PASSWORD"),
			Hostname: os.Getenv("CLICK_HOST"),
			Port:     os.Getenv("CLICK_PORT"),
			DBName:   os.Getenv("CLICK_DB_NAME"),
		}
		return clickHouseConfig, nil
	default:
		return nil, fmt.Errorf("unsupported DB_TYPE: %s", Database)

	}

}
