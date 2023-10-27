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

type KafkaHandler struct {
	producer1   sarama.SyncProducer
	consumer    sarama.Consumer
	Config      config.KafkaConfig
	dbConnector config.MySQLConnect
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
		return nil, errors.New("Invalid database type: expected 'kafka'")
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
		producer1:   producer1,
		consumer:    consumer,
		Config:      kafkaConfig,
		dbConnector: &MySQLDBConnector{},
	}, nil
}

func (kh *KafkaHandler) SendMessage(topic string, message string) error {
	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	_, _, err := kh.producer1.SendMessage(producerMessage)
	if err != nil {
		return err
	}
	return nil
}

func (kh *KafkaHandler) ConsumeMessage(topic string) {
	partitionConsumer, err := kh.consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
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
			db, err := kh.dbConnector.EstablishMySQLConnection()
			if err != nil {
				logs.NewLog.Error(fmt.Sprintf("Error establishing MySQL connection %v", err))
			}
			if strings.Contains(topic, "contacts") {
				query := "INSERT INTO Contacts(ID, Name, Email, Details, Status) VALUES"
				err := InsertDataToMySQL(messages, query, db)
				fmt.Println("hi")

				messages = make([]string, 0)
				if err != nil {
					logs.NewLog.Errorf(fmt.Sprintf("Error inserting contact data into MySQL: %v", err))
				}
			} else {
				query := "INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType,ActivityDate)VALUES"
				err := InsertDataToMySQL(messages, query, db)
				messages = make([]string, 0)
				if err != nil {
					logs.NewLog.Errorf(fmt.Sprintf("Error inserting activity data into MySQL: %v", err))
				}
			}
		}
	}
}

func (kh *KafkaHandler) Close() {
	kh.producer1.Close()
	kh.consumer.Close()
}
