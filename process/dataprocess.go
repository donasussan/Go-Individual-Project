package process

import (
	cryptoRand "crypto/rand"
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"datastream/services"
	"datastream/types"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
)

func generateRandomID() (string, error) {
	uuidObj, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	randomBytes := make([]byte, 8)
	_, err = io.ReadFull(cryptoRand.Reader, randomBytes)
	if err != nil {
		return "", err
	}
	randomString := fmt.Sprintf("%s-%x", uuidObj, randomBytes)
	randomString = strings.ReplaceAll(randomString, "-", "")
	return randomString, nil
}
func CSVReadToContactsStruct(filename string) ([]types.Contacts, error) {
	file, err := os.Open(filename)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("failed to open file: %v", err))
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	contactsStruct := make([]types.Contacts, 0)

	for lineNumber := 1; ; lineNumber++ {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			logs.NewLog.Error(fmt.Sprintf("failed to read CSV: %v", err))
			return nil, err
		}
		if len(record) != 3 {
			logs.NewLog.Error(fmt.Sprintf("invalid number of columns in CSV record %d: %v", lineNumber, record))
			return nil, err
		}
		randomID, err := generateRandomID()
		if err != nil {
			logs.NewLog.Error(fmt.Sprintf("failed to generate random ID: %v", err))
			return nil, err
		}
		name := record[0]
		email := record[1]
		details := record[2]
		contact := types.Contacts{
			ID:      randomID,
			Name:    name,
			Email:   email,
			Details: details,
		}
		contactsStruct = append(contactsStruct, contact)
	}

	return contactsStruct, err
}

func SendDataToKafkaProducers(ContactsData string, ActivityData string, topics *config.KafkaConfig) error {
	configData, err := database.LoadDatabaseConfig("kafka")
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error loading Kafka database config: %v", err))
		return err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Invalid database type: expected 'kafka'")
	}
	kafkaConfig.ContactsTopic = topics.ContactsTopic
	kafkaConfig.ActivityTopic = topics.ActivityTopic
	producer1, producer2, err := services.NewKafkaProducers(&kafkaConfig)
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka producers: %v", err))
		return err
	}
	defer producer1.Close()
	defer producer2.Close()
	err1 := services.SendMessage(producer1, kafkaConfig.ContactsTopic, ContactsData)
	if err1 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic1: %v", err1))
		return err1
	}
	err2 := services.SendMessage(producer2, kafkaConfig.ActivityTopic, ActivityData)
	if err2 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic2: %v", err2))
		return err2
	}
	return nil
}
func SendConsumerContactsToMySQL(topics *config.KafkaConfig) error {
	Database := "kafka"
	configData, err := database.LoadDatabaseConfig(Database)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
		return err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Error converting database config to KafkaConfig")
	}
	consumer, err := services.NewKafkaConsumer(&kafkaConfig, topics.ContactsTopic)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	ContactMessages := services.ConsumeMessage(consumer, topics.ContactsTopic)
	if ContactMessages == nil {
		logs.NewLog.Error("No contact data received from Kafka.")
		return nil
	}
	services.InsertContactDataToMySQL(ContactMessages)
	return nil
}

func SendConsumerActivityToMySQL(topics *config.KafkaConfig) error {
	Database := "kafka"
	configData, err := database.LoadDatabaseConfig(Database)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
		return err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Error converting database config to KafkaConfig")
	}
	consumer, err := services.NewKafkaConsumer(&kafkaConfig, topics.ActivityTopic)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	ActivityMessages := services.ConsumeMessage(consumer, topics.ActivityTopic)
	if ActivityMessages == nil {
		logs.NewLog.Error("No activity data received from Kafka.")
		return nil
	}
	services.InsertActivityDataToMySQL(ActivityMessages)
	return nil
}
