package database

import (
	"datastream/config"
	"datastream/logs"
	"fmt"
)

type DataStore interface {
	InsertContact(ContactsData string, ActivityData string) error
}

type Kafka struct {
	ConfigData config.KafkaConfig
}

func NewKafkaStore(configData config.KafkaConfig) *Kafka {
	return &Kafka{
		ConfigData: configData,
	}
}

func (kafka *Kafka) InsertContact(ContactsData string, ActivityData string) error {
	logger, _ := logs.NewSimpleLogger("datalog.log")
	configData, err := config.LoadKafkaConfigFromEnv()
	if err != nil {
		logger.Fatalf(fmt.Sprintf("Error loading Kafka config: %v", err))
	}

	producer1, producer2, err := config.NewKafkaProducers(configData)
	if err != nil {
		logger.Fatalf(fmt.Sprintf("Error creating Kafka producers: %v", err))
	}
	defer producer1.Close()
	defer producer2.Close()

	err1 := config.SendMessage(producer1, configData.Topic1, ContactsData)
	if err1 != nil {
		logger.Fatalf(fmt.Sprintf("Error sending message to Topic1: %v", err1))
	}

	err3 := config.SendMessage(producer2, configData.Topic2, ActivityData)
	if err3 != nil {
		logger.Fatalf(fmt.Sprintf("Error sending message to Topic2: %v", err3))
	}

	return nil
}
