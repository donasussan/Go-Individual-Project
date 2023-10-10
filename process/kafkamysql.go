package process

import (
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"fmt"
	"strings"
)

func SendDataToKafkaProducers(ContactsData string, ActivityData string) error {
	configData, err := config.LoadDatabaseConfig("kafka")
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error loading Kafka database config: %v", err))
		return err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Invalid database type: expected 'kafka'")
	}
	producer1, producer2, err := database.NewKafkaProducers(&kafkaConfig)
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka producers: %v", err))
		return err
	}
	defer producer1.Close()
	defer producer2.Close()
	err1 := database.SendMessage(producer1, kafkaConfig.Topic1, ContactsData)
	if err1 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic1: %v", err1))
		return err1
	}
	err2 := database.SendMessage(producer2, kafkaConfig.Topic2, ActivityData)
	if err2 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic2: %v", err2))
		return err2
	}
	return nil
}

func SendConsumerContactsToMySQL() error {
	Database := "kafka"
	configData, err := config.LoadDatabaseConfig(Database)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
		return err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Error converting database config to KafkaConfig")
		return fmt.Errorf("error converting database config to KafkaConfig")
	}
	consumer, err := database.NewKafkaConsumer(&kafkaConfig, kafkaConfig.Topic1)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	ContactMessages := database.ConsumeMessage(consumer, kafkaConfig.Topic1)
	if ContactMessages == nil {
		logs.NewLog.Error("No contact data received from Kafka.")
		return nil
	}
	go InsertContactDataToMySQL(ContactMessages)
	return nil
}

func SendConsumerActivityToMySQL() error {
	Database := "kafka"
	configData, err := config.LoadDatabaseConfig(Database)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
		return err
	}
	kafkaConfig, ok := configData.(config.KafkaConfig)
	if !ok {
		logs.NewLog.Error("Error converting database config to KafkaConfig")
		return fmt.Errorf("error converting database config to KafkaConfig")
	}
	consumer, err := database.NewKafkaConsumer(&kafkaConfig, kafkaConfig.Topic2)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	ActivityMessages := database.ConsumeMessage(consumer, kafkaConfig.Topic2)
	if ActivityMessages == nil {
		logs.NewLog.Error("No activity data received from Kafka.")
		return nil
	}
	go InsertActivityDataToMySQL(ActivityMessages)
	return nil
}
func InsertContactDataToMySQL(messages []string) error {
	mysqlConnector, err := database.ConnectMySQL()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error configuring MySQL: %v", err))
		return err
	}
	db, err := mysqlConnector.Connect()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error connecting to MySQL: %v", err))
		return err
	}
	batchSize := 100
	contactsInserted := 0
	for _, message := range messages {
		if message == "EOF" {
			continue
		}
		message = strings.TrimRight(message, ",")
		query := fmt.Sprintf("INSERT INTO Contacts (ID, Name, Email, Details, Status) VALUES %s;", message)
		_, err = db.Exec(query)
		if err != nil {
			logs.NewLog.Errorf(fmt.Sprintf("error executing MySQL query: %v", err))
		}
		contactsInserted++
		if contactsInserted%batchSize == 0 {
			logs.NewLog.Info(fmt.Sprintf("%d contacts inserted\n", contactsInserted))
		}
	}

	return nil
}

func InsertActivityDataToMySQL(messages []string) error {
	mysqlConnector, err := database.ConnectMySQL()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error configuring MySQL: %v", err))
		return err
	}
	db, err := mysqlConnector.Connect()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error connecting to MySQL: %v", err))
		return err
	}
	defer db.Close()
	filteredMessages := make([]string, 0, len(messages))
	for _, message := range messages {
		if message != "EOF" {
			message = strings.TrimRight(message, ",")
			filteredMessages = append(filteredMessages, message)
		}
	}
	for _, message := range filteredMessages {
		query := fmt.Sprintf("INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType,ActivityDate)VALUES %s;",
			message)

		_, err = db.Exec(query)
		if err != nil {
			logs.NewLog.Errorf(fmt.Sprintf("error executing MySQL query: %v", err))
		}
	}
	return nil
}
