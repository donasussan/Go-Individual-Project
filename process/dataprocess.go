package process

import (
	cryptoRand "crypto/rand"
	"datastream/config"
	"datastream/database"
	"datastream/logs"
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
	producer1, producer2, err := database.NewKafkaProducers(&kafkaConfig)
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka producers: %v", err))
		return err
	}
	defer producer1.Close()
	defer producer2.Close()
	err1 := database.SendMessage(producer1, kafkaConfig.ContactsTopic, ContactsData)
	if err1 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic1: %v", err1))
		return err1
	}
	err2 := database.SendMessage(producer2, kafkaConfig.ActivityTopic, ActivityData)
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
	consumer, err := database.NewKafkaConsumer(&kafkaConfig, topics.ContactsTopic)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	ContactMessages := database.ConsumeMessage(consumer, topics.ContactsTopic)
	if ContactMessages == nil {
		logs.NewLog.Error("No contact data received from Kafka.")
		return nil
	}
	go InsertContactDataToMySQL(ContactMessages)
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
	consumer, err := database.NewKafkaConsumer(&kafkaConfig, topics.ActivityTopic)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	ActivityMessages := database.ConsumeMessage(consumer, topics.ActivityTopic)
	if ActivityMessages == nil {
		logs.NewLog.Error("No activity data received from Kafka.")
		return nil
	}
	go InsertActivityDataToMySQL(ActivityMessages)
	return nil
}
func InsertContactDataToMySQL(messages []string) error {
	mysqlConnector, err := ConnectMySQL()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error configuring MySQL: %v", err))
		return err
	}
	db, err := mysqlConnector.Connect()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error connecting to MySQL Database: %v", err))
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
func ConnectMySQL() (*config.MySQLConnector, error) {
	Database := "mysql"
	configData, err := database.LoadDatabaseConfig(Database)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
	}
	mysqlConfig, _ := configData.(config.MySQLConfig)
	mysqlConnector := config.MySQLConnector{Config: mysqlConfig}
	return &mysqlConnector, nil
}
func ConnectClickhouse() (*config.ClickHouseConnector, error) {
	Database := "clickhouse"
	configData, err := database.LoadDatabaseConfig(Database)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
	}
	clickhouseConfig, _ := configData.(config.ClickHouseConfig)
	clickhouseConnector := config.ClickHouseConnector{Config: clickhouseConfig}
	return &clickhouseConnector, nil
}

func InsertActivityDataToMySQL(messages []string) error {
	mysqlConnector, err := ConnectMySQL()
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

func GetQueryResultFromClickhouse() ([]config.ResultData, error) {
	clickhouseConnector, err := ConnectClickhouse()
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	db, err := clickhouseConnector.Connect()
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	defer db.Close()
	query := "SELECT co.ID, co.Email, JSONExtractString(co.Details, 'country') AS Country FROM Contacts AS co " +
		"WHERE (JSONExtractString(co.Details, 'country') IN ('USA', 'UK')) AND (co.ID IN" +
		"(SELECT ContactID FROM dona_campaign.contact_based_activity_summary WHERE opened >= 30))"
	rows, err := db.Query(query)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	defer rows.Close()
	var results []config.ResultData
	for rows.Next() {
		var ID, Email, Country string
		err := rows.Scan(&ID, &Email, &Country)
		if err != nil {
			logs.NewLog.Info("Cannot create a struct for this user")
			continue
		}
		fmt.Printf("ID: %s, Email: %s, Country: %s\n", ID, Email, Country)
		result := config.ResultData{
			ID:      ID,
			Email:   Email,
			Country: Country,
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	return results, nil
}
