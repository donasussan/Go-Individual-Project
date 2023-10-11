package process

import (
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"fmt"
	"strings"
)

func SendDataToKafkaProducers(ContactsData string, ActivityData string) error {
	configData, err := database.LoadDatabaseConfig("kafka")
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
	configData, err := database.LoadDatabaseConfig(Database)
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
	configData, err := database.LoadDatabaseConfig(Database)
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

func DisplayQueryResults() ([]config.ResultData, error) {
	clickhouseConnector, err := ConnectClickhouse()
	if err != nil {
		return nil, err
	}
	db, err := clickhouseConnector.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	subquery := "SELECT ContactID FROM contact_based_activity_summary WHERE opened >= 30"

	subqueryRows, err := db.Query(subquery)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	defer subqueryRows.Close()

	results := make([]config.ResultData, 0)

	for subqueryRows.Next() {
		var contactID string
		err := subqueryRows.Scan(&contactID)
		if err != nil {
			logs.NewLog.Errorf(fmt.Sprint(err))
			return nil, err
		}

		fmt.Printf("ContactID: %s\n", contactID)

		query := fmt.Sprintf("SELECT ID, Email, JSONExtractString(Details, 'country') AS Country "+
			"FROM Contacts WHERE ID = '%s' AND JSONExtractString(Details, 'country') IN ('USA', 'UK')", contactID)

		// fmt.Println(query)

		rows, err := db.Query(query)

		if err == nil {
			if rows.Next() {
				fmt.Println("hi")
				var ID, Email, Country string
				err := rows.Scan(&ID, &Email, &Country)
				// fmt.Println(Country)
				if err != nil {
					logs.NewLog.Info("Cannot create struct for this user")
					continue
				}

				fmt.Printf("ID: %s, Email: %s, Country: %s\n", ID, Email, Country)

				result := config.ResultData{
					ID:      ID,
					Email:   Email,
					Country: Country,
				}
				results = append(results, result)

				rows.Close()
			} else {
				fmt.Println("continue")
				continue
			}
		} else {
			logs.NewLog.Info("User do not meet query criteria")
		}
	}
	return results, nil

}