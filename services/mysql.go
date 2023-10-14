package services

import (
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"fmt"
	"strings"
)

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
