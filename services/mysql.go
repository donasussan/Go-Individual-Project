package services

import (
	"database/sql"
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

func InsertContactDataToMySQL(messages []string, table string) error {
	db, err := EstablishMySQLConnection()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error establishing MySQL connection %v", err))
	}
	defer db.Close()
	batchSize := 100
	contactsInserted := 0
	for _, message := range messages {
		if message == "EOF" {
			break
		}
		message = strings.TrimRight(message, ",")
		query := fmt.Sprintf("INSERT INTO %s (ID, Name, Email, Details, Status) VALUES %s;", table, message)
		_, err = db.Exec(query)
		if err != nil {
			if strings.Contains(err.Error(), "Duplicate entry") {
				logs.NewLog.Warning("Duplicate entry: " + err.Error())
			} else {
				logs.NewLog.Error("Error executing MySQL query: " + err.Error())
			}
		}
		contactsInserted++
		if contactsInserted%batchSize == 0 {
			logs.NewLog.Info(fmt.Sprintf("%d Contacts inserted to MySQL\n", contactsInserted))
		}
	}
	return nil
}
func InsertActivityDataToMySQL(messages []string, table string) error {
	db, err := EstablishMySQLConnection()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error establishing MySQL connection %v", err))
	}
	defer db.Close()
	filteredMessages := make([]string, 0, len(messages))
	for _, message := range messages {
		message = strings.TrimRight(message, ",")
		filteredMessages = append(filteredMessages, message)
	}
	for _, message := range filteredMessages {
		query := fmt.Sprintf("INSERT INTO %s (ContactsID, CampaignID, ActivityType,ActivityDate)VALUES %s;",
			table, message)
		fmt.Println(query)

		_, err = db.Exec(query)
		if err != nil {
			logs.NewLog.Errorf(fmt.Sprintf("error executing MySQL query: %v", err))
		}
	}
	return nil
}

func EstablishMySQLConnection() (*sql.DB, error) {
	mysqlConnector, err := ConnectMySQL()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error configuring MySQL: %v", err))
		return nil, err
	}
	db, err := mysqlConnector.Connect()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("error connecting to MySQL: %v", err))
		return nil, err
	}
	return db, nil
}
