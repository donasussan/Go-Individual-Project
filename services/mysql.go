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
func InsertDataToMySQL(messages []string, query string) error {
	db, err := EstablishMySQLConnection()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error establishing MySQL connection %v", err))
	}
	filteredMessages := make([]string, 0, len(messages))
	for _, message := range messages {
		message = strings.TrimRight(message, ",")
		filteredMessages = append(filteredMessages, message)
	}
	for _, message := range filteredMessages {
		query := fmt.Sprintf("%s %s;", query, message)
		_, err := db.Exec(query)
		if err != nil {
			logs.NewLog.Errorf(fmt.Sprintf("error executing MySQL query: %v", err))
			return err
		}
	}
	return nil
}
