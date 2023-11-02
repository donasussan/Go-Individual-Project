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
func InsertDataIntoTable(db *sql.DB, tableName string, data map[string]interface{}) error {
	var columns []string
	var placeholders []string
	var values []interface{}

	for column, value := range data {
		columns = append(columns, column)
		placeholders = append(placeholders, "?")
		values = append(values, value)
	}

	sqlStatement := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	stmt, err := db.Prepare(sqlStatement)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(values...)
	if err != nil {
		return err
	}

	return nil
}
