package services

import (
	"database/sql"
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"fmt"
)

func ConnectClickhouse() (*config.ClickHouseConnector, error) {

	configData, err := database.LoadDatabaseConfig("clickhouse")
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error loading database config: %v", err))
	}
	clickhouseConfig, _ := configData.(config.ClickHouseConfig)
	clickhouseConnector := config.ClickHouseConnector{Config: clickhouseConfig}
	return &clickhouseConnector, nil
}
func ReturnClickhouseDB() (*sql.DB, error) {
	clickhouseConnector, err := ConnectClickhouse()
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	db, err := clickhouseConnector.Connect()
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return db, err
	}
	return db, err
}
func GetQueryResultFromClickhouse(query string) (*sql.Rows, error) {
	db, err := ReturnClickhouseDB()
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Could not connect to Clickhouse,%v", err))
	}
	rows, err := db.Query(query)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	return rows, nil
}
