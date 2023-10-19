package services

import (
	"database/sql"
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"fmt"
)

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
func GetQueryResultFromClickhouse(query string) (*sql.Rows, error) {
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
	rows, err := db.Query(query)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	return rows, nil
}
