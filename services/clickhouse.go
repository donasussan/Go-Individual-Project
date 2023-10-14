package services

import (
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
		"(SELECT ContactsID FROM dona_campaign.contact_activity WHERE opened >= 30))"
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
