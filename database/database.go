package database

import (
	"datastream/config"
	"datastream/logs"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func LoadDatabaseConfig(Database string) (config.DatabaseConfig, error) {
	if err := godotenv.Load(); err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error loading .env file: %v", err))
		return nil, err
	}

	switch Database {
	case "mysql":
		mysqlConfig := config.MySQLConfig{
			Username: os.Getenv("DB_USERNAME"),
			Password: os.Getenv("DB_PASSWORD"),
			Hostname: os.Getenv("DB_HOST"),
			Port:     os.Getenv("DB_PORT"),
			DBName:   os.Getenv("DB_NAME"),
		}
		return mysqlConfig, nil
	case "kafka":

		kafkaConfig := config.KafkaConfig{
			Broker: os.Getenv("KAFKA_BROKER"),
		}
		return kafkaConfig, nil
	case "clickhouse":
		clickHouseConfig := config.ClickHouseConfig{
			Username: os.Getenv("CLICK_USERNAME"),
			Password: os.Getenv("CLICK_PASSWORD"),
			Hostname: os.Getenv("CLICK_HOST"),
			Port:     os.Getenv("CLICK_PORT"),
			DBName:   os.Getenv("CLICK_DB_NAME"),
		}
		return clickHouseConfig, nil
	default:
		return nil, fmt.Errorf("unsupported DB_TYPE: %s", Database)

	}

}
