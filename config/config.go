package config

import (
	"database/sql"
	"datastream/logs"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
)

type DBConnector interface {
	Connect() (*sql.DB, error)
	Close() error
}

type MySQLConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

type MySQLConnector struct {
	Config MySQLConfig
	Db     *sql.DB
}

type KafkaConfig struct {
	Broker        string
	ContactsTopic string
	ActivityTopic string
}
type KafkaConnector struct {
	config   KafkaConfig
	Producer sarama.SyncProducer
	Consumer sarama.Consumer
}
type ClickHouseConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

type ClickHouseConnector struct {
	Config ClickHouseConfig
	db     *sql.DB
}
type DatabaseConfig interface {
	GetConfig() map[string]string
}

func (m MySQLConfig) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		m.Username,
		m.Password,
		m.Hostname,
		m.Port,
		m.DBName)
}

func (m *MySQLConnector) Connect() (*sql.DB, error) {
	dsn := m.Config.GetDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logs.NewLog.Error(err.Error())
		return nil, err
	}
	db.SetMaxOpenConns(100000)

	m.Db = db
	return db, nil
}
func (m *MySQLConnector) Close() error {
	if m.Db != nil {
		err := m.Db.Close()
		m.Db = nil
		return err
	}
	return nil
}

func (c ClickHouseConfig) GetDSN() string {
	return fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s",
		c.Hostname,
		c.Port,
		c.Username,
		c.Password,
		c.DBName)
}
func (c *ClickHouseConnector) Connect() (*sql.DB, error) {
	DSN := c.Config.GetDSN()
	db, err := sql.Open("clickhouse", DSN)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}
	c.db = db
	return db, nil
}

func (c *ClickHouseConnector) Close() error {
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

func (m MySQLConfig) GetConfig() map[string]string {
	return map[string]string{
		"Username": m.Username,
		"Password": m.Password,
		"Hostname": m.Hostname,
		"Port":     m.Port,
		"DBName":   m.DBName,
	}
}
func (c ClickHouseConfig) GetConfig() map[string]string {
	return map[string]string{
		"Username": c.Username,
		"Password": c.Password,
		"Hostname": c.Hostname,
		"Port":     c.Port,
		"DBName":   c.DBName,
	}
}

func (k KafkaConfig) GetConfig() map[string]string {
	return map[string]string{

		"Broker": k.Broker,
	}
}
