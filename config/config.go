package config

import (
	"database/sql"
	"datastream/logs"
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
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
	Broker string
	Topic1 string
	Topic2 string
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
	config ClickHouseConfig
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
	DSN := c.config.GetDSN()
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

func LoadDatabaseConfig(Database string) (DatabaseConfig, error) {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
		return nil, err
	}

	switch Database {
	case "mysql":
		mysqlConfig := MySQLConfig{
			Username: os.Getenv("DB_USERNAME"),
			Password: os.Getenv("DB_PASSWORD"),
			Hostname: os.Getenv("DB_HOST"),
			Port:     os.Getenv("DB_PORT"),
			DBName:   os.Getenv("DB_NAME"),
		}
		return mysqlConfig, nil
	case "kafka":
		kafkaConfig := KafkaConfig{
			Broker: os.Getenv("KAFKA_BROKER"),
			Topic1: os.Getenv("KAFKA_TOPIC_CONTACTS"),
			Topic2: os.Getenv("KAFKA_TOPIC_CONTACT_ACTIVITY"),
		}
		return kafkaConfig, nil
	case "clickhouse":
		clickHouseConfig := ClickHouseConfig{
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

func NewKafkaProducers(config *KafkaConfig) (sarama.SyncProducer, sarama.SyncProducer, error) {
	producer1, err := sarama.NewSyncProducer([]string{config.Broker}, nil)
	if err != nil {
		return nil, nil, err
	}

	producer2, err := sarama.NewSyncProducer([]string{config.Broker}, nil)
	if err != nil {
		return nil, nil, err
	}

	return producer1, producer2, nil
}

func SendMessage(producer sarama.SyncProducer, topic string, message string) error {
	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.SendMessage(producerMessage)
	return err
}

func NewKafkaConsumer(config *KafkaConfig, topic string) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer([]string{config.Broker}, nil)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
func ConsumeMessage(consumer sarama.Consumer, topic string) []string {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		logs.NewLog.Error("Error creating partition consumer")
		return nil
	}
	defer partitionConsumer.Close()
	var messages []string

	for msg := range partitionConsumer.Messages() {
		message := string(msg.Value)
		messages = append(messages, message)

		if message == "EOF" {
			break
		}
	}
	return messages
}
