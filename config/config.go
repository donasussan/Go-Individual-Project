package config

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type MySQLConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

func LoadMySQLConfigFromEnv() (*MySQLConfig, error) {
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")

	config := &MySQLConfig{
		Username: dbUser,
		Password: dbPassword,
		Hostname: dbHost,
		Port:     dbPort,
		DBName:   dbName,
	}

	return config, nil
}

func GenerateMySQLDSN(config *MySQLConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config.Username, config.Password, config.Hostname, config.Port, config.DBName)
}

type MySQLDatabase struct {
	DSN string
}

func (m *MySQLDatabase) Connect(config *MySQLConfig) (*sql.DB, error) {
	return sql.Open("mysql", m.DSN)
}

// func (m *MySQLDatabase) Query(query string) error {
// 	fmt.Println("Executing MySQL query:", query)
// 	return nil
// }

func (m *MySQLDatabase) Close() error {
	fmt.Println("Closing MySQL connection")
	return nil
}
func ConnectToMySQL() (*sql.DB, error) {
	config, err := LoadMySQLConfigFromEnv()
	if err != nil {
		return nil, err
	}

	dsn := GenerateMySQLDSN(config)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return db, nil
}

type ClickHouseConfig struct {
	Username string
	Password string
	Hostname string
	Port     string
	DBName   string
}

func LoadClickHouseConfigFromEnv() (*ClickHouseConfig, error) {
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	dbUser := os.Getenv("CLICK_USERNAME")
	dbPassword := os.Getenv("CLICK_PASSWORD")
	dbHost := os.Getenv("CLICK_HOST")
	dbPort := os.Getenv("CLICK_PORT")
	dbName := os.Getenv("CLICK_DB_NAME")

	config := &ClickHouseConfig{
		Username: dbUser,
		Password: dbPassword,
		Hostname: dbHost,
		Port:     dbPort,
		DBName:   dbName,
	}

	return config, nil
}

func GenerateClickHouseDSN(config *ClickHouseConfig) string {
	return fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s",
		config.Hostname, config.Port, config.Username, config.Password, config.DBName)
}

type ClickHouseDatabase struct {
	DSN  string
	Conn *sql.DB
}

func (c *ClickHouseDatabase) Connect() error {
	connect, err := sql.Open("clickhouse", c.DSN)
	if err != nil {
		return err
	}
	c.Conn = connect
	return nil
}

func (c *ClickHouseDatabase) Query(query string) error {
	_, err := c.Conn.Exec(query)
	if err != nil {
		return err
	}
	fmt.Println("Executing ClickHouse query:", query)
	return nil
}

func (c *ClickHouseDatabase) Close() error {
	if c.Conn != nil {
		err := c.Conn.Close()
		if err != nil {
			return err
		}
		fmt.Println("Closing ClickHouse connection")
	}
	return nil
}

type KafkaConfig struct {
	Brokers string
	Topic1  string
	Topic2  string
}

func LoadKafkaConfigFromEnv() (*KafkaConfig, error) {
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	topic1 := os.Getenv("KAFKA_TOPIC1")
	topic2 := os.Getenv("KAFKA_TOPIC2")

	config := &KafkaConfig{
		Brokers: brokers,
		Topic1:  topic1,
		Topic2:  topic2,
	}

	return config, nil
}

func NewKafkaProducers(config *KafkaConfig) (sarama.SyncProducer, sarama.SyncProducer, error) {
	producer1, err := sarama.NewSyncProducer([]string{config.Brokers}, nil)
	if err != nil {
		return nil, nil, err
	}

	producer2, err := sarama.NewSyncProducer([]string{config.Brokers}, nil)
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
	consumer, err := sarama.NewConsumer([]string{config.Brokers}, nil)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
func ConsumeMessage(consumer sarama.Consumer, topic string) []string {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("Error creating partition consumer for %s: %v\n", topic, err)
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
