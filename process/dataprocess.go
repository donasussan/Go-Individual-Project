package process

import (
	cryptoRand "crypto/rand"
	"datastream/logs"
	"datastream/services"
	"datastream/types"
	"encoding/csv"
	"errors"
	"fmt"
	"html/template"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"unicode"

	"github.com/google/uuid"
)

var FileUploaded = template.Must(template.ParseFiles("templates/HomePage.html"))

func generateRandomID() (string, error) {
	uuidObj, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	randomBytes := make([]byte, 8)
	_, err = io.ReadFull(cryptoRand.Reader, randomBytes)
	if err != nil {
		return "", err
	}
	randomString := fmt.Sprintf("%s-%x", uuidObj, randomBytes)
	randomString = strings.ReplaceAll(randomString, "-", "")
	return randomString, nil
}
func ValidateUploadedFileFormat(filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		errMsg := fmt.Sprintf("file does not exist: %s", filename)
		logs.NewLog.Error(errMsg)
		return errors.New(errMsg)
	}
	file, err := os.Open(filename)
	if err != nil {
		errMsg := fmt.Sprintf("failed to open file: %v", err)
		logs.NewLog.Error(errMsg)
		return errors.New(errMsg)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		errMsg := fmt.Sprintf("failed to get file information: %v", err)
		logs.NewLog.Error(errMsg)
		return errors.New(errMsg)
	}
	if stat.Size() == 0 {
		errMsg := fmt.Sprintf("file is empty: %s", filename)
		logs.NewLog.Error(errMsg)
		return errors.New(errMsg)
	}
	fileType := getFileType(filename)
	switch fileType {
	case "csv":
		if err != nil {
			logs.NewLog.Error(err.Error())
			return err
		}
	default:
		err := fmt.Sprint("unsupported file type, please upload .csv file")
		logs.NewLog.Error(err)
		return errors.New(err)
	}
	return nil
}
func getFileType(filename string) string {
	parts := strings.Split(filename, ".")
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}
	return "Invalid File Type"
}
func validateCSVRecord(lineNumber int, record []string) error {
	if len(record) != 3 {
		logs.NewLog.Error(fmt.Sprintf("invalid number of columns in CSV record %d: %v", lineNumber, record))
		return nil
	}
	name := record[0]
	email := record[1]
	details := record[2]
	if !isValidName(name) {
		logs.NewLog.Error(fmt.Sprintf("invalid name in CSV record %d: %s", lineNumber, name))
	}
	if !isValidEmail(email) {
		logs.NewLog.Error(fmt.Sprintf("invalid email in CSV record %d: %s", lineNumber, email))
	}
	if !isValidDetails(details) {
		logs.NewLog.Error(fmt.Sprintf("invalid details in CSV record %d: %s", lineNumber, details))
	}
	return nil
}
func CSVReadToDataInsertion(filename string, batchSize int) error {
	uploadedfile, err := os.Open(filename)
	reader := csv.NewReader(uploadedfile)
	contactsBatches := make([]types.Contacts, 0)
	rowCount := 0
	lineNumber := 0
	var wg sync.WaitGroup
	for {
		lineNumber++
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			logs.NewLog.Error(fmt.Sprintf("failed to read CSV: %v", err))
			return err
		}
		if err := validateCSVRecord(lineNumber, record); err != nil {
			logs.NewLog.Error(err.Error())
			return err
		}
		name := record[0]
		email := record[1]
		details := record[2]
		randomID, err := generateRandomID()
		if err != nil {
			logs.NewLog.Error(fmt.Sprintf("failed to generate random ID: %v", err))
			return err
		}
		contact := types.Contacts{
			ID:      randomID,
			Name:    name,
			Email:   email,
			Details: details,
		}
		rowCount++
		contactsBatches = append(contactsBatches, contact)

		if rowCount >= batchSize {
			wg.Add(1)
			go func(batch []types.Contacts) {
				defer wg.Done()
				activityProcess(batch)
				printContactsBatch(batch, lineNumber)
			}(contactsBatches)
			contactsBatches = make([]types.Contacts, 0)
			rowCount = 0
		}
	}

	if len(contactsBatches) > 0 {
		printContactsBatch(contactsBatches, lineNumber)
	}
	wg.Wait()
	go SendKafkaConsumerActivityToMySQL()
	err1 := SendKafkaConsumerContactsToMySQL()
	if err1 != nil {
		logs.NewLog.Error(fmt.Sprint("Error extracting data from consumer", err))
	}
	return err
}
func printContactsBatch(batch []types.Contacts, lineNumber int) {
	fmt.Printf("Batch at line %d inserted to Kafka\n", lineNumber)
}
func activityProcess(contacts []types.Contacts) {
	for _, contact := range contacts {
		statusContact, activitiesSlice, _ := ReturnContactsAndActivitiesStructs(contact)
		contactsData, _ := getContactsDataString(statusContact)
		activityDetails := getActivityDetailsString(activitiesSlice)
		err := SendDataToKafkaProducers(contactsData, activityDetails)
		if err != nil {
			logs.NewLog.Error(fmt.Sprint("Error sending data to Kafka", err))
		}
	}
}
func getContactsDataString(statusContact types.ContactStatus) (string, string) {
	statusInfo := fmt.Sprintf("('%s', '%s','%s', '%s', %d),", statusContact.Contact.ID, statusContact.Contact.Name,
		statusContact.Contact.Email, statusContact.Contact.Details, statusContact.Status)
	return statusInfo, ""
}
func getActivityDetailsString(activities []types.ContactActivity) string {
	var details string
	for _, activity := range activities {
		// details += fmt.Sprintf("('%s', %d, %d, '%s'),", activity.Contactid, activity.Campaignid,
		// 	activity.Activitytype, activity.Activitydate.Format("2006-01-02 15:04:05"))
		details += fmt.Sprintf("('%s', %d, %d, '%s'),", activity.Contactid, activity.Campaignid,
			activity.Activitytype, activity.Activitydate.Format("2006-01-02 15:04:05"))

	}
	return details
}

func SendDataToKafkaProducers(ContactsData string, ActivityData string) error {
	producer1, producer2, kafkaConfig, err := services.KafkaConfigAndCreateProducers()
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka producers: %v", err))
		return err
	}
	err1 := services.SendMessage(producer1, kafkaConfig.ContactsTopic, ContactsData)
	if err1 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic1: %v", err1))
		return err1
	}
	err2 := services.SendMessage(producer2, kafkaConfig.ActivityTopic, ActivityData)
	if err2 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic2: %v", err2))
		return err2
	}
	return nil
}

func SendKafkaConsumerContactsToMySQL() error {
	consumer, kafkaConfig, err := services.KafkaConfigAndCreateConsumer()
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	services.ConsumeMessage(consumer, kafkaConfig.ContactsTopic)
	return nil
}
func SendKafkaConsumerActivityToMySQL() error {
	consumer, kafkaConfig, err := services.KafkaConfigAndCreateConsumer()
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()
	services.ConsumeMessage(consumer, kafkaConfig.ActivityTopic)
	return nil
}
func isValidName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, char := range name {
		if !unicode.IsLetter(char) && !unicode.IsSpace(char) {
			return false
		}
	}
	return true
}
func isValidEmail(email string) bool {
	emailPattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	validEmail := regexp.MustCompile(emailPattern)
	return validEmail.MatchString(email)
}
func isValidDetails(details string) bool {
	detailsPattern := `^\{"dob": "\d{4}-\d{2}-\d{2}", "city": "[A-Za-z0-9 ]+", "country": "[A-Za-z0-9 ]+"\}$`
	validDetails := regexp.MustCompile(detailsPattern)
	return validDetails.MatchString(details)
}
