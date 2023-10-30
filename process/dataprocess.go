package process

import (
	cryptoRand "crypto/rand"
	"datastream/logs"
	"datastream/services"
	"datastream/types"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/google/uuid"
)

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
		err := fmt.Sprintln("unsupported file type, please upload .csv file")
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
func validateCSVRecord(lineNumber int, record []string) []error {
	var errors []error

	if len(record) != 3 {
		logs.NewLog.Error(fmt.Sprintf("invalid number of columns in CSV record %d: %v", lineNumber, record))
		errors = append(errors, fmt.Errorf("invalid number of columns in CSV record %d: %v", lineNumber, record))
		return errors
	}
	name := record[0]
	email := record[1]
	details := record[2]
	if !isValidName(name) {
		logs.NewLog.Error(fmt.Sprintf("invalid name in CSV record %d: %s", lineNumber, name))
		errors = append(errors, fmt.Errorf("invalid name in CSV record %d: %s", lineNumber, name))
	}
	if !isValidEmail(email) {
		logs.NewLog.Error(fmt.Sprintf("invalid email in CSV record %d: %s", lineNumber, email))
		errors = append(errors, fmt.Errorf("invalid email in CSV record %d: %s", lineNumber, email))
	}
	if !isValidDetails(details) {
		logs.NewLog.Error(fmt.Sprintf("invalid details in CSV record %d: %s", lineNumber, details))
		errors = append(errors, fmt.Errorf("invalid details in CSV record %d: %s", lineNumber, details))
	}
	return errors
}
func CSVReadToDataInsertion(filename string, batchSize int, doneChan chan struct{}) error {
	kafkahandler, err := services.NewKafkaHandler()
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka handler: %v", err))
	}
	uploadedfile, err := os.Open(filename)
	reader := csv.NewReader(uploadedfile)
	contactsBatches := make([]types.Contacts, 0)
	rowCount := 0
	lineNumber := 0

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
		validateCSVRecord(lineNumber, record)

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

		go func(batch []types.Contacts) {
			activityProcess(batch, kafkahandler)
		}(contactsBatches)
		contactsBatches = make([]types.Contacts, 0)
		rowCount = 0
	}
	doneChan <- struct{}{}
	logs.NewLog.Errorf(fmt.Sprintln("Data Inserted to Kafka"))
	go SendKafkaConsumerContactsToMySQL(kafkahandler)

	SendKafkaConsumerActivityToMySQL()
	return err
}

func activityProcess(contacts []types.Contacts, kafkahandler *services.KafkaHandler) {
	for _, contact := range contacts {
		statusContact, activitiesSlice, _ := ReturnContactsAndActivitiesStructs(contact)
		contactsData := getContactsDataString(statusContact)
		activityDetails := getActivityDetailsString(activitiesSlice)
		err := SendDataToKafkaProducers(kafkahandler, contactsData, activityDetails)
		if err != nil {
			logs.NewLog.Error(fmt.Sprint("Error sending data to Kafka", err))
		}
	}
}
func getContactsDataString(statusContact types.ContactStatus) string {
	statusInfo := fmt.Sprintf("('%s', '%s','%s', '%s', %d),", statusContact.Contact.ID, statusContact.Contact.Name,
		statusContact.Contact.Email, statusContact.Contact.Details, statusContact.Status)
	return statusInfo
}
func getActivityDetailsString(activities []types.ContactActivity) string {
	var details string
	for _, activity := range activities {
		details += fmt.Sprintf("('%s', %d, %d, '%s'),", activity.Contactid, activity.Campaignid,
			activity.Activitytype, activity.Activitydate.Format("2006-01-02 15:04:05"))

	}
	return details
}

func SendDataToKafkaProducers(kafkahandler *services.KafkaHandler, ContactsData string, ActivityData string) error {

	err1 := kafkahandler.SendMessage(kafkahandler.Config.ContactsTopic, ContactsData)
	if err1 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic1: %v", err1))
		return err1
	}
	err2 := kafkahandler.SendMessage(kafkahandler.Config.ActivityTopic, ActivityData)
	if err2 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic2: %v", err2))
		return err2
	}
	return nil
}
func SendKafkaConsumerContactsToMySQL(kafkahandler *services.KafkaHandler) {
	messages := []string{}
	messageCount := 0

	for {
		message, err := kafkahandler.ConsumeMessage(kafkahandler.Config.ContactsTopic)
		if err != nil {
			logs.NewLog.Error("Error consuming messages")
		}
		if message != "" {
			messages = append(messages, message)
			messageCount++
			query := "INSERT INTO Contacts(ID, Name, Email, Details, Status) VALUES"
			filteredMessages := make([]string, 0)

			for i, msg := range messages {
				if i == len(messages)-1 {
					msg = strings.TrimRight(msg, ",")
				}
				filteredMessages = append(filteredMessages, msg)
			}

			for i, msg := range filteredMessages {
				filteredMessages[i] = strings.Trim(msg, "[]")
			}

			query = fmt.Sprintf("%s %s;", query, strings.Join(filteredMessages, ""))
			fmt.Println(query)
			err := services.InsertDataToMySQL(query)
			messages = make([]string, 0)
			if err != nil {
				logs.NewLog.Errorf(fmt.Sprintf("Error inserting contacts data into MySQL: %v", err))
			}
		}
	}
}

func SendKafkaConsumerActivityToMySQL() {
	kafkahandlernew, _ := services.NewKafkaHandler()

	messages := []string{}
	messageCount := 0

	for {
		message, err := kafkahandlernew.ConsumeMessage(kafkahandlernew.Config.ActivityTopic)
		if err != nil {
			logs.NewLog.Error("Error consuming messages")
		}
		if message != "" {
			messages = append(messages, message)
			messageCount++

			query := "INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType, ActivityDate) VALUES"
			filteredMessages := make([]string, len(messages))

			for i, msg := range messages {
				filteredMessages = make([]string, 0)

				if i == len(messages)-1 {
					msg = strings.TrimRight(msg, ",")

				}
				filteredMessages = append(filteredMessages, msg)

			}
			for i, msg := range filteredMessages {
				filteredMessages[i] = strings.Trim(msg, "[]")
			}

			query = fmt.Sprintf("%s %s;", query, strings.Join(filteredMessages, ""))
			err := services.InsertDataToMySQL(query)

			messages = make([]string, 0)
			if err != nil {
				logs.NewLog.Errorf(fmt.Sprintf("Error inserting activity data into MySQL: %v", err))
			}
		}
	}
}
