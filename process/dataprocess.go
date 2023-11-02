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
	KafkaHandlerIns, err := services.NewKafkaHandler()
	if err != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error creating Kafka handler: %v", err))
	}
	uploadedfile, err := os.Open(filename)
	reader := csv.NewReader(uploadedfile)
	contactsBatches := make([]types.Contacts, 0)
	rowCount := 0
	lineNumber := 0
	batchCount := 0

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

		if len(contactsBatches) == batchSize {
			activityProcess(contactsBatches, KafkaHandlerIns)
			contactsBatches = make([]types.Contacts, 0)
			batchCount++
			fmt.Printf("Processed %d batches to Kafka\n", batchCount)
		}
	}

	doneChan <- struct{}{}
	logs.NewLog.Info(fmt.Sprintln("Data Inserted to Kafka"))

	go SendKafkaConsumerActivityToMySQL()
	go SendKafkaConsumerContactsToMySQL()

	return err
}

func activityProcess(contacts []types.Contacts, KafkaHandlerIns *services.KafkaHandler) {
	for _, contact := range contacts {
		statusContact, activitiesSlice, _ := ReturnContactsAndActivitiesStructs(contact)
		contactsData := getContactsDataString(statusContact)
		activityDetails := getActivityDetailsString(activitiesSlice)
		err := SendDataToKafkaProducers(KafkaHandlerIns, contactsData, activityDetails)
		if err != nil {
			logs.NewLog.Error(fmt.Sprint("Error sending data to Kafka", err))
		}
	}
}
func getContactsDataString(statusContact types.ContactStatus) string {
	statusInfo := fmt.Sprintf("%s,%s,%s,%d,%s;", statusContact.Contact.ID, statusContact.Contact.Name,
		statusContact.Contact.Email, statusContact.Status, statusContact.Contact.Details)
	return statusInfo
}
func getActivityDetailsString(activities []types.ContactActivity) string {
	var details string
	for _, activity := range activities {
		details += fmt.Sprintf("%s,%d,%d,%s;", activity.Contactid, activity.Campaignid,
			activity.Activitytype, activity.Activitydate.Format("2006-01-02 15:04:05"))

	}
	return details
}

func SendDataToKafkaProducers(KafkaHandlerIns *services.KafkaHandler, ContactsData string, ActivityData string) error {

	err1 := KafkaHandlerIns.SendMessage(KafkaHandlerIns.Config.ContactsTopic, ContactsData)
	if err1 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic1: %v", err1))
		return err1
	}
	err2 := KafkaHandlerIns.SendMessage(KafkaHandlerIns.Config.ActivityTopic, ActivityData)
	if err2 != nil {
		logs.NewLog.Fatalf(fmt.Sprintf("Error sending message to Topic2: %v", err2))
		return err2
	}
	return nil
}

func SendKafkaConsumerContactsToMySQL() {
	db, err := services.EstablishMySQLConnection()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error establishing MySQL connection: %v", err))
	}
	KafkaHandlerIns_contacts, _ := services.NewKafkaHandler()

	for {
		message, err := KafkaHandlerIns_contacts.ConsumeMessage(KafkaHandlerIns_contacts.Config.ContactsTopic)
		if err != nil {
			logs.NewLog.Error("Error consuming messages")
		}
		if message != "" {
			parts := strings.Split(message, ";")

			for _, part := range parts {
				values := strings.SplitN(part, ",", 5)

				if len(values) == 5 {
					keyValuePairs := make(map[string]interface{})
					keyValuePairs["ID"] = values[0]
					keyValuePairs["Name"] = values[1]
					keyValuePairs["Email"] = values[2]
					keyValuePairs["Status"] = values[3]
					keyValuePairs["Details"] = values[4]

					err := services.InsertDataIntoTable(db, "Contacts", keyValuePairs)

					if err != nil {
						logs.NewLog.Error(fmt.Sprintf("Error inserting data into MySQL: %v", err))
					}
				} else {
					logs.NewLog.Error("Invalid part format")
				}
			}
		}
	}
}
func SendKafkaConsumerActivityToMySQL() {
	KafkaHandlerIns_Activity, _ := services.NewKafkaHandler()
	db, err := services.EstablishMySQLConnection()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error establishing MySQL connection: %v", err))
	}

	for {
		message, err := KafkaHandlerIns_Activity.ConsumeMessage(KafkaHandlerIns_Activity.Config.ActivityTopic)
		if err != nil {
			logs.NewLog.Error("Error consuming messages")
		}
		if message != "" {
			parts := strings.Split(message, ";")

			for _, part := range parts {
				values := strings.SplitN(part, ",", 4)

				if len(values) == 4 {
					keyValuePairs := make(map[string]interface{})
					keyValuePairs["ContactsID"] = values[0]
					keyValuePairs["CampaignID"] = values[1]
					keyValuePairs["ActivityType"] = values[2]
					keyValuePairs["ActivityDate"] = values[3]

					err := services.InsertDataIntoTable(db, "ContactActivity", keyValuePairs)

					if err != nil {
						logs.NewLog.Error(fmt.Sprintf("Error inserting data into MySQL: %v", err))
					}
				} else {
					logs.NewLog.Error("Invalid part format")
				}
			}
		}
	}
}
