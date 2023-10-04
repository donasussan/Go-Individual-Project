package api

import (
	"datastream/config"
	"datastream/database"
	"datastream/logs"
	"datastream/process"
	"datastream/types"
	"fmt"
	"html/template"
	"net/http"
)

func HomePageHandler(w http.ResponseWriter, r *http.Request) {
	logger, _ := logs.NewSimpleLogger("datalog.log")

	tmpl, err := template.ParseFiles("templates/HomePage.html")
	if err != nil {
		logger.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = tmpl.Execute(w, nil)
	if err != nil {
		logger.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func Upload(w http.ResponseWriter, r *http.Request) {

	logger, _ := logs.NewSimpleLogger("datalog.log")
	file, header, err := r.FormFile("file")
	if err != nil {
		logger.Error(fmt.Sprintf("Error retrieving file: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	filename := header.Filename
	contacts, err := process.CSVread(filename)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	contactsStruct := ContactsStruct(contacts)
	go activityProcess(contactsStruct)
}

func ContactsStruct(contacts []types.Contacts) []types.Contacts {
	var contactsStruct []types.Contacts

	for _, contact := range contacts {
		contactsStruct = append(contactsStruct, types.Contacts{
			ID:      contact.ID,
			Name:    contact.Name,
			Email:   contact.Email,
			Details: contact.Details,
		})
	}

	return contactsStruct
}

func activityProcess(contacts []types.Contacts) {

	for _, contact := range contacts {
		statusContact, activitiesSlice, _ := process.MainData(contact.ID, contact)
		contactsData, _ := getContactsData(statusContact)
		activityDetails := getActivityDetails(activitiesSlice)
		go SendContactDataToKafka(contactsData, activityDetails)
		go ReadKafkaData()
	}
}
func SendContactDataToKafka(contactsData string, activityDetails string) {
	logger, _ := logs.NewSimpleLogger("datalog.log")
	var kafkaConfig config.KafkaConfig
	kafkaStore := database.NewKafkaStore(kafkaConfig)
	err := kafkaStore.InsertContact(contactsData, activityDetails)
	if err != nil {
		logger.Error(fmt.Sprintf("Error inserting contact data into Kafka: %v\n", err))
	}
}
func getContactsData(statusContact types.ContactStatus) (string, string) {
	statusInfo := fmt.Sprintf("(%d, %s,%s, %s, %d),", statusContact.Contact.ID, statusContact.Contact.Name,
		statusContact.Contact.Email, statusContact.Contact.Details, statusContact.Status)
	return statusInfo, ""
}

func getActivityDetails(activities []types.ContactActivity) string {
	var details string
	for _, activity := range activities {
		details += fmt.Sprintf("(%d, %d, %d, %s),", activity.Contactid, activity.Campaignid,
			activity.Activitytype, activity.Activitydate)
	}
	return details
}
func ReadKafkaData() error {
	logger, _ := logs.NewSimpleLogger("datalog.log")
	configData, err := config.LoadKafkaConfigFromEnv()
	if err != nil {
		logger.Errorf(fmt.Sprintf("Error loading Kafka config: %v", err))
		return err
	}
	consumer, err := config.NewKafkaConsumer(configData, configData.Topic1)
	if err != nil {
		logger.Errorf(fmt.Sprintf("Error creating Kafka consumer: %v", err))
		return err
	}
	defer consumer.Close()

	messages := config.ConsumeMessage(consumer, configData.Topic1)

	if messages == nil {
		logger.Error("No messages received from Kafka.")
		return nil
	}
	for _, message := range messages {
		fmt.Println(message)
	}
	return nil
}

// func SendContactDataToMySQL(messages []string) error {
// 	//logger, _ := logs.NewSimpleLogger("datalog.log")
// 	db, err := config.ConnectToMySQL()
// 	//logger.Error(fmt.Println(db))

// 	if err != nil {
// 		return fmt.Errorf("error connecting to MySQL: %v", err)
// 	}
// 	defer db.Close()

// 	// Trim trailing commas from each element in the message slice
// 	for i := range messages {
// 		messages[i] = strings.TrimRight(messages[i], ",")

// 		query := fmt.Sprintf("INSERT INTO contacts (Name, Email, Details, Status) VALUES %s;", messages)

// 		// Execute the query with the message values
// 		_, err = db.Exec(query)
// 		if err != nil {
// 			return fmt.Errorf("error executing MySQL query: %v", err)
// 		}
// 	}
// 	return nil
// }

// func ResultPageHandler() {
// }
// func GetDataFromClickHouse() {
// }
