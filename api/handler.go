package api

import (
	"datastream/config"
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

	var contactsData []types.Contacts

	for _, contact := range contacts {
		contactsData = append(contactsData, types.Contacts{
			ID:      contact.ID,
			Name:    contact.Name,
			Email:   contact.Email,
			Details: contact.Details,
		})
	}
	go activityProcess(contactsData)
}

func activityProcess(contacts []types.Contacts) {
	for _, contact := range contacts {
		fmt.Printf("ID: %d\n", contact.ID)

		statusContact, activitiesSlice, _ := process.MainData(contact.ID, contact)
		ContactsData, _ := getContactsData(statusContact)
		ActivityDetails := getActivityDetails(activitiesSlice)
		uploadToKafka(ContactsData, ActivityDetails)
	}
}

func getContactsData(statusContact types.ContactStatus) (string, string) {
	statusInfo := fmt.Sprintf("(%d, %s,%s, %s, %d)\n", statusContact.Contact.ID, statusContact.Contact.Name,
		statusContact.Contact.Email, statusContact.Contact.Details, statusContact.Status)
	return statusInfo, ""
}

func getActivityDetails(activities []types.ContactActivity) string {
	var details string
	for _, activity := range activities {
		details += fmt.Sprintf("(%d, %d, %d, %s)\n", activity.Contactid, activity.Campaignid,
			activity.Activitytype, activity.Activitydate)
	}
	return details
}

func uploadToKafka(StatusContact string, Activities string) {
	ConfigData, err := config.LoadKafkaConfigFromEnv()
	if err != nil {
		fmt.Printf("Error loading Kafka config: %v\n", err)
		return
	}
	producer1, producer2, err := config.NewKafkaProducers(ConfigData)
	if err != nil {
		fmt.Printf("Error creating Kafka producer: %v\n", err)
		return
	}
	defer producer1.Close()
	defer producer2.Close()
	message1 := ""
	message2 := ""
	err1 := config.SendMessage(producer1, ConfigData.Topic1, message1)
	if err1 != nil {
		fmt.Printf("Error sending message to Topic1: %v\n", err1)
	}
	err2 := config.SendMessage(producer2, ConfigData.Topic2, message2)
	if err2 != nil {
		fmt.Printf("Error sending message to Topic2: %v\n", err2)
	}
}

func TakeDataFromKafka() {
	ConfigData, err := config.LoadKafkaConfigFromEnv()
	consumer1, err := config.NewKafkaConsumer(ConfigData, ConfigData.Topic1)
	if err != nil {
		fmt.Printf("Error creating Kafka consumer for Topic1: %v\n", err)
		return
	}
	defer consumer1.Close()

	consumer2, err := config.NewKafkaConsumer(ConfigData, ConfigData.Topic2)
	if err != nil {
		fmt.Printf("Error creating Kafka consumer for Topic2: %v\n", err)
		return
	}
	defer consumer2.Close()

	go config.ConsumeMessage(consumer1, ConfigData.Topic1)
	go config.ConsumeMessage(consumer2, ConfigData.Topic2)
	select {}
}
func ResultPageHandler() {
}
func GetDataFromClickHouse() {
}
