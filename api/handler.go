package api

import (
	"datastream/database"
	"datastream/logs"
	"datastream/process"
	"datastream/types"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
)

func HomePageHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("templates/HomePage.html")
	if err != nil {
		logs.NewLog.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = tmpl.Execute(w, nil)
	if err != nil {
		logs.NewLog.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func Upload(w http.ResponseWriter, r *http.Request) {

	file, header, err := r.FormFile("file")
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error retrieving file: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	tmpFile, err := os.CreateTemp("", "newuploaded-*.csv")
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error creating temporary file: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer tmpFile.Close()

	_, err = io.Copy(tmpFile, file)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error copying file content: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	filename := header.Filename
	contacts, err := process.CSVread(filename)
	if err != nil {
		logs.NewLog.Error(err.Error())
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
		SendContactDataToKafka(contactsData, activityDetails)

	}
	SendContactDataToKafka("EOF", "EOF")
	go database.ReadKafkaTopic1()
	go database.ReadKafkaTopic2()
}
func SendContactDataToKafka(contactsData string, activityDetails string) {
	err := database.InsertContact(contactsData, activityDetails)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error inserting contact data into Kafka: %v\n", err))
	}
}
func getContactsData(statusContact types.ContactStatus) (string, string) {
	statusInfo := fmt.Sprintf("(%d, '%s','%s', '%s', %d),", statusContact.Contact.ID, statusContact.Contact.Name,
		statusContact.Contact.Email, statusContact.Contact.Details, statusContact.Status)
	return statusInfo, ""
}

func getActivityDetails(activities []types.ContactActivity) string {
	var details string
	for _, activity := range activities {
		details += fmt.Sprintf("(%d, %d, %d,%s),", activity.Contactid, activity.Campaignid,
			activity.Activitytype, activity.Activitydate)
	}
	return details
}

// func ResultPageHandler() {
// }
// func GetDataFromClickHouse() {
// }
