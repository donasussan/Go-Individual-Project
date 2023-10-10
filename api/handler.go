package api

import (
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
	file, _, err := r.FormFile("file")
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
	filePath := tmpFile.Name()
	contacts, err := process.CSVReadToContactsStruct(filePath)
	if err != nil {
		logs.NewLog.Error(err.Error())
		return
	}
	activityProcess(contacts)
}

func activityProcess(contacts []types.Contacts) {
	contactCounter := 0
	for _, contact := range contacts {
		contactCounter++
		statusContact, activitiesSlice, _ := process.ReturnContactsAndActivitiesStructs(contact.ID, contact)
		contactsData, _ := getContactsDataString(statusContact)
		activityDetails := getActivityDetailsString(activitiesSlice)
		logs.NewLog.Info(fmt.Sprintf("Processing contact %d\n", contactCounter))
		process.SendDataToKafkaProducers(contactsData, activityDetails)
	}
	process.SendDataToKafkaProducers("EOF", "EOF")
	go process.SendConsumerContactsToMySQL()
	go process.SendConsumerActivityToMySQL()
}

func getContactsDataString(statusContact types.ContactStatus) (string, string) {
	statusInfo := fmt.Sprintf("('%s', '%s','%s', '%s', %d),", statusContact.Contact.ID, statusContact.Contact.Name,
		statusContact.Contact.Email, statusContact.Contact.Details, statusContact.Status)
	return statusInfo, ""
}

func getActivityDetailsString(activities []types.ContactActivity) string {
	var details string
	for _, activity := range activities {
		details += fmt.Sprintf("('%s', %d, %d,%s),", activity.Contactid, activity.Campaignid,
			activity.Activitytype, activity.Activitydate)
	}
	return details
}

func ResultView(w http.ResponseWriter, r *http.Request) {
	htmlFile, err := os.Open("templates/ResultPage.html")
	if err != nil {
		logs.NewLog.Error("Error reading HTML file: " + err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer htmlFile.Close()

	w.Header().Set("Content-Type", "text/html")

	_, err = io.Copy(w, htmlFile)
	if err != nil {
		logs.NewLog.Error("Error serving HTML content: " + err.Error())
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}
