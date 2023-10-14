package api

import (
	"datastream/config"
	"datastream/logs"
	"datastream/process"
	"datastream/services"
	"datastream/types"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/google/uuid"
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
	uuid := uuid.New()

	file, header, err := r.FormFile("file")
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error retrieving file: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	fileNameWithUUID := fmt.Sprintf("%s-%s", uuid, header.Filename)
	tmpFile, err := os.CreateTemp("", fileNameWithUUID)
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
	activityProcess(contacts, filePath)
	http.Redirect(w, r, "/HomePage.html", http.StatusSeeOther)
}

func activityProcess(contacts []types.Contacts, filePath string) {
	filename := strings.ReplaceAll(filePath, "/", "")
	topics := &config.KafkaConfig{
		ContactsTopic: filename + "Contacts",
		ActivityTopic: filename + "ContactsData",
	}
	done := make(chan struct{})
	contactCounter := 0
	for _, contact := range contacts {
		contactCounter++
		statusContact, activitiesSlice, _ := process.ReturnContactsAndActivitiesStructs(contact.ID, contact)
		contactsData, _ := getContactsDataString(statusContact)
		activityDetails := getActivityDetailsString(activitiesSlice)
		logs.NewLog.Info(fmt.Sprintf("Processing contact %d\n", contactCounter))

		go func() {
			process.SendDataToKafkaProducers(contactsData, activityDetails, topics)
			done <- struct{}{}
		}()
	}
	for i := 0; i < contactCounter; i++ {
		process.SendDataToKafkaProducers("EOF", "EOF", topics)
	}
	for i := 0; i < contactCounter; i++ {
		<-done
	}
	process.SendConsumerContactsToMySQL(topics)
	go process.SendConsumerActivityToMySQL(topics)
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

func MultipleQueryView(w http.ResponseWriter, r *http.Request) {
	htmlFile, err := os.Open("templates/QueryView.html")
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
func DisplayTheQueryResult(w http.ResponseWriter, r *http.Request) {
	results, err := services.GetQueryResultFromClickhouse()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error getting Result%v", http.StatusInternalServerError))
		return
	}
	tmpl, err := template.ParseFiles("templates/ResultPage.html")
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error parsing html file%v", http.StatusInternalServerError))
		return
	}

	data := struct {
		Results []config.ResultData
	}{
		Results: results,
	}

	err = tmpl.Execute(w, data)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Internal Server Error %v", http.StatusInternalServerError))
	}
}
