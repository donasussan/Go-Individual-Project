package api

import (
	"datastream/config"
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
	done := make(chan struct{})
	contactCounter := 0
	for _, contact := range contacts {
		contactCounter++
		statusContact, activitiesSlice, _ := process.ReturnContactsAndActivitiesStructs(contact.ID, contact)
		contactsData, _ := getContactsDataString(statusContact)
		activityDetails := getActivityDetailsString(activitiesSlice)
		logs.NewLog.Info(fmt.Sprintf("Processing contact %d\n", contactCounter))

		go func() {
			process.SendDataToKafkaProducers(contactsData, activityDetails)
			done <- struct{}{}
		}()
	}
	for i := 0; i < contactCounter; i++ {
		process.SendDataToKafkaProducers("EOF", "EOF")
	}
	for i := 0; i < contactCounter; i++ {
		<-done
	}
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
	results, err := process.DisplayQueryResults()
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

// func DisplayTheQueryResultFromClickhouse(w http.ResponseWriter, r *http.Request)  {
// 	queryResult, _ := process.DisplayQueryResults()
// 	jsonData, err := json.Marshal(queryResult)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	w.Write(jsonData)
// 	return
// }

// func DisplayTheQueryResult(w http.ResponseWriter, r *http.Request) {
// 	results, err := process.DisplayQueryResults()
// 	if err != nil {

// 		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
// 		return
// 	}
// 	htmlTemplate := `
// 		<!DOCTYPE html>
// 		<html>

// 		<head>
// 			<title>Query Results</title>
// 		</head>

// 		<body>
// 			<h1>Query Results</h1>

// 			<table border="1">
// 				<tr>
// 					<th>ID</th>
// 					<th>Email</th>
// 					<th>Country</th>
// 				</tr>
// 				{{range .Results}}
// 				<tr>
// 					<td>{{.ID}}</td>
// 					<td>{{.Email}}</td>
// 					<td>{{.Country}}</td>
// 				</tr>
// 				{{end}}
// 			</table>
// 		</body>

// 		</html>`

// 	tmpl, err := template.New("result").Parse(htmlTemplate)
// 	if err != nil {
// 		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
// 		return
// 	}

// 	data := struct {
// 		Results []config.ResultData
// 	}{
// 		Results: results,
// 	}

// 	err = tmpl.Execute(w, data)
// 	if err != nil {
// 		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
// 	}
// }
