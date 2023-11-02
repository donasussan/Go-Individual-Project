package api

import (
	"datastream/logs"
	"datastream/process"
	"datastream/services"
	"datastream/types"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

var ModifyHomePage = template.Must(template.ParseFiles("templates/HomePage.html"))

func ParseFileHTMLTemplates(htmlpage string) *template.Template {
	tmpl, err := template.ParseFiles(htmlpage)
	if err != nil {
		return nil
	}
	return tmpl
}

func HomePageHandler(w http.ResponseWriter, r *http.Request) {
	tmpl := ParseFileHTMLTemplates("templates/HomePage.html")
	err := tmpl.Execute(w, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func HandleFileUpload(w http.ResponseWriter, r *http.Request) {
	doneChan := make(chan struct{})

	uploadedFile, header, err := r.FormFile("uploadedfile")
	if err != nil {
		handleError(w, "Error retrieving file: %v", err, http.StatusInternalServerError)
		return
	}

	if header.Filename == "" {
		handleError(w, "Invalid file name", nil, http.StatusBadRequest)
		return
	}

	fileNameWithUUID := generateUniqueFileName(header.Filename)
	filePath := filepath.Join("/home/user/go_learn/data_stream/uploadfiles", fileNameWithUUID)

	if err := saveUploadedFile(uploadedFile, filePath, header.Size); err != nil {
		handleError(w, "Error saving uploaded file: %v", err, http.StatusInternalServerError)
		return
	}

	if err := process.ValidateUploadedFileFormat(filePath); err != nil {
		displayErrorPage(w, err)
		return
	}

	go func() {
		if err := processCSVFile(filePath, doneChan); err != nil {
			logs.NewLog.Error(fmt.Sprintf("Error processing uploaded file: %v", err))
		}
	}()

	select {
	case <-doneChan:
		http.Redirect(w, r, "/HomePage.html?success=File+uploaded+successfully", http.StatusSeeOther)
	case <-time.After(500 * time.Second):
		handleError(w, "We are experiencing some technical difficulties!", nil, http.StatusInternalServerError)
	}
}

func generateUniqueFileName(originalName string) string {
	return fmt.Sprintf("%s-%s", uuid.New(), originalName)
}

func saveUploadedFile(uploadedFile io.Reader, filePath string, expectedSize int64) error {
	tempFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	written, err := io.Copy(tempFile, uploadedFile)
	if written != expectedSize {
		return errors.New("not all bytes were copied")
	}
	return err
}

func processCSVFile(filePath string, doneChan chan struct{}) error {
	return process.CSVReadToDataInsertion(filePath, 100, doneChan)
}

func displayErrorPage(w http.ResponseWriter, err error) {
	data := struct {
		Error string
	}{
		Error: err.Error(),
	}
	ModifyHomePage.ExecuteTemplate(w, "HomePage.html", data)
}

func EntireQueryDisplay(w http.ResponseWriter, r *http.Request) {
	results, err := GetEntireResultData()
	if err != nil {
		handleError(w, "Error getting Result", err, http.StatusInternalServerError)
		return
	}

	tmpl := ParseFileHTMLTemplates("templates/ResultPage.html")
	data := struct {
		Results []types.ResultData
	}{
		Results: results,
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		handleError(w, "Internal Server Error", err, http.StatusInternalServerError)
	}
}

func RefreshQuery(w http.ResponseWriter, r *http.Request) {
	tmpl := ParseFileHTMLTemplates("templates/QueryView.html")
	results, err := GetCountOfPeople()
	if err != nil {
		handleError(w, "Error getting Result", err, http.StatusInternalServerError)
		return
	}
	data := struct {
		Results []types.Count
	}{
		Results: results,
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		handleError(w, "Internal Server Error", err, http.StatusInternalServerError)
	}
}

func handleError(w http.ResponseWriter, message string, err error, statusCode int) {
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf(message, err))
	} else {
		logs.NewLog.Error(message)
	}
	http.Error(w, message, statusCode)
}

func GetCountOfPeople() ([]types.Count, error) {
	query := "SELECT COUNT(*) AS CountOfPeople FROM Contacts AS co " +
		"WHERE (JSONExtractString(co.Details, 'country') IN ('USA', 'UK')) " +
		"AND (co.ID IN (SELECT ContactsID " +
		"FROM dona_campaign.ContactActivity WHERE opened >= 30))"
	rows, err := services.GetQueryResultFromClickhouse(query)
	if err != nil {
		handleError(nil, "Error getting clickhouse Query", err, http.StatusInternalServerError)
		return nil, err
	}

	var results []types.Count
	for rows.Next() {
		var Count int
		rows.Scan(&Count)
		result := types.Count{
			Count: Count,
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		handleError(nil, fmt.Sprint(err), nil, http.StatusInternalServerError)
		return nil, err
	}
	return results, nil
}

func GetEntireResultData() ([]types.ResultData, error) {
	query := "SELECT co.ID, co.Email, JSONExtractString(co.Details, 'country') AS Country " +
		"FROM Contacts AS co " +
		"WHERE (JSONExtractString(co.Details, 'country') IN ('USA', 'UK')) " +
		"AND (co.ID IN (SELECT ContactsID " +
		"FROM dona_campaign.ContactActivity WHERE opened >= 30))"
	rows, err := services.GetQueryResultFromClickhouse(query)
	if err != nil {
		handleError(nil, "Error getting clickhouse Query Result", err, http.StatusInternalServerError)
		return nil, err
	}

	var results []types.ResultData
	for rows.Next() {
		var ID, Email, Country string
		err := rows.Scan(&ID, &Email, &Country)
		if err != nil {
			logs.NewLog.Info("Cannot create a struct for this user")
			continue
		}
		result := types.ResultData{
			ID:      ID,
			Email:   Email,
			Country: Country,
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		handleError(nil, fmt.Sprint(err), nil, http.StatusInternalServerError)
		return nil, err
	}
	return results, err
}
