package api

import (
	"datastream/logs"
	"datastream/process"
	"datastream/services"
	"datastream/types"
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

func ParsefileHTMLtemplates(htmlpage string) *template.Template {
	tmpl, err := template.ParseFiles(htmlpage)
	if err != nil {
		return nil
	}
	return tmpl
}

func HomePageHandler(w http.ResponseWriter, r *http.Request) {
	tmpl := ParsefileHTMLtemplates("templates/HomePage.html")
	err := tmpl.Execute(w, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func HandleFileUpload(w http.ResponseWriter, r *http.Request) {
	doneChan := make(chan struct{})

	uploadedFile, header, err := r.FormFile("uploadedfile")

	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error retrieving file: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	if header.Filename == "" {
		err := fmt.Errorf("invalid file name")
		logs.NewLog.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer uploadedFile.Close()

	fileNameWithUUID := fmt.Sprintf("%s-%s", uuid.New(), header.Filename)
	filePath := filepath.Join("/home/user/go_learn/data_stream/uploadfiles", fileNameWithUUID)
	outputFile, err := os.Create(filePath)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error creating the file: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer outputFile.Close()

	written, err := io.Copy(outputFile, uploadedFile)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error copying file content: %v", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if written != header.Size {
		logs.NewLog.Error("Data copy verification failed: Number of bytes copied does not match the original file size")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	} else {
		logs.NewLog.Info(fmt.Sprintf("Copied %d bytes", written))
	}
	err = process.ValidateUploadedFileFormat(filePath)
	if err != nil {
		data := struct {
			Error string
		}{
			Error: err.Error(),
		}
		ModifyHomePage.ExecuteTemplate(w, "HomePage.html", data)
		return
	}
	go func() {
		err := process.CSVReadToDataInsertion(filePath, 100, doneChan)
		if err != nil {
			logs.NewLog.Error(fmt.Sprintf("Error processing uploaded file: %v", err))
		}
	}()
	select {
	case <-doneChan:
		http.Redirect(w, r, "/HomePage.html?success=File+uploaded+successfully", http.StatusSeeOther)
	case <-time.After(5 * time.Second):
		logs.NewLog.Error("Processing took too long.")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func EntireQueryDisplay(w http.ResponseWriter, r *http.Request) {
	results, err := GetEntireResultData()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error getting Result%v", http.StatusInternalServerError))
		return
	}
	tmpl := ParsefileHTMLtemplates("templates/ResultPage.html")
	data := struct {
		Results []types.ResultData
	}{
		Results: results,
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Internal Server Error %v", http.StatusInternalServerError))
	}
}

func RefreshQuery(w http.ResponseWriter, r *http.Request) {
	tmpl := ParsefileHTMLtemplates("templates/QueryView.html")
	results, err := GetCountOfPeople()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Error getting Result %v", http.StatusInternalServerError))
		return
	}
	data := struct {
		Results []types.Count
	}{
		Results: results,
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Internal Server Error %v", http.StatusInternalServerError))
	}

}

func GetCountOfPeople() ([]types.Count, error) {
	query := "SELECT COUNT(*) AS CountOfPeople FROM Contacts AS co " +
		"WHERE (JSONExtractString(co.Details, 'country') IN ('USA', 'UK')) " +
		"AND (co.ID IN (SELECT ContactsID " +
		"FROM dona_campaign.ContactActivity WHERE opened >= 30))"
	rows, err := services.GetQueryResultFromClickhouse(query)
	if err != nil {
		logs.NewLog.Error(fmt.Sprint("Error getting clickhouse Query", err))
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
		logs.NewLog.Errorf(fmt.Sprint(err))
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
		logs.NewLog.Error(fmt.Sprint("Error getting clickhouse Query Result", err))
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
		logs.NewLog.Errorf(fmt.Sprint(err))
		return nil, err
	}
	return results, err
}
