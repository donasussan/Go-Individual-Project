package api

import (
	"bytes"
	"datastream/logs"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHomePageHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(HomePageHandler)
	handler.ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v, want %v", status, http.StatusOK)
	}
}

func TestRefreshQuery(t *testing.T) {
	t.Run("Successful execution", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/refreshQuery", nil)
		w := httptest.NewRecorder()
		RefreshQuery(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

	})
}
func TestHandleFileUpload_Success(t *testing.T) {
	logs.InsForLogging()

	requestBody := &bytes.Buffer{}
	writer := multipart.NewWriter(requestBody)
	part, _ := writer.CreateFormFile("uploadedfile", "/home/user/go_learn/data_stream/sampledata/sample.csv")
	fileContent := []byte("name,email,details\nDona,dona@gmail.com,\"{\"\"dob\"\": \"\"1990-12-05\"\", \"\"city\"\": \"\"City2\"\", \"\"country\"\": \"\"Country2\"\"}\"\n")
	part.Write(fileContent)
	writer.Close()

	req, err := http.NewRequest("POST", "/upload", requestBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder()

	HandleFileUpload(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Errorf("Expected status code %d, got %d", http.StatusSeeOther, rr.Code)
	}

}
func TestHandleFileUpload_EmptyFile(t *testing.T) {
	logs.InsForLogging()
	requestBody := &bytes.Buffer{}
	writer := multipart.NewWriter(requestBody)
	part, _ := writer.CreateFormFile("uploadedfile", "/home/user/go_learn/data_stream/sampledata/nodata.csv")
	fileContent := []byte("")
	part.Write(fileContent)
	writer.Close()
	req, err := http.NewRequest("POST", "/upload", requestBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rr := httptest.NewRecorder()
	HandleFileUpload(rr, req)
	expectedErrorMessage := "file is empty"
	if !strings.Contains(rr.Body.String(), expectedErrorMessage) {
		t.Errorf("Expected error message '%s' not found in the response body", expectedErrorMessage)
	}
}
func TestHandleFileUpload_InvalidFileName(t *testing.T) {
	logs.InsForLogging()

	requestBody := &bytes.Buffer{}
	writer := multipart.NewWriter(requestBody)
	part, _ := writer.CreateFormFile("uploadedfile", "inva!id.csv")
	fileContent := []byte("name,email,details\nDona,dona@gmail.com,\"{\"\"dob\"\": \"\"1990-12-05\"\", \"\"city\"\": \"\"City2\"\", \"\"country\"\": \"\"Country2\"\"}\"\n")
	part.Write(fileContent)
	writer.Close()

	req, err := http.NewRequest("POST", "/upload", requestBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder()

	HandleFileUpload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, rr.Code)
	}
}
func TestEntireQueryDisplay(t *testing.T) {
	req := httptest.NewRequest("GET", "/path", nil)

	rr := httptest.NewRecorder()
	EntireQueryDisplay(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status %d, but got %d", http.StatusOK, rr.Code)
	}

}
