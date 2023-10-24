package api

import (
	"net/http"
	"net/http/httptest"
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
func TestQueryView(t *testing.T) {
	t.Run("Successful execution", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/query", nil)
		w := httptest.NewRecorder()
		QueryView(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

	})
	t.Run("Error executing HTML template", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/query", nil)
		w := httptest.NewRecorder()
		QueryView(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status code %d, got %d", http.StatusInternalServerError, w.Code)
		}

	})
}

// func TestRefreshQuery(t *testing.T) {
// 	t.Run("Successful execution", func(t *testing.T) {
// 		req := httptest.NewRequest(http.MethodGet, "/refreshQuery", nil)
// 		w := httptest.NewRecorder()
// 		RefreshQuery(w, req)

// 		if w.Code != http.StatusOK {
// 			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
// 		}

// 	})
// }

// func TestUploadFile(t *testing.T) {
// 	csvContent := `dona,dona@gmail.com,{"country":"india","city":"kerala","dob":"29-09-2001"}`

// 	body := &bytes.Buffer{}
// 	writer := multipart.NewWriter(body)
// 	part, _ := writer.CreateFormFile("file", "test.csv")
// 	part.Write([]byte(csvContent))
// 	writer.Close()
// 	req := httptest.NewRequest("POST", "/upload", body)
// 	req.Header.Set("Content-Type", writer.FormDataContentType())

// 	// Test case 1: Successful upload of a CSV file
// 	t.Run("SuccessfulUpload", func(t *testing.T) {
// 		recorder := httptest.NewRecorder()
// 		handler, file, err := HandleFileUpload(recorder, req)

// 		if err != nil {
// 			t.Errorf("Expected no error, got %v", err)
// 		}
// 		if handler == nil {
// 			t.Errorf("Expected a file handler, got nil")
// 		}
// 		if file == nil {
// 			t.Errorf("Expected a file reader, got nil")
// 		}
// 		if !strings.HasSuffix(handler.Filename, ".csv") {
// 			t.Errorf("Expected file to have a .csv extension, got %s", handler.Filename)
// 		}
// 	})

// 	// Test case 2: Missing file in the request
// 	t.Run("MissingFile", func(t *testing.T) {
// 		body := &bytes.Buffer{}
// 		writer := multipart.NewWriter(body)
// 		req := httptest.NewRequest("POST", "/upload", body)          // Include an empty file
// 		req.Header.Set("Content-Type", writer.FormDataContentType()) // Set the correct content type
// 		recorder := httptest.NewRecorder()

// 		handler, file, err := HandleFileUpload(recorder, req)

// 		if err == nil {
// 			t.Errorf("Expected error, but got no error: %v", err)
// 		}
// 		if handler != nil {
// 			t.Errorf("Expected a nil file handler, got %v", handler)
// 		}
// 		if file != nil {
// 			t.Errorf("Expected a nil file reader, got %v", file)
// 		}
// 	})

// 	// Test case 3: Uploaded file is not a CSV
// 	t.Run("NonCSVFile", func(t *testing.T) {
// 		recorder := httptest.NewRecorder()

// 		// Create a sample request with a non-CSV file
// 		nonCSVContent := "This is not a CSV content"
// 		body := &bytes.Buffer{}
// 		writer := multipart.NewWriter(body)
// 		part, _ := writer.CreateFormFile("file", "test.txt")
// 		part.Write([]byte(nonCSVContent))
// 		writer.Close()

// 		req := httptest.NewRequest("POST", "/upload", body)
// 		req.Header.Set("Content-Type", writer.FormDataContentType())

// 		_, _, err := HandleFileUpload(recorder, req)

// 		if err == nil {
// 			t.Errorf("Expected an error, but got no error")
// 		}
// 		if !strings.Contains(err.Error(), "File is not a CSV") {
// 			t.Errorf("Expected error message to contain 'File is not a CSV', got %v", err)
// 		}
// 	})
// }
