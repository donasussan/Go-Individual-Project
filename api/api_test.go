package api

import (
	"datastream/types"
	"testing"
)

// func TestUpload(t *testing.T) {
// 	csvContent := "Dona, dona@example.com, jsondata"
// 	tmpFile, err := os.CreateTemp("", "test-*.csv")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	_, err = tmpFile.WriteString(csvContent)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	tmpFilePath := tmpFile.Name()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	expectedContactsSlice := []types.Contacts{
// 		{
// 			Name:    "Dona",
// 			Email:   " dona@example.com",
// 			Details: " jsondata",
// 		},
// 	}
// 	actualContacts, _ := process.CSVReadToContactsStruct(tmpFilePath)
// 	expectedContactsSlice[0].ID = actualContacts[0].ID
// 	if len(expectedContactsSlice) != len(actualContacts) {
// 		t.Errorf("Expected contacts length: %d, but got: %d", len(expectedContactsSlice), len(actualContacts))
// 	}
// 	for i := range expectedContactsSlice {
// 		if i >= len(actualContacts) {
// 			t.Errorf("Expected contact at index %d, but actualContacts is shorter", i)
// 			continue
// 		}
// 		if expectedContactsSlice[i].Name != actualContacts[i].Name ||
// 			expectedContactsSlice[i].Email != actualContacts[i].Email ||
// 			expectedContactsSlice[i].Details != actualContacts[i].Details ||
// 			expectedContactsSlice[i].ID != actualContacts[i].ID {
// 			t.Errorf("Mismatch at index %d. Expected: %+v, but got: %+v", i, expectedContactsSlice[i], actualContacts[i])
// 		}
// 	}
// }

func TestGetContactsDataString(t *testing.T) {
	statusContact := types.ContactStatus{
		Contact: types.Contacts{
			ID:      "123",
			Name:    "Dona",
			Email:   "dona@example.com",
			Details: "jsondata",
		},
		Status: 1,
	}
	contactsData, _ := getContactsDataString(statusContact)
	expectedContactsData := "('123', 'Dona','dona@example.com', 'jsondata', 1),"
	if contactsData != expectedContactsData {
		t.Errorf("Expected ContactsData: %s, but got: %s", expectedContactsData, contactsData)
	}
}

func TestGetActivityDetailsString(t *testing.T) {
	activities := []types.ContactActivity{
		{
			Contactid:    "123",
			Campaignid:   456,
			Activitytype: 1,
			Activitydate: " 2023-10-10",
		},
	}
	activityDetails := getActivityDetailsString(activities)
	expectedActivityDetails := `('123', 456, 1, 2023-10-10),`
	if activityDetails != expectedActivityDetails {
		t.Errorf("Expected ActivityDetails: %s, but got: %s", expectedActivityDetails, activityDetails)
	}
}

// type MockQueryResultGetter struct{}

// func (m *MockQueryResultGetter) GetQueryResultFromClickhouse() ([]config.ResultData, error) {
// 	return []config.ResultData{
// 		{ID: "1", Email: "test1@example.com", Country: "USA"},
// 		{ID: "2", Email: "test2@example.com", Country: "UK"},
// 	}, nil
// }

// func TestDisplayTheQueryResult(t *testing.T) {
// 	req, err := http.NewRequest("GET", "/your-endpoint", nil)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	rr := httptest.NewRecorder()
// 	DisplayTheQueryResult(rr, req)

// 	if status := rr.Code; status != http.StatusOK {
// 		t.Errorf("Handler returned wrong status code: got %v, want %v", status, http.StatusOK)
// 	}
// 	expectedResponse := "Expected HTML content for the results page"
// 	actualResponse := rr.Body.String()

// 	if expectedResponse != actualResponse {
// 		t.Errorf("Expected response: %s\nActual response: %s", expectedResponse, actualResponse)
// 	}
// }
