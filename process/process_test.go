package process

import (
	"datastream/logs"
	"datastream/types"
	"testing"
	"time"
)

func TestValidateUploadedFileFormat(t *testing.T) {
	logs.InsForLogging()
	tempFile := "/home/user/go_learn/data_stream/sampledata/sample.csv"
	err := ValidateUploadedFileFormat(tempFile)
	if err != nil {
		t.Errorf("Expected nil error, but got: %v", err)
	}

	err = ValidateUploadedFileFormat("non_existent_file.csv")
	expectedErrMsg := "file does not exist: non_existent_file.csv"
	if err == nil || err.Error() != expectedErrMsg {
		t.Errorf("Expected error: %s, but got: %v", expectedErrMsg, err)
	}

	emptyFile := "/home/user/go_learn/data_stream/sampledata/nodata.csv"
	err = ValidateUploadedFileFormat(emptyFile)
	expectedErrMsg = "file is empty: /home/user/go_learn/data_stream/sampledata/nodata.csv"
	if err == nil || err.Error() != expectedErrMsg {
		t.Errorf("Expected error: %s, but got: %v", expectedErrMsg, err)
	}

	unsupportedFile := "/home/user/go_learn/data_stream/sampledata/image.png"
	err = ValidateUploadedFileFormat(unsupportedFile)
	expectedErrMsg = "unsupported file type, please upload .csv file"
	if err == nil || err.Error() != expectedErrMsg {
		t.Errorf("Expected error: %s, but got: %v", expectedErrMsg, err)
	}
}
func TestValidateCSVRecord(t *testing.T) {
	logs.InsForLogging()
	t.Run("ValidRecord", func(t *testing.T) {
		record := []string{"John Doe", "john@example.com", `{"dob": "1990-12-05", "city": "City2", "country": "Country2"}`}
		err := validateCSVRecord(1, record)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("InvalidNumberOfColumns", func(t *testing.T) {
		record := []string{"Name", "Email"}
		err := validateCSVRecord(2, record)
		if err == nil {
			t.Errorf("Expected an error, got nil")
		}
	})

	t.Run("InvalidName", func(t *testing.T) {
		record := []string{"", "john@example.com", `{"dob": "1990-12-05", "city": "City2", "country": "Country2"}`}
		err := validateCSVRecord(3, record)
		if err == nil {
			t.Errorf("Expected an error, got nil")
		}
	})

	t.Run("InvalidEmail", func(t *testing.T) {
		record := []string{"John Doe", "invalid-email", `{"dob": "1990-12-05", "city": "City2", "country": "Country2"}`}
		err := validateCSVRecord(4, record)
		if err == nil {
			t.Errorf("Expected an error, got nil")
		}
	})

	t.Run("InvalidDetails", func(t *testing.T) {
		record := []string{"John Doe", "john@example.com", ""}
		err := validateCSVRecord(5, record)
		if err == nil {
			t.Errorf("Expected an error, got nil")
		}
	})
}

func TestIsValidName(t *testing.T) {
	t.Run("ValidName", func(t *testing.T) {
		name := "John Doe"
		if !isValidName(name) {
			t.Errorf("Expected name to be valid, got invalid")
		}
	})

	t.Run("InvalidName", func(t *testing.T) {
		name := ""
		if isValidName(name) {
			t.Errorf("Expected name to be invalid, got valid")
		}
	})
}

func TestIsValidEmail(t *testing.T) {
	t.Run("ValidEmail", func(t *testing.T) {
		email := "john@example.com"
		if !isValidEmail(email) {
			t.Errorf("Expected email to be valid, got invalid")
		}
	})

	t.Run("InvalidEmail", func(t *testing.T) {
		email := "invalid-email"
		if isValidEmail(email) {
			t.Errorf("Expected email to be invalid, got valid")
		}
	})
}

func TestIsValidDetails(t *testing.T) {
	t.Run("ValidDetails", func(t *testing.T) {
		details := `{"dob": "1990-12-05", "city": "City2", "country": "Country2"}`
		if !isValidDetails(details) {
			t.Errorf("Expected details to be valid, got invalid")
		}
	})

	t.Run("InvalidDetails", func(t *testing.T) {
		details := ""
		if isValidDetails(details) {
			t.Errorf("Expected details to be invalid, got valid")
		}
	})
}

func TestGenerateActivity(t *testing.T) {
	p_id := "123"
	activityDate = time.Now()
	activityString = ""
	GenerateActivity(p_id)
	if activityString == "" {
		t.Error("ActivityString should not be empty")
	}
}

func TestSeparateContactActivities(t *testing.T) {
	activityDate := time.Date(2023, 10, 10, 0, 0, 0, 0, time.UTC)

	input := "(123, 1, 2, 2023-10-10)"
	numColumns := 4
	expectedOutput := []types.ContactActivity{
		{
			Contactid:    "123",
			Campaignid:   1,
			Activitytype: 2,
			Activitydate: activityDate,
		},
	}
	activities, _ := SeparateContactActivities(input, numColumns)

	for i := range expectedOutput {
		if activities[i] != expectedOutput[i] {
			t.Errorf("Mismatch at index %d: Expected %v, but got %v",
				i, expectedOutput[i], activities[i])
		}
	}
}
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
	activityDate := time.Date(2023, 10, 10, 0, 0, 0, 0, time.UTC)
	activities := []types.ContactActivity{
		{
			Contactid:    "123",
			Campaignid:   456,
			Activitytype: 1,
			Activitydate: activityDate,
		},
	}
	activityDetails := getActivityDetailsString(activities)
	expectedActivityDetails := `('123', 456, 1, '2023-10-10 00:00:00'),`
	if activityDetails != expectedActivityDetails {
		t.Errorf("Expected ActivityDetails: %s, but got: %s", expectedActivityDetails, activityDetails)
	}
}
