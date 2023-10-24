package process

import (
	"datastream/logs"
	"datastream/types"
	"fmt"
	"testing"
	"time"
)

func TestValidateUploadedFileFormat(t *testing.T) {
	err := ValidateUploadedFileFormat("nonexistent.txt")
	if err == nil {
		t.Error("Expected error for nonexistent file, but got nil")
	}
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

	input := "(123, 1, 2, 2023-10-10 00:00:00 +0000 UTC)"
	numColumns := 4
	expectedOutput := []types.ContactActivity{
		{
			Contactid:    "123",
			Campaignid:   1,
			Activitytype: 2,
			Activitydate: activityDate,
		},
	}
	activities, err := SeparateContactActivities(input, numColumns)
	if err != nil {
		logs.NewLog.Errorf(fmt.Sprintf("Unexpected error: %v", err))
	}
	if len(activities) != len(expectedOutput) {
		logs.NewLog.Errorf(fmt.Sprintf("Expected %d elements, but got %d", len(expectedOutput), len(activities)))
		return
	}
	for i := range expectedOutput {
		if activities[i] != expectedOutput[i] {
			logs.NewLog.Errorf(fmt.Sprintf("Mismatch at index %d: Expected %v, but got %v",
				i, expectedOutput[i], activities[i]))
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
