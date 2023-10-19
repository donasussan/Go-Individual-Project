package process

import (
	"datastream/logs"
	"datastream/types"
	"fmt"
	"testing"
	"time"
)

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
	input := "(123, 1, 2, 2023-10-14),(456, 2, 3, 2023-10-15)"
	numColumns := 4
	expectedOutput := []types.ContactActivity{
		{
			Contactid:    "123",
			Campaignid:   1,
			Activitytype: 2,
			Activitydate: "2023-10-14",
		},
		{
			Contactid:    "456",
			Campaignid:   2,
			Activitytype: 3,
			Activitydate: "2023-10-15",
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

//	func TestReturnContactsAndActivitiesStructs(t *testing.T) {
//		testID := "testID"
//		status, activities, err := ReturnContactsAndActivitiesStructs(testID, types.Contacts{})
//		if err != nil {
//			t.Errorf("ReturnContactsAndActivitiesStructs returned an error: %v", err)
//		}
//		if status.Contact.ID != testID {
//			t.Errorf("Expected Contact.ID to be %s, got %s", testID, status.Contact.ID)
//		}
//		if len(activities) == 0 {
//			t.Error("Expected at least one activity, but none were returned")
//		}
//	}
