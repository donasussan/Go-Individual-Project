package api

import (
	"datastream/config"
	"datastream/process"
	"datastream/types"
	"os"
	"testing"
)

func TestUpload(t *testing.T) {
	csvContent := "Dona, dona@example.com, jsondata"
	tmpFile, err := os.CreateTemp("", "test-*.csv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = tmpFile.WriteString(csvContent)
	if err != nil {
		t.Fatal(err)
	}
	tmpFilePath := tmpFile.Name()
	fileContent, err := os.ReadFile(tmpFilePath)
	if err != nil {
		t.Fatal(err)
	}
	expectedCSVContent := "Dona, dona@example.com, jsondata"
	if string(fileContent) != expectedCSVContent {
		t.Errorf("Expected CSV content: %s, but got: %s", expectedCSVContent, string(fileContent))
	}
	expectedContactsSlice := []types.Contacts{
		{
			Name:    "Dona",
			Email:   " dona@example.com",
			Details: " jsondata",
		},
	}
	actualContacts, _ := process.CSVReadToContactsStruct(tmpFilePath)
	expectedContactsSlice[0].ID = actualContacts[0].ID
	if len(expectedContactsSlice) != len(actualContacts) {
		t.Errorf("Expected contacts length: %d, but got: %d", len(expectedContactsSlice), len(actualContacts))
	}
	for i := range expectedContactsSlice {
		if i >= len(actualContacts) {
			t.Errorf("Expected contact at index %d, but actualContacts is shorter", i)
			continue
		}
		if expectedContactsSlice[i].Name != actualContacts[i].Name ||
			expectedContactsSlice[i].Email != actualContacts[i].Email ||
			expectedContactsSlice[i].Details != actualContacts[i].Details ||
			expectedContactsSlice[i].ID != actualContacts[i].ID {
			t.Errorf("Mismatch at index %d. Expected: %+v, but got: %+v", i, expectedContactsSlice[i], actualContacts[i])
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

type MockKafkaConsumer struct{}

func (m *MockKafkaConsumer) Close() {
}

func (m *MockKafkaConsumer) ConsumeMessage(topic string) chan string {
	return make(chan string)
}
func TestSendConsumerActivityToMySQL(t *testing.T) {
	mockKafkaConfig := config.KafkaConfig{
		Broker: []string{"localhost:9092"},
		Topic2: "test-topic",
	}

	config.NewKafkaConsumer = func(kc *config.KafkaConfig, topic string) (config.KafkaConsumer, error) {
		return &MockKafkaConsumer{}, nil
	}

	config.ConsumeMessage = func(c config.KafkaConsumer, topic string) chan string {
		msgChan := make(chan string, 1)
		msgChan <- "test-message"
		close(msgChan)
		return msgChan
	}

	defer func() {
		config.NewKafkaConsumer = config.NewKafkaConsumerFunc
		config.ConsumeMessage = config.ConsumeMessageFunc
	}()

	err := SendConsumerActivityToMySQL()
	if err != nil {
		t.Errorf("SendConsumerActivityToMySQL returned an error: %v", err)
	}
}
