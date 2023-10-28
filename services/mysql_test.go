package services

import (
	"datastream/logs"
	"testing"
)

func TestInsertDataToSQL(t *testing.T) {
	logs.InsForLogging()

	testCases := []struct {
		name      string
		messages  []string
		query     string
		expectErr bool
	}{
		{
			name: "Successful insertion",
			messages: []string{
				"('d1d7033fae6341ab94dcabe01ee9d2cc7f69235ee08acfec', 86, 1, '2023-09-01 00:00:00')",
			},
			query:     "INSERT INTO test_ContactActivity (ContactsID, CampaignID, ActivityType, ActivityDate) VALUES",
			expectErr: false,
		},
		{
			name:      "Error during query execution",
			messages:  []string{"message1", "message2"},
			query:     "INSERT INTO ContactActivity (ContactsID, CampaignID, ActivityType, ActivityDate) VALUES",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectErr {
				err := InsertDataToMySQL(tc.messages, tc.query)
				if err == nil {
					t.Errorf("Expected an error but got none")
				}
			} else {
				err := InsertDataToMySQL(tc.messages, tc.query)
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}

		})
	}
}
