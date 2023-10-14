package services

import (
	"datastream/logs"
	"fmt"
	"testing"
)

func TestGetQueryResultFromClickhouse(t *testing.T) {
	results, err := GetQueryResultFromClickhouse()
	if err != nil {
		logs.NewLog.Error(fmt.Sprintf("Expected no error, but got an error: %v", err))
	}
	expectedCountries := []string{"USA", "UK"}
	if len(results) != len(expectedCountries) {
		logs.NewLog.Errorf(fmt.Sprintf("Expected %d results, but got %d", len(expectedCountries), len(results)))
	}
	for i, expectedCountry := range expectedCountries {
		if results[i].Country != expectedCountry {
			logs.NewLog.Error(fmt.Sprintf("Result at index %d: Expected country '%s', but got '%s'", i, expectedCountry, results[i].Country))
		}
	}
}
