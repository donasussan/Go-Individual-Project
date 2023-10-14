package process

import (
	"testing"
)

func TestGetQueryResultFromClickhouse(t *testing.T) {
	results, err := GetQueryResultFromClickhouse()
	if err != nil {
		t.Errorf("Expected no error, but got an error: %v", err)
	}
	expectedCountries := []string{"USA", "UK"}
	for _, result := range results {
		matchFound := false
		for _, expectedCountry := range expectedCountries {
			if result.Country == expectedCountry {
				matchFound = true
				break
			}
		}
		if !matchFound {
			t.Errorf("Country field does not match expected values in result: %s", result.Country)
		}
	}
}
