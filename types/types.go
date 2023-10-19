package types

import "time"

type Contacts struct {
	Name    string
	Email   string
	Details string
	ID      string
}
type ContactActivity struct {
	Activitydate time.Time
	Contactid    string
	Campaignid   int
	Activitytype int
}

type ContactStatus struct {
	Contact Contacts
	Status  int
}

type ResultData struct {
	ID      string `json:"ID"`
	Email   string `json:"Email"`
	Country string `json:"Country"`
}
type Count struct {
	Count int
}
