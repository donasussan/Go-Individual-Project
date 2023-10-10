package types

import (
	"fmt"
)

type Contacts struct {
	Name    string
	Email   string
	Details string
	ID      string
}
type ContactActivity struct {
	Activitydate string
	Contactid    string
	Campaignid   int
	Activitytype int
}

type ContactStatus struct {
	Contact Contacts
	Status  int
}

type QueryOutput struct {
}

func (ca *ContactActivity) StringConv() string {
	return fmt.Sprintf("Contactid: %s, Campaignid: %d, Activitytype: %d, Activitydate: %s",
		ca.Contactid, ca.Campaignid, ca.Activitytype, ca.Activitydate)
}
