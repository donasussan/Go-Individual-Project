package database

import "datastream/types"

type DataStore interface {
	InsertContact(contact types.Contacts) error
	GenerateActivity(types.ContactActivity) (types.ContactActivity, error)
}

type Kafka struct {
}
