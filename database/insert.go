package database

import "datastream/types"

// KafkaProducer is an interface that defines the methods a Kafka producer should implement.
type DataStore interface {
	InsertContact(contact types.Contacts) error
	GenerateActivity(contact types.ContactActivity) (types.ContactActivity, error)
}
