package main

import (
	"datastream/logs"
	"datastream/routes"
	"datastream/services"
	"net/http"
)

func main() {
	logs.InsForLogging()
	defer logs.CloseLog()
	services.NewKafkaHandler()

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	routes.SetupRouter()
	http.ListenAndServe(":8080", nil)
}
