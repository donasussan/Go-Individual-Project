package main

import (
	"datastream/logs"
	"datastream/routes"
	"net/http"
)

func main() {
	logs.InsForLogging()
	defer logs.CloseLog()
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	routes.SetupRouter()
	http.ListenAndServe(":8080", nil)
}
