package main

import (
	"datastream/logs"
	"datastream/routes"
	"net/http"
)

func main() {
	logs.InsForLogging()
	defer logs.CloseLog()
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))
	routes.SetupRouter()
	http.ListenAndServe(":8080", nil)
}
