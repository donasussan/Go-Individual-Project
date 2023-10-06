package main

import (
	route "datastream/routes"

	"datastream/logs"
	"net/http"
)

func main() {
	logs.InsForLogging()
	defer logs.CloseLog()
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))
	route.SetupRouter()
	http.ListenAndServe(":8080", nil)

}
