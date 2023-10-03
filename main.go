package main

import (
	route "datastream/routes"

	"net/http"
)

func main() {

	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))
	route.SetupRouter()
	http.ListenAndServe(":8080", nil)

}
