package routes

import (
	"datastream/api"
	"net/http"
)

func SetupRouter() {
	http.HandleFunc("/", api.HomePageHandler)
	http.HandleFunc("/upload", api.HandleFileUpload)
	http.HandleFunc("/result", api.EntireQueryDisplay)
	http.HandleFunc("/refreshQuery", api.RefreshQuery)
}
