package routes

import (
	"datastream/api"
	"net/http"
)

func SetupRouter() {
	http.HandleFunc("/", api.HomePageHandler)
	http.HandleFunc("/upload", api.Upload)
	http.HandleFunc("/query", api.QueryView)
	http.HandleFunc("/result", api.EntireQueryDisplay)
	http.HandleFunc("/refreshQuery", api.RefreshQuery)
}
