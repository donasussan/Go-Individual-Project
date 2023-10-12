package route

import (
	"datastream/api"
	"net/http"
)

func SetupRouter() {
	http.HandleFunc("/", api.HomePageHandler)
	http.HandleFunc("/upload", api.Upload)
	http.HandleFunc("/query", api.MultipleQueryView)
	http.HandleFunc("/result", api.DisplayTheQueryResult)
}
