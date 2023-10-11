package route

import (
	"datastream/api"
	"net/http"
)

func SetupRouter() {
	http.HandleFunc("/", api.HomePageHandler)
	http.HandleFunc("/upload", api.Upload)
	http.HandleFunc("/Result", api.QueryView)
	http.HandleFunc("/query", api.DisplayTheQueryResult)
}
