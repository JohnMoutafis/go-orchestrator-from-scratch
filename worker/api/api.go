package workerApi

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"cube/worker"
)

type Api struct {
	Address string
	Port    int
	Worker  *worker.Worker
	// Mux > multiplexer == request router
	Router *chi.Mux
}

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

// Server
func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Get("/", a.GetTasksHandler)
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
	a.Router.Route("/stats", func(r chi.Router) {
		r.Get("/", a.GetStatsHandler)
	})
}

func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}
