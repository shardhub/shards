package v1

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi"
	"go.uber.org/zap"

	"github.com/shardhub/shards/services/librarian"
)

const (
	RFC3339Milli = "2006-01-02T15:04:05.999Z07:00"
)

type API struct {
	librarian *librarian.Librarian

	mux    *chi.Mux
	logger *zap.Logger
}

func New(librarian *librarian.Librarian, logger *zap.Logger) *API {
	api := &API{
		librarian: librarian,

		mux:    chi.NewMux(),
		logger: logger,
	}

	api.mux.Route("/databases", func(r chi.Router) {
		r.Get("/", api.databasesListHandler)

		r.Route("/{name:[A-Za-z0-9-_]+}", func(r chi.Router) {
			r.Route("/dbs", func(r chi.Router) {
				r.Post("/", api.dbCreateHandler)
			})
		})
	})

	return api
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

func (a *API) databasesListHandler(w http.ResponseWriter, r *http.Request) {
	databases := a.librarian.Databases()

	type responseData struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}

	type response struct {
		Data []responseData `json:"data"`
	}

	result := response{
		Data: make([]responseData, 0, len(databases)),
	}
	for _, db := range databases {
		result.Data = append(result.Data, responseData{
			Type: "databases",
			ID:   db,
		})
	}

	b, err := json.Marshal(&result)
	if err != nil {
		// TODO
		a.logger.Error("Cannot marshal reponse", zap.Error(err))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.api+json")
	if _, err := w.Write(b); err != nil {
		// TODO
		a.logger.Error("Cannot write response", zap.Error(err))
	}
}

func (a *API) dbCreateHandler(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	database := a.librarian.Get(name)
	if database == nil {
		// TODO
		http.Error(w, "", http.StatusNotFound)
		return
	}

	// TODO
	res, err := database.Create(r.Context())
	if err != nil {
		// TODO
		a.logger.Error("Cannot create DB", zap.Error(err))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	type responseAttributes struct {
		Database  string  `json:"database"`
		Username  string  `json:"username"`
		Password  string  `json:"password"`
		ExpiredAt *string `json:"expiredAt"`
	}

	type responseData struct {
		Type       string             `json:"type"`
		ID         string             `json:"id"`
		Attributes responseAttributes `json:"attributes"`
	}

	type response struct {
		Data responseData `json:"data"`
	}

	var expiredAt *string
	if res.ExpiredAt != nil {
		v := res.ExpiredAt.Format(RFC3339Milli)

		expiredAt = &v
	}

	result := response{
		Data: responseData{
			Type: "dbs",
			ID:   res.Database + "_" + res.Username,
			Attributes: responseAttributes{
				Database:  res.Database,
				Username:  res.Username,
				Password:  res.Password,
				ExpiredAt: expiredAt,
			},
		},
	}

	b, err := json.Marshal(&result)
	if err != nil {
		// TODO
		a.logger.Error("Cannot marshal reponse", zap.Error(err))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.api+json")
	w.WriteHeader(http.StatusCreated)
	if _, err := w.Write(b); err != nil {
		// TODO
		a.logger.Error("Cannot write response", zap.Error(err))
	}
}
