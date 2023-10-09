package router

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/cors"

	infra "github.com/diwise/service-chassis/pkg/infrastructure/router"
)

func New(_ string) infra.Router {
	r := chi.NewRouter()

	r.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	return &impl{r: r}
}

type impl struct {
	r *chi.Mux
}

func (r *impl) Router() *chi.Mux {
	return r.r
}
