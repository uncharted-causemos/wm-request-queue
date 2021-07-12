package api

import (
	"compress/flate"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/render"
	api_middleware "gitlab.uncharted.software/WM/wm-request-queue/api/middleware"
	"gitlab.uncharted.software/WM/wm-request-queue/api/routes"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// NewRouter returns a chi router with endpoints registered.
func NewRouter(cfg config.Config) (chi.Router, error) {

	// Setup the router and configure baseline middleware
	r := chi.NewRouter()

	r.Use(api_middleware.Logger(cfg.Logger))
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Compress(flate.DefaultCompression))

	// Configure CORS handling
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		AllowCredentials: true,
	})
	r.Use(c.Handler)

	r.Route("/data-pipeline", func(r chi.Router) {
		r.Use(render.SetContentType(render.ContentTypeJSON))
		r.Put("/enqueue", routes.EnqueueRequest(&cfg)) // PUT instead of POST due to idempotency
		r.Get("/waiting", routes.Waiting(&cfg))
	})

	return r, nil
}
