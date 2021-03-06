package api

import (
	"compress/flate"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/render"
	api_middleware "gitlab.uncharted.software/WM/wm-request-queue/api/middleware"
	"gitlab.uncharted.software/WM/wm-request-queue/api/pipeline"
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"gitlab.uncharted.software/WM/wm-request-queue/api/routes"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

// NewRouter returns a chi router with endpoints registered.
func NewRouter(cfg config.Config, queue queue.RequestQueue, runner *pipeline.DataPipelineRunner) (chi.Router, error) {

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
		r.Put("/enqueue", routes.EnqueueRequest(&cfg, queue)) // PUT instead of POST due to idempotency
		r.Put("/bulk-enqueue", routes.BulkEnqueueRequest(&cfg, queue))
		r.Get("/status", routes.StatusRequest(&cfg, queue, runner))
		r.Put("/start", routes.StartRequest(&cfg, runner))
		r.Put("/stop", routes.StopRequest(&cfg, runner))
		r.Put("/clear", routes.ClearRequest(&cfg, queue))
		r.Put("/force-flow", routes.ForceDispatchRequest(&cfg, queue, runner))
		r.Get("/jobs", routes.JobsRequest(&cfg, queue))
		r.Put("/retry-flow/{run_id}", routes.RetryFlowRequest(&cfg, queue, runner))
	})

	return r, nil
}
