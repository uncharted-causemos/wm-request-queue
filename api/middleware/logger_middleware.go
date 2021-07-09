package middleware

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/middleware"
	"go.uber.org/zap"
)

// Logger creates a middleware wrapper around a zap Sugared logger that logs
// HTTP requests.
func Logger(l *zap.SugaredLogger) func(next http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			lw := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			t1 := time.Now()
			h.ServeHTTP(lw, r)
			if lw.Status() == 0 {
				lw.WriteHeader(http.StatusOK)
			}
			logString := newRequestLogger().
				requestType(r.Method).
				request(r.URL.String()).
				params(r.URL.String()).
				status(lw.Status()).
				duration(time.Since(t1)).
				render()
			if lw.Status() < 500 {
				l.Info(logString)
			} else {
				l.Warn(logString)
			}
		}
		return http.HandlerFunc(fn)
	}
}
