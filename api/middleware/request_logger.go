package middleware

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/vova616/xxhash"
)

type requestLogger struct {
	buf *bytes.Buffer
}

func newRequestLogger() *requestLogger {
	return &requestLogger{
		buf: &bytes.Buffer{},
	}
}

func (r *requestLogger) write(format string, args ...interface{}) {
	fmt.Fprintf(r.buf, format, args...)
}

func (r *requestLogger) requestType(reqType string) *requestLogger {
	r.write("%s ", reqType)
	return r
}

func (r *requestLogger) request(request string) *requestLogger {
	urlsplit := strings.Split(request, "?")
	url := urlsplit[0]
	cs := strings.Split(url, "/")
	// write out base URL
	if len(cs) == 2 && cs[0] == "" && cs[1] == "" {
		r.write("/")
	} else {
		for _, c := range cs {
			if c != "" {
				r.write("/")
				r.write(c)
			}
		}
	}
	return r
}

func (r *requestLogger) params(request string) *requestLogger {
	urlsplit := strings.Split(request, "?")
	if len(urlsplit) > 1 {
		// hash query params
		r.write("?")
		hash := xxhash.Checksum32([]byte(urlsplit[1]))
		r.write("%#x ", hash)
	} else {
		r.buf.WriteString(" ")
	}
	return r
}

func (r *requestLogger) status(status int) *requestLogger {
	r.write("%03d", status)
	return r
}

func (r *requestLogger) duration(duration time.Duration) *requestLogger {
	r.buf.WriteString(" in ")
	r.write("%.2fms", duration.Seconds()*1000)
	return r
}

func (r *requestLogger) render() *bytes.Buffer {
	return r.buf
}
