package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatsPprofLoopbackOnly(t *testing.T) {
	t.Parallel()
	tests := []struct {
		listen string
		pprof  int
	}{
		{"127.0.0.1:9464", http.StatusOK},
		{"[::1]:9464", http.StatusOK},
		{"0.0.0.0:9464", http.StatusNotFound},
		{"[::]:9464", http.StatusNotFound},
		{"bad", http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.listen, func(t *testing.T) {
			t.Parallel()
			m := newMeter()
			h := statsHTTPHandler(m.statsHandler(func() LinkState { return LinkUp }), tc.listen)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", http.NoBody))
			if rec.Code != http.StatusOK {
				t.Fatalf("/stats status = %d, want 200", rec.Code)
			}

			recPprof := httptest.NewRecorder()
			h.ServeHTTP(recPprof, httptest.NewRequest(http.MethodGet, "/debug/pprof/", http.NoBody))
			if recPprof.Code != tc.pprof {
				t.Fatalf("/debug/pprof/ status = %d, want %d", recPprof.Code, tc.pprof)
			}

			if tc.pprof == http.StatusOK {
				for _, path := range []string{"/debug/pprof/heap", "/debug/pprof/goroutine?debug=1"} {
					r := httptest.NewRecorder()
					h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, http.NoBody))
					if r.Code != http.StatusOK {
						t.Fatalf("%s status = %d, want 200", path, r.Code)
					}
				}
			}
		})
	}
}
