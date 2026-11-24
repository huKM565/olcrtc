package server

import (
	"net"
	"net/http"
	"net/http/pprof"
	"net/netip"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

func statsHTTPHandler(stats http.Handler, listen string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/stats", stats)
	host, _, err := net.SplitHostPort(listen)
	if err == nil {
		if addr, parseErr := netip.ParseAddr(host); parseErr == nil && addr.IsLoopback() {
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			return mux
		}
	}
	logger.Warnf("stats: pprof disabled for non-loopback listener %q", listen)
	return mux
}
