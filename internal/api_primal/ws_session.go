package api_primal

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

func checkOrigin(r *http.Request, opts WSGatewayOptions) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		// Non-browser clients commonly omit Origin.
		return true
	}
	if opts.AllowAnyOrigin {
		return true
	}
	if len(opts.AllowedOrigins) == 0 {
		return false
	}
	parsedOrigin, err := url.Parse(origin)
	if err != nil || parsedOrigin.Scheme == "" || parsedOrigin.Host == "" {
		return false
	}
	normalizedOrigin := parsedOrigin.Scheme + "://" + parsedOrigin.Host
	for _, allowed := range opts.AllowedOrigins {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		if strings.EqualFold(allowed, normalizedOrigin) {
			return true
		}
	}
	return false
}

func (g WSGateway) runWS(conn *websocket.Conn, r *http.Request) {
	defer conn.Close()
	metrics.IncPrimalWSConnection()
	defer metrics.DecPrimalWSConnection()
	conn.SetReadLimit(g.opts.MaxMessageBytes)
	remoteAddr := conn.RemoteAddr().String()
	g.log.Info("compat_ws_connected", "remote_addr", remoteAddr)
	defer g.log.Info("compat_ws_disconnected", "remote_addr", remoteAddr)
	session := newWSConnSession(g, conn, r.Context(), remoteAddr)
	session.run()
}
