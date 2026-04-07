package api_primal

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store/failure"
)

type WSGateway struct {
	query    query.Service
	upgrader websocket.Upgrader
	opts     WSGatewayOptions
	log      *slog.Logger
}

type WSGatewayOptions struct {
	MaxSubscriptions  int
	RequestTimeout    time.Duration
	MaxMessageBytes   int64
	MaxReqPerMinute   int
	MaxDMReqPerMinute int
	AllowedOrigins    []string
	AllowAnyOrigin    bool
	Logger            *slog.Logger
	QueryOptions      query.ServiceOptions
}

type dmLiveSubscription struct {
	SubID    string
	Kind     string
	Receiver string
	Sender   string
}

const (
	primalKindRange           = 10000113
	primalKindDirectMsgCount  = 10000117
	primalKindDirectMsgCounts = 10000118
	primalKindDirectMsgCount2 = 10000134
	primalKindFilteringReason = 10000131
	primalKindHiddenByContent = 10000137
	primalKindUserPubkey      = 10000138
	primalKindRecommendedRead = 10000145
	primalKindReadsTopics     = 10000146
	primalKindCreatorPaidTier = 10000147
	primalKindFeaturedAuthors = 10000148
	parameterizedListKind     = 30000
)

func NewWSGateway(reader EventReader, opts WSGatewayOptions) WSGateway {
	if opts.MaxSubscriptions <= 0 {
		opts.MaxSubscriptions = 200
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 10 * time.Second
	}
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = 1 << 20 // 1 MiB
	}
	if opts.MaxReqPerMinute <= 0 {
		opts.MaxReqPerMinute = 240
	}
	if opts.MaxDMReqPerMinute <= 0 {
		opts.MaxDMReqPerMinute = 30
	}
	wsLog := opts.Logger
	if wsLog == nil {
		wsLog = logging.New("api_primal_ws")
	}
	return WSGateway{
		query: query.NewServiceWithOptions(reader, opts.QueryOptions),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return checkOrigin(r, opts) },
		},
		opts: opts,
		log:  wsLog,
	}
}

func (g WSGateway) Handle(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err := failure.FromPanic(recovered)
			class := failure.ClassifyError(err)
			g.log.Error("compat_ws_panic_recovered", "failure_class", class.Class, "failure_reason", class.Reason, "error", err)
		}
	}()
	conn, err := g.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	g.runWS(conn, r)
}
