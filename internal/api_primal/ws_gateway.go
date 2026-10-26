package api_primal

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/relaypublish"
	"github.com/xdzczk/nostrmash/internal/store/failure"
)

type WSGateway struct {
	query    query.Service
	upgrader websocket.Upgrader
	opts     WSGatewayOptions
	log      *slog.Logger
}

const (
	// WSDialectLegacy keeps the original /primal/ws cache shapes.
	WSDialectLegacy = ""
	// WSDialectPrimalV1 speaks the cache protocol the Primal iOS app expects.
	WSDialectPrimalV1 = "primal_v1"
)

// CanonicalPersister stores a validated event so the author can read it
// back from this cache before relay fanout finishes.
type CanonicalPersister interface {
	PersistCanonical(ctx context.Context, event model.Event, tags [][]string, relayURL string) error
}

type WSGatewayOptions struct {
	MaxSubscriptions      int
	RequestTimeout        time.Duration
	MaxMessageBytes       int64
	MaxReqPerMinute       int
	MaxDMReqPerMinute     int
	MaxBroadcastPerMinute int
	AllowedOrigins        []string
	AllowAnyOrigin        bool
	Logger                *slog.Logger
	QueryOptions          query.ServiceOptions
	Dialect               string
	ServerPubkey          string
	DefaultRelays         []string
	DefaultSettings       json.RawMessage
	ClientConfig          json.RawMessage
	RecommendedUsers      []string
	BlossomServers        []string
	MediaFetchEnabled     bool
	Persister             CanonicalPersister
	Publisher             *relaypublish.Client
}

type dmLiveSubscription struct {
	SubID    string
	Kind     string
	Receiver string
	Sender   string
}

type notifyLiveSubscription struct {
	SubID   string
	Pubkey  string
	Version int
}

type wsQueuedFrame struct {
	binary  bool
	payload []byte
}

type wsFrameHandler func(*wsConnSession, string, []any) error

const (
	wsFrameREQ   = "REQ"
	wsFrameClose = "CLOSE"

	primalKindNoteStats       = 10000100
	primalKindNetStats        = 10000101
	primalKindDefaultSettings = 10000103
	primalKindUserStats       = 10000105
	primalKindMentions        = 10000107
	primalKindUserScore       = 10000108
	primalKindNotification    = 10000110
	primalKindSeenTimestamp   = 10000111
	primalKindNotifStats      = 10000112
	primalKindRange           = 10000113
	primalKindNoteActions     = 10000115
	primalKindDirectMsgCount  = 10000117
	primalKindDirectMsgCounts = 10000118
	primalKindMediaMetadata   = 10000119
	primalKindDefaultRelays   = 10000124
	primalKindFollowingUser   = 10000125
	primalKindWebPreview      = 10000128
	primalKindPostZaps        = 10000129
	primalKindNotifStats2     = 10000132
	primalKindUserFollowers   = 10000133
	primalKindDirectMsgCount2 = 10000134
	primalKindFilteringReason = 10000131
	primalKindHiddenByContent = 10000137
	primalKindUserPubkey      = 10000138
	primalKindRelays          = 10000139
	primalKindRelayHints      = 10000141
	primalKindLongFormMeta    = 10000144
	primalKindRecommendedRead = 10000145
	primalKindReadsTopics     = 10000146
	primalKindCreatorPaidTier = 10000147
	primalKindFeaturedAuthors = 10000148
	primalKindBroadcastResult = 10000149
	primalKindSubsettings     = 10000155
	primalKindPopularHashtags = 10000160
	primalKindClientConfig    = 10000162
	primalKindNIP19Decode     = 10000173
	primalKindBlossomServers  = 10000175
	primalKindPremiumState    = 100001603
	parameterizedListKind     = 30000
	appSettingsKind           = 30078
)

// wsFrameHandlers is the single registration point for supported WS frame kinds.
var wsFrameHandlers = map[string]wsFrameHandler{
	wsFrameREQ:   handleWSFrameREQ,
	wsFrameClose: handleWSFrameClose,
}

func handleWSFrameREQ(s *wsConnSession, subID string, args []any) error {
	return s.handleREQFrame(subID, args)
}

func handleWSFrameClose(s *wsConnSession, subID string, _ []any) error {
	s.handleCLOSEFrame(subID)
	return nil
}

func NewWSGateway(reader EventReader, opts WSGatewayOptions) (WSGateway, error) {
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
	if opts.MaxBroadcastPerMinute <= 0 {
		opts.MaxBroadcastPerMinute = 30
	}
	if len(opts.DefaultSettings) == 0 {
		opts.DefaultSettings = defaultSettingsJSON
	}
	if len(opts.ClientConfig) == 0 {
		opts.ClientConfig = json.RawMessage("null")
	}
	wsLog := opts.Logger
	if wsLog == nil {
		wsLog = logging.New("api_primal_ws")
	}
	service, err := query.NewServiceFromStoreReader(reader, opts.QueryOptions)
	if err != nil {
		return WSGateway{}, err
	}
	return WSGateway{
		query: service,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return checkOrigin(r, opts) },
		},
		opts: opts,
		log:  wsLog,
	}, nil
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
