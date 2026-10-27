package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	contactListCrawlQueueDepth   *prometheus.GaugeVec
	contactListCrawlCyclesTotal  *prometheus.CounterVec
	contactListCrawlPubkeysTotal *prometheus.CounterVec
	contactListCrawlTagsEnqueued prometheus.Counter
)

func registerContactListCrawlMetrics() {
	contactListCrawlQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_contact_list_crawl_queue_depth",
			Help: "Contact-list crawl rows that are due to be fetched, by hop.",
		},
		[]string{"hop"},
	)
	contactListCrawlCyclesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_contact_list_crawl_cycles_total",
			Help: "Contact-list crawl cycles by outcome.",
		},
		[]string{"outcome"},
	)
	contactListCrawlPubkeysTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_contact_list_crawl_pubkeys_total",
			Help: "Contact-list crawl pubkey fetches by outcome.",
		},
		[]string{"outcome"},
	)
	contactListCrawlTagsEnqueued = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "nostrmash_contact_list_crawl_tags_enqueued_total",
			Help: "Follow-list p-tags enqueued for a later contact-list crawl hop.",
		},
	)
	registry.MustRegister(
		contactListCrawlQueueDepth,
		contactListCrawlCyclesTotal,
		contactListCrawlPubkeysTotal,
		contactListCrawlTagsEnqueued,
	)
}

func SetContactListCrawlQueueDepth(hop string, count float64) {
	ensureRegistered()
	if count < 0 {
		count = 0
	}
	contactListCrawlQueueDepth.WithLabelValues(hop).Set(count)
}

func IncContactListCrawlCycle(outcome string) {
	ensureRegistered()
	contactListCrawlCyclesTotal.WithLabelValues(outcome).Inc()
}

func IncContactListCrawlPubkey(outcome string) {
	ensureRegistered()
	contactListCrawlPubkeysTotal.WithLabelValues(outcome).Inc()
}

func AddContactListCrawlTagsEnqueued(count float64) {
	ensureRegistered()
	if count <= 0 {
		return
	}
	contactListCrawlTagsEnqueued.Add(count)
}
