package store

type NetworkStats struct {
	Events   int64 `json:"events"`
	Profiles int64 `json:"profiles"`
	Relays   int64 `json:"relays"`
}

type CuratedRecommendedRead struct {
	EventID string `json:"event_id"`
	Title   string `json:"title"`
	URL     string `json:"url"`
	Rank    int    `json:"rank"`
}

type CuratedReadsTopic struct {
	Topic string `json:"topic"`
	Rank  int    `json:"rank"`
}

type CuratedFeaturedAuthor struct {
	Pubkey string `json:"pubkey"`
	Rank   int    `json:"rank"`
}
