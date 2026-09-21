package query

import "encoding/json"

type AuthorZapsResult struct {
	Pubkey      string            `json:"pubkey"`
	Zaps        []json.RawMessage `json:"zaps"`
	NextCursor  *EventCursor      `json:"-"`
	Consistency string            `json:"consistency"`
}

type AuthorReactionsResult struct {
	Pubkey      string            `json:"pubkey"`
	Reactions   []json.RawMessage `json:"reactions"`
	NextCursor  *EventCursor      `json:"-"`
	Consistency string            `json:"consistency"`
}

type AuthorEventsResult struct {
	Pubkey      string            `json:"pubkey"`
	Events      []json.RawMessage `json:"events"`
	NextCursor  *EventCursor      `json:"-"`
	Consistency string            `json:"consistency"`
}
