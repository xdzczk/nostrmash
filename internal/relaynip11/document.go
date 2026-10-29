package relaynip11

import (
	"encoding/json"
	"time"
)

// StoredSummary is the JSON object written to relay_registry.capability_summary_json.
// ok false means the last fetch failed and info is absent. ok true with an
// empty info object means the relay published a document with no retained fields.
type StoredSummary struct {
	FetchedAt time.Time `json:"fetched_at"`
	OK        bool      `json:"ok"`
	Error     string    `json:"error,omitempty"`
	Info      *Info     `json:"info,omitempty"`
}

// Info is the whitelist of NIP-11 fields retained for relay pages.
// Values are what the relay claimed, not what a probe measured.
type Info struct {
	Name          string      `json:"name,omitempty"`
	Description   string      `json:"description,omitempty"`
	Icon          string      `json:"icon,omitempty"`
	Banner        string      `json:"banner,omitempty"`
	Pubkey        string      `json:"pubkey,omitempty"`
	Contact       string      `json:"contact,omitempty"`
	SupportedNIPs []int       `json:"supported_nips,omitempty"`
	Software      string      `json:"software,omitempty"`
	Version       string      `json:"version,omitempty"`
	Limitation    *Limitation `json:"limitation,omitempty"`
	Fees          *Fees       `json:"fees,omitempty"`
	Retention     []Retention `json:"retention,omitempty"`
}

// Limitation is the NIP-11 limitation object, restricted to known keys.
// Boolean pointers are nil when the relay omitted the field.
type Limitation struct {
	MaxMessageLength    *int64 `json:"max_message_length,omitempty"`
	MaxSubscriptions    *int64 `json:"max_subscriptions,omitempty"`
	MaxFilters          *int64 `json:"max_filters,omitempty"`
	MaxLimit            *int64 `json:"max_limit,omitempty"`
	MaxSubidLength      *int64 `json:"max_subid_length,omitempty"`
	MaxEventTags        *int64 `json:"max_event_tags,omitempty"`
	MaxContentLength    *int64 `json:"max_content_length,omitempty"`
	MinPowDifficulty    *int64 `json:"min_pow_difficulty,omitempty"`
	AuthRequired        *bool  `json:"auth_required,omitempty"`
	PaymentRequired     *bool  `json:"payment_required,omitempty"`
	RestrictedWrites    *bool  `json:"restricted_writes,omitempty"`
	CreatedAtLowerLimit *int64 `json:"created_at_lower_limit,omitempty"`
	CreatedAtUpperLimit *int64 `json:"created_at_upper_limit,omitempty"`
}

// Fees is the NIP-11 fee schedule.
type Fees struct {
	Admission    []Fee `json:"admission,omitempty"`
	Subscription []Fee `json:"subscription,omitempty"`
	Publication  []Fee `json:"publication,omitempty"`
}

// Fee is one NIP-11 fee entry.
type Fee struct {
	Amount int64  `json:"amount"`
	Unit   string `json:"unit"`
	Period *int64 `json:"period,omitempty"`
	Kinds  []int  `json:"kinds,omitempty"`
}

// Retention is one NIP-11 retention rule. Kinds may mix kind numbers and
// two-element ranges, matching the relay document.
type Retention struct {
	Kinds json.RawMessage `json:"kinds,omitempty"`
	Time  *int64          `json:"time,omitempty"`
	Count *int64          `json:"count,omitempty"`
}
