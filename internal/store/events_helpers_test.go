package store

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

func TestDBResultFromErr(t *testing.T) {
	t.Run("nil error reports ok", func(t *testing.T) {
		if got := dbResultFromErr(nil); got != "ok" {
			t.Fatalf("dbResultFromErr(nil) = %q, want %q", got, "ok")
		}
	})

	t.Run("not found maps distinctly", func(t *testing.T) {
		if got := dbResultFromErr(ErrNotFound); got != "not_found" {
			t.Fatalf("dbResultFromErr(ErrNotFound) = %q, want %q", got, "not_found")
		}
	})

	t.Run("other errors report generic failure", func(t *testing.T) {
		if got := dbResultFromErr(errors.New("boom")); got != "error" {
			t.Fatalf("dbResultFromErr(other) = %q, want %q", got, "error")
		}
	})
}

func TestExpandEventTags(t *testing.T) {
	got := ExpandEventTags("evt_1", 1, [][]string{
		{"e", "target_1", "wss://relay.one", "reply"},
		{},
		{"p", "author_1"},
		{"client", "damus"},
		{"client"},
	})

	want := []model.EventTag{
		{
			EventID:    "evt_1",
			TagName:    "e",
			TagIndex:   0,
			ValueIndex: 0,
			Value:      "target_1",
		},
		{
			EventID:    "evt_1",
			TagName:    "e",
			TagIndex:   0,
			ValueIndex: 1,
			Value:      "wss://relay.one",
		},
		{
			EventID:    "evt_1",
			TagName:    "e",
			TagIndex:   0,
			ValueIndex: 2,
			Value:      "reply",
		},
		{
			EventID:    "evt_1",
			TagName:    "p",
			TagIndex:   2,
			ValueIndex: 0,
			Value:      "author_1",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExpandEventTags() mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestExpandEventTags_KindScopedFilters(t *testing.T) {
	contact := ExpandEventTags("c1", 3, [][]string{
		{"p", "bob"},
		{"p", "carol"},
		{"d", "ignored-on-kind3-but-allowed"},
	})
	if len(contact) != 1 || contact[0].TagName != "d" {
		t.Fatalf("kind-3 expand should keep only d-tag, got %#v", contact)
	}

	relays := ExpandEventTags("r1", 10002, [][]string{
		{"r", "wss://relay.one"},
		{"r", "wss://relay.two"},
		{"p", "hint_pub"},
	})
	if len(relays) != 1 || relays[0].TagName != "p" {
		t.Fatalf("kind-10002 expand should drop r-tags, got %#v", relays)
	}
}

func TestOpenPool(t *testing.T) {
	t.Run("rejects invalid dsn", func(t *testing.T) {
		pool, err := OpenPool(context.Background(), "not a postgres dsn", 0)
		if err == nil {
			if pool != nil {
				pool.Close()
			}
			t.Fatal("expected invalid DSN to fail")
		}
	})

	t.Run("connects to configured test database", func(t *testing.T) {
		dbURL := dbtest.DatabaseURL(t, "store")
		pool, err := OpenPool(context.Background(), dbURL, 0)
		if err != nil {
			t.Fatalf("OpenPool(test DB): %v", err)
		}
		pool.Close()
	})
}
