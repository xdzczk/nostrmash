package derivation

import (
	"math"
	"testing"
)

func near(got, want float64) bool {
	return math.Abs(got-want) < 1e-9
}

func TestSumDiscountedReplyWeights(t *testing.T) {
	t.Run("one greeting is full weight", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM", Weight: 1},
		})
		if got != 1 {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("repeated greetings are discounted together", func(t *testing.T) {
		rows := make([]weightedReply, 0, 18)
		for i := 0; i < 18; i++ {
			rows = append(rows, weightedReply{Pubkey: string(rune('a' + i)), Content: "  gm! ", Weight: 1})
		}
		got := sumDiscountedReplyWeights(rows)
		if !near(got, 18*repeatedReplyPhraseDiscount) {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("punctuation urls and mentions normalize into the same greeting", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM https://example.com/coffee.jpg", Weight: 1},
			{Pubkey: "b", Content: "gm nostr:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqx", Weight: 1},
		})
		if !near(got, 2*repeatedReplyPhraseDiscount) {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("distinct short replies stay at full weight", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "dup 1", Weight: 1},
			{Pubkey: "a", Content: "dup 2", Weight: 1},
			{Pubkey: "b", Content: "other", Weight: 1},
		})
		if got != 2 {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("a real sentence alongside a greeting keeps full weight", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM", Weight: 1},
			{Pubkey: "a", Content: "This is a real comment about the note and its argument.", Weight: 1},
			{Pubkey: "b", Content: "GM", Weight: 1},
			{Pubkey: "c", Content: "GM", Weight: 1},
		})
		// a is substantive; b and c share "gm" and are discounted.
		want := 1 + 2*repeatedReplyPhraseDiscount
		if !near(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})

	t.Run("the same short word from several people is discounted", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "bitcoin", Weight: 1},
			{Pubkey: "b", Content: "Bitcoin!", Weight: 1},
			{Pubkey: "c", Content: "something else entirely about the protocol", Weight: 1},
		})
		want := 2*repeatedReplyPhraseDiscount + 1
		if !near(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})

	t.Run("identical long replies stay at full weight", func(t *testing.T) {
		text := "I wrote this out because the longer argument is the part worth answering in detail."
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: text, Weight: 1},
			{Pubkey: "b", Content: text, Weight: 1},
		})
		if got != 2 {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("emoji-only replies cluster", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "🔥🔥", Weight: 1},
			{Pubkey: "b", Content: "👋", Weight: 1},
		})
		if !near(got, 2*repeatedReplyPhraseDiscount) {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("trust weight is scaled after the phrase discount", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "gn", Weight: 0.5},
			{Pubkey: "b", Content: "GN!", Weight: 0.25},
		})
		want := (0.5 + 0.25) * repeatedReplyPhraseDiscount
		if !near(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})
}
