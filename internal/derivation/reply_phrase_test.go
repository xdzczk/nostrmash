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
		if got.Total != 1 || got.Substantive != 1 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("repeated greetings are discounted together", func(t *testing.T) {
		rows := make([]weightedReply, 0, 18)
		for i := 0; i < 18; i++ {
			rows = append(rows, weightedReply{Pubkey: string(rune('a' + i)), Content: "  gm! ", Weight: 1})
		}
		got := sumDiscountedReplyWeights(rows)
		if !near(got.Total, 18*repeatedReplyPhraseDiscount) || got.Substantive != 0 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("punctuation urls and mentions normalize into the same greeting", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM https://example.com/coffee.jpg", Weight: 1},
			{Pubkey: "b", Content: "gm nostr:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqx", Weight: 1},
		})
		if !near(got.Total, 2*repeatedReplyPhraseDiscount) || got.Substantive != 0 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("greeting variants share one bucket", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM", Weight: 1},
			{Pubkey: "b", Content: "good morning", Weight: 1},
			{Pubkey: "c", Content: "GM Gigi", Weight: 1},
			{Pubkey: "d", Content: "PV", Weight: 1},
			{Pubkey: "e", Content: "gm & now is coffeetime!", Weight: 1},
			{Pubkey: "f", Content: "❤️", Weight: 1},
			{Pubkey: "g", Content: "hello", Weight: 1},
		})
		if !near(got.Total, 7*repeatedReplyPhraseDiscount) || got.Substantive != 0 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("one-line wishes join the greeting bucket", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM", Weight: 1},
			{Pubkey: "b", Content: "GM dear soul, enjoy your wonderful day...peace", Weight: 1},
			{Pubkey: "c", Content: "GM. No coffee no workee", Weight: 1},
			{Pubkey: "d", Content: "have a great day", Weight: 1},
			{Pubkey: "e", Content: "enjoy your wonderful day", Weight: 1},
			{Pubkey: "f", Content: "Mornin Boyo! Enjoy the unfolding...", Weight: 1},
		})
		if !near(got.Total, 6*repeatedReplyPhraseDiscount) || got.Substantive != 0 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("a second short wish does not clear a greeting", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "Gm", Weight: 1},
			{Pubkey: "a", Content: "Enjoy coffee have great day", Weight: 1},
			{Pubkey: "b", Content: "good morning", Weight: 1},
		})
		if !near(got.Total, 2*repeatedReplyPhraseDiscount) || got.Substantive != 0 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("a remark stays at full weight beside the pile", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM", Weight: 1},
			{Pubkey: "b", Content: "good morning", Weight: 1},
			{Pubkey: "c", Content: "Two espressos on that cracked stone ledge is how I would start a Friday too.", Weight: 1},
			{Pubkey: "d", Content: "GM Gigi. Sending you good vibes from this spot in space-time. Things have been very busy here too", Weight: 1},
			{Pubkey: "e", Content: "GM the binance hacker froze the funds", Weight: 1},
			{Pubkey: "f", Content: "Good morning Do a set of push ups, it will only take 1-2 mins, you'll feel better, don't trust verify", Weight: 1},
		})
		// Four remarks, two greetings at 0.2.
		want := 4 + 2*repeatedReplyPhraseDiscount
		if !near(got.Total, want) || got.Substantive != 4 {
			t.Fatalf("got %+v want total %v substantive 4", got, want)
		}
	})

	t.Run("distinct short replies stay at full weight", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "dup 1", Weight: 1},
			{Pubkey: "a", Content: "dup 2", Weight: 1},
			{Pubkey: "b", Content: "other", Weight: 1},
		})
		if got.Total != 2 || got.Substantive != 2 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("good point is not a greeting variant", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "good point", Weight: 1},
			{Pubkey: "b", Content: "GM", Weight: 1},
		})
		if got.Total != 2 || got.Substantive != 2 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("a real sentence alongside a greeting keeps full weight", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM", Weight: 1},
			{Pubkey: "a", Content: "This is a real comment about the note and its argument.", Weight: 1},
			{Pubkey: "b", Content: "GM", Weight: 1},
			{Pubkey: "c", Content: "GM", Weight: 1},
		})
		// a is substantive; b and c share the greeting bucket and are discounted.
		want := 1 + 2*repeatedReplyPhraseDiscount
		if !near(got.Total, want) || got.Substantive != 1 {
			t.Fatalf("got %+v want total %v substantive 1", got, want)
		}
	})

	t.Run("the same short word from several people is discounted", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "bitcoin", Weight: 1},
			{Pubkey: "b", Content: "Bitcoin!", Weight: 1},
			{Pubkey: "c", Content: "something else entirely about the protocol", Weight: 1},
		})
		want := 2*repeatedReplyPhraseDiscount + 1
		if !near(got.Total, want) || got.Substantive != 1 {
			t.Fatalf("got %+v want total %v substantive 1", got, want)
		}
		if _, ok := got.DiscountedPubkeys["a"]; !ok {
			t.Fatalf("copied reply missing from pile: %+v", got.DiscountedPubkeys)
		}
		if _, ok := got.DiscountedPubkeys["c"]; ok {
			t.Fatalf("remark joined the pile: %+v", got.DiscountedPubkeys)
		}
	})

	t.Run("near copies of a short reply share the pile", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "Bitcoin is money", Weight: 1},
			{Pubkey: "b", Content: "Bitcoin is the money", Weight: 1},
			{Pubkey: "c", Content: "biticoin is money", Weight: 1},
			{Pubkey: "d", Content: "bitcoin sound money", Weight: 1},
			{Pubkey: "e", Content: "bitcoin is real money", Weight: 1},
			{Pubkey: "f", Content: "Bitcoin is money of course it is the point of this", Weight: 1},
		})
		want := 5*repeatedReplyPhraseDiscount + 1
		if !near(got.Total, want) || got.Substantive != 1 {
			t.Fatalf("got %+v want total %v substantive 1", got, want)
		}
		if len(got.DiscountedPubkeys) != 5 {
			t.Fatalf("pile pubkeys = %+v", got.DiscountedPubkeys)
		}
	})

	t.Run("a second extra word stays out of the pile", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "Bitcoin is money", Weight: 1},
			{Pubkey: "b", Content: "Bitcoin is money", Weight: 1},
			{Pubkey: "c", Content: "Bitcoin is the money", Weight: 1},
			{Pubkey: "d", Content: "bitcoin is the real money", Weight: 1},
		})
		want := 3*repeatedReplyPhraseDiscount + 1
		if !near(got.Total, want) || got.Substantive != 1 {
			t.Fatalf("got %+v want total %v substantive 1", got, want)
		}
		if _, ok := got.DiscountedPubkeys["d"]; ok {
			t.Fatalf("two-word drift joined the pile: %+v", got.DiscountedPubkeys)
		}
	})

	t.Run("one short reply stays full weight", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "Bitcoin is the money", Weight: 1},
		})
		if got.Total != 1 || got.Substantive != 1 || len(got.DiscountedPubkeys) != 0 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("unrelated short replies do not share a pile", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "yes", Weight: 1},
			{Pubkey: "b", Content: "no", Weight: 1},
		})
		if got.Total != 2 || got.Substantive != 2 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("a translation of the slogan is its own pile", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "Bitcoin is money", Weight: 1},
			{Pubkey: "b", Content: "Bitcoin is money", Weight: 1},
			{Pubkey: "c", Content: "bitcoin é dinheiro", Weight: 1},
			{Pubkey: "d", Content: "bitcoin é dinheiro", Weight: 1},
		})
		want := 4 * repeatedReplyPhraseDiscount
		if !near(got.Total, want) || got.Substantive != 0 || len(got.DiscountedPubkeys) != 4 {
			t.Fatalf("got %+v want %v", got, want)
		}
	})

	t.Run("one person who also reposts still counts the repost", func(t *testing.T) {
		replies := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "Bitcoin is money", Weight: 1},
		})
		got := repostWeightOutsidePile([]weightedReply{{Pubkey: "a", Weight: 1}}, replies.DiscountedPubkeys)
		if got != 1 || replies.Substantive != 1 {
			t.Fatalf("repost %v replies %+v", got, replies)
		}
	})

	t.Run("a repost from a greeting pile is not a second vote", func(t *testing.T) {
		replies := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "GM", Weight: 1},
			{Pubkey: "b", Content: "good morning", Weight: 1},
			{Pubkey: "c", Content: "Two espressos on that cracked stone ledge is how I would start a Friday too.", Weight: 1},
		})
		got := repostWeightOutsidePile([]weightedReply{
			{Pubkey: "a", Weight: 1},
			{Pubkey: "b", Weight: 1},
			{Pubkey: "c", Weight: 1},
		}, replies.DiscountedPubkeys)
		if got != 1 || replies.Substantive != 1 {
			t.Fatalf("repost %v replies %+v", got, replies)
		}
	})

	t.Run("a repost from the pile is not a second vote", func(t *testing.T) {
		replies := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "Bitcoin is money", Weight: 1},
			{Pubkey: "b", Content: "Bitcoin is money", Weight: 1},
			{Pubkey: "c", Content: "This is a real comment about the note and its argument.", Weight: 1},
		})
		got := repostWeightOutsidePile([]weightedReply{
			{Pubkey: "a", Weight: 1},
			{Pubkey: "b", Weight: 0.5},
			{Pubkey: "c", Weight: 1},
			{Pubkey: "d", Weight: 1},
		}, replies.DiscountedPubkeys)
		if got != 2 {
			t.Fatalf("repost weight = %v, pile = %+v", got, replies.DiscountedPubkeys)
		}
	})

	t.Run("identical long replies stay at full weight", func(t *testing.T) {
		text := "I wrote this out because the longer argument is the part worth answering in detail."
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: text, Weight: 1},
			{Pubkey: "b", Content: text, Weight: 1},
		})
		if got.Total != 2 || got.Substantive != 2 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("emoji-only replies cluster", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "🔥🔥", Weight: 1},
			{Pubkey: "b", Content: "👋", Weight: 1},
		})
		if !near(got.Total, 2*repeatedReplyPhraseDiscount) || got.Substantive != 0 {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("trust weight is scaled after the phrase discount", func(t *testing.T) {
		got := sumDiscountedReplyWeights([]weightedReply{
			{Pubkey: "a", Content: "gn", Weight: 0.5},
			{Pubkey: "b", Content: "GN!", Weight: 0.25},
		})
		want := (0.5 + 0.25) * repeatedReplyPhraseDiscount
		if !near(got.Total, want) || got.Substantive != 0 {
			t.Fatalf("got %+v want %v", got, want)
		}
	})
}
