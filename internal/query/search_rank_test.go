package query

import (
	"strings"
	"testing"
	"time"
)

func TestRankNotes_RecencyAndEngagementBeatStaleExactMatch(t *testing.T) {
	now := time.Date(2026, 10, 25, 16, 0, 0, 0, time.UTC)
	old := NoteScoreInput{
		ID:        "old",
		Pubkey:    "human-old",
		CreatedAt: now.Add(-2 * 365 * 24 * time.Hour).Unix(),
		Content:   "bitcoin bitcoin bitcoin bitcoin",
	}
	fresh := NoteScoreInput{
		ID:         "fresh",
		Pubkey:     "human-new",
		CreatedAt:  now.Add(-30 * time.Minute).Unix(),
		Content:    "bitcoin still works as peer to peer cash if you hold your own keys",
		ReplyCount: 3,
	}
	ranked := RankNotes("bitcoin", []NoteScoreInput{old, fresh}, RankOptions{Now: now})
	if len(ranked) != 2 {
		t.Fatalf("expected both notes, got %#v", ranked)
	}
	if ranked[0].ID != "fresh" {
		t.Fatalf("today's note with replies must outrank a 2-year-old note with zero engagement, got %#v", idsOf(ranked))
	}
	if ScoreNote("bitcoin", old, now) >= ScoreNote("bitcoin", fresh, now) {
		t.Fatalf("score(old)=%v score(fresh)=%v", ScoreNote("bitcoin", old, now), ScoreNote("bitcoin", fresh, now))
	}
}

func TestRankNotes_HashtagStubRanksBelowProse(t *testing.T) {
	now := time.Date(2026, 10, 25, 16, 0, 0, 0, time.UTC)
	stub := NoteScoreInput{
		ID:        "stub",
		Pubkey:    "p1",
		CreatedAt: now.Add(-time.Minute).Unix(),
		Content:   "#bitcoin",
	}
	oneWord := NoteScoreInput{
		ID:        "word",
		Pubkey:    "p2",
		CreatedAt: now.Add(-2 * time.Minute).Unix(),
		Content:   "Bitcoin",
	}
	prose := NoteScoreInput{
		ID:         "prose",
		Pubkey:     "p3",
		CreatedAt:  now.Add(-2 * time.Hour).Unix(),
		Content:    "bitcoin miners are debating fee policy again this week",
		ReplyCount: 2,
	}
	ranked := RankNotes("bitcoin", []NoteScoreInput{stub, oneWord, prose}, RankOptions{Now: now})
	if ranked[0].ID != "prose" {
		t.Fatalf("prose must lead, got %#v", idsOf(ranked))
	}
	if !IsQueryStub("#bitcoin", "bitcoin") || !IsQueryStub("Bitcoin", "bitcoin") {
		t.Fatal("expected hashtag-only and one-word query notes to be stubs")
	}
	if IsQueryStub(prose.Content, "bitcoin") {
		t.Fatal("prose must not be a stub")
	}
}

func TestRankNotes_BotAndTickerDemotion(t *testing.T) {
	now := time.Date(2026, 10, 25, 16, 0, 0, 0, time.UTC)
	human := NoteScoreInput{
		ID:         "human",
		Pubkey:     "alice",
		CreatedAt:  now.Add(-3 * time.Hour).Unix(),
		Content:    "bitcoin looks quiet today but the mempool is clearing",
		ReplyCount: 1,
	}
	bot := NoteScoreInput{
		ID:         "bot",
		Pubkey:     "pricebot",
		CreatedAt:  now.Add(-time.Minute).Unix(),
		Content:    "Bitcoin price: $81123, Sats per USD: 1233",
		AuthorBot:  true,
		ReplyCount: 4,
	}
	ticker := NoteScoreInput{
		ID:        "ticker",
		Pubkey:    "ticker",
		CreatedAt: now.Add(-2 * time.Minute).Unix(),
		Content:   "Bitcoin price: $81100, Sats per USD: 1234",
	}
	blockArt := NoteScoreInput{
		ID:                "art",
		Pubkey:            "artist",
		CreatedAt:         now.Add(-time.Minute).Unix(),
		Content:           "Block Art bitcoin █▀▄░",
		AuthorName:        "blocks",
		AuthorDisplayName: "Block Art",
	}
	rss := NoteScoreInput{
		ID:          "rss",
		Pubkey:      "feed",
		CreatedAt:   now.Add(-time.Minute).Unix(),
		Content:     "bitcoin headline from the mirror",
		AuthorName:  "Example (RSS Feed)",
		AuthorNIP05: "news@mostr.pub",
	}
	ranked := RankNotes("bitcoin", []NoteScoreInput{bot, ticker, blockArt, rss, human}, RankOptions{Now: now})
	if ranked[0].ID != "human" {
		t.Fatalf("human prose must outrank bots and automated feeds, got %#v", idsOf(ranked))
	}
}

func TestRankNotes_AuthorCapOnFirstPage(t *testing.T) {
	now := time.Date(2026, 10, 25, 16, 0, 0, 0, time.UTC)
	notes := make([]NoteScoreInput, 0, 40)
	for i := 0; i < 20; i++ {
		notes = append(notes, NoteScoreInput{
			ID:         "spam-" + string(rune('a'+i)),
			Pubkey:     "spammer",
			CreatedAt:  now.Add(-time.Duration(i) * time.Minute).Unix(),
			Content:    "bitcoin thread " + string(rune('a'+i)) + " from one account about fees policy and mining",
			ReplyCount: int64(30 - i),
		})
	}
	for i := 0; i < 18; i++ {
		pubkey := "human-" + string(rune('a'+i))
		notes = append(notes, NoteScoreInput{
			ID:        pubkey,
			Pubkey:    pubkey,
			CreatedAt: now.Add(-time.Duration(40+i) * time.Minute).Unix(),
			Content:   "bitcoin note " + pubkey + " about fees policy and self custody",
		})
	}
	ranked := RankNotes("bitcoin", notes, RankOptions{Now: now})
	window := ranked
	if len(window) > NoteAuthorCapWindow {
		window = window[:NoteAuthorCapWindow]
	}
	count := 0
	for _, note := range window {
		if note.Pubkey == "spammer" {
			count++
		}
	}
	if count > NoteAuthorCap {
		t.Fatalf("expected at most %d notes from one author in the top %d, got %d", NoteAuthorCap, NoteAuthorCapWindow, count)
	}
}

func TestRankNotes_NearDuplicateTickerCollapses(t *testing.T) {
	now := time.Date(2026, 10, 25, 16, 0, 0, 0, time.UTC)
	notes := []NoteScoreInput{
		{ID: "p1", Pubkey: "bot", CreatedAt: now.Add(-time.Minute).Unix(), Content: "Bitcoin price: $81123, Sats per USD: 1233"},
		{ID: "p2", Pubkey: "bot", CreatedAt: now.Add(-2 * time.Minute).Unix(), Content: "Bitcoin price: $81100, Sats per USD: 1234"},
		{ID: "p3", Pubkey: "bot", CreatedAt: now.Add(-3 * time.Minute).Unix(), Content: "Bitcoin price: $81050, Sats per USD: 1235"},
		{ID: "human", Pubkey: "alice", CreatedAt: now.Add(-4 * time.Hour).Unix(), Content: "bitcoin fees dropped after the weekend rush", ReplyCount: 2},
	}
	ranked := RankNotes("bitcoin", notes, RankOptions{Now: now})
	if ranked[0].ID != "human" {
		t.Fatalf("duplicate price ticks must not lead, got %#v", idsOf(ranked))
	}
}

func TestRankProfiles_ExactNameBeforeFollowersAndBotsLast(t *testing.T) {
	now := time.Date(2026, 10, 25, 16, 0, 0, 0, time.UTC)
	profiles := []ProfileScoreInput{
		{Pubkey: "partial", Name: "jackal", DisplayName: "jackalope", About: "not jack", FollowerCount: 5_000_000, RecentActivityAt: now.Unix()},
		{Pubkey: "url", Name: "wss://relay.damus.io:1234", FollowerCount: 9_000_000, RecentActivityAt: now.Unix()},
		{Pubkey: "hex", Name: strings.Repeat("ab", 32), FollowerCount: 10},
		{Pubkey: "bot", Name: "jack", DisplayName: "jack", Bot: true, FollowerCount: 8_000_000, RecentActivityAt: now.Unix()},
		{Pubkey: "small", Name: "jack", DisplayName: "Jack", NIP05: "jack@example.com", FollowerCount: 12, RecentActivityAt: now.Add(-2 * time.Hour).Unix()},
		{Pubkey: "popular", Name: "jack", DisplayName: "Jack", NIP05: "jack@cash.app", FollowerCount: 1_000_000, RecentActivityAt: now.Add(-time.Hour).Unix()},
	}
	ranked := RankProfiles("jack", profiles, RankOptions{Now: now})
	if ranked[0].Pubkey != "popular" {
		t.Fatalf("exact name with followers must lead, got %#v", profileIDs(ranked))
	}
	if ranked[1].Pubkey != "small" {
		t.Fatalf("the other exact jack must stay ahead of non-exact and bots, got %#v", profileIDs(ranked))
	}
	var botAt, urlAt, hexAt int
	for i, profile := range ranked {
		switch profile.Pubkey {
		case "bot":
			botAt = i
		case "url":
			urlAt = i
		case "hex":
			hexAt = i
		}
	}
	if botAt < 2 {
		t.Fatalf("bots must rank after human exact matches, got %#v", profileIDs(ranked))
	}
	if urlAt < botAt || hexAt < botAt {
		t.Fatalf("url and hex names rank last, got %#v", profileIDs(ranked))
	}
}

func TestParseSearchQuery_PhraseOperator(t *testing.T) {
	parsed := ParseSearchQuery(`"not your keys" coins`)
	if len(parsed.Phrases) != 1 || parsed.Phrases[0] != "not your keys" {
		t.Fatalf("unexpected phrases: %#v", parsed)
	}
	if !ContentMatchesPhrases("Not Your Keys, not your coins", parsed.Phrases) {
		t.Fatal("phrase match must ignore case and not require the quote characters")
	}
	if ContentMatchesPhrases("not your coins and keys", parsed.Phrases) {
		t.Fatal("reordered words must not satisfy the phrase operator")
	}
	now := time.Date(2026, 10, 25, 16, 0, 0, 0, time.UTC)
	ranked := RankNotes(`"not your keys"`, []NoteScoreInput{
		{ID: "miss", Pubkey: "a", CreatedAt: now.Unix(), Content: "not your coins and keys"},
		{ID: "hit", Pubkey: "b", CreatedAt: now.Add(-time.Hour).Unix(), Content: "repeat after me: not your keys"},
	}, RankOptions{Now: now})
	if len(ranked) != 1 || ranked[0].ID != "hit" {
		t.Fatalf("phrase filter dropped or kept the wrong note: %#v", idsOf(ranked))
	}
}

func TestChooseCorrection_BitconiToBitcoin(t *testing.T) {
	lexicon := []TermFrequency{
		{Term: "bitconize", DocumentFrequency: 4},
		{Term: "bitcoin", DocumentFrequency: 134211},
		{Term: "bitcoiner", DocumentFrequency: 800},
	}
	corrected, ok := ChooseCorrection("bitconi", 1, true, lexicon)
	if !ok || corrected != "bitcoin" {
		t.Fatalf("expected bitcoin correction, got %q ok=%v", corrected, ok)
	}
	if _, ok := ChooseCorrection("bitcoin", 134211, false, lexicon); ok {
		t.Fatal("a strong raw query must not be rewritten")
	}
	if _, ok := ChooseCorrection("bitconi", 500, true, lexicon); ok {
		t.Fatal("a high-frequency raw token must not be rewritten")
	}
	neighbors := Edits("bitconi", 1)
	found := false
	for _, neighbor := range neighbors {
		if neighbor == "bitcoin" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("edit distance 1 of bitconi must include bitcoin")
	}
}

func TestCanonicalRelayHost(t *testing.T) {
	host, ok := CanonicalRelayHost("wss://relay.damus.io:1234")
	if !ok || host != "relay.damus.io" {
		t.Fatalf("url host: %q ok=%v", host, ok)
	}
	host, ok = CanonicalRelayHost("relay.damus.io")
	if !ok || host != "relay.damus.io" {
		t.Fatalf("bare host: %q ok=%v", host, ok)
	}
	if _, ok := CanonicalRelayHost("jack"); ok {
		t.Fatal("a person name is not a relay host")
	}
	if _, ok := CanonicalRelayHost("jack@example.com"); ok {
		t.Fatal("a nip05 is not a relay host")
	}
}

func TestQueryResultsWeak_PrefixOnlyHit(t *testing.T) {
	if !QueryResultsWeak("bitconi", []string{"Bitconize launched"}, 3) {
		t.Fatal("prefix-only hits are weak")
	}
	if QueryResultsWeak("bitcoin", []string{"bitcoin is a protocol"}, 1000) {
		t.Fatal("a whole-word high-frequency hit is not weak")
	}
}

func idsOf(notes []NoteScoreInput) []string {
	out := make([]string, len(notes))
	for i, note := range notes {
		out[i] = note.ID
	}
	return out
}

func profileIDs(profiles []ProfileScoreInput) []string {
	out := make([]string, len(profiles))
	for i, profile := range profiles {
		out[i] = profile.Pubkey
	}
	return out
}
