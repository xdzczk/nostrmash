package derivation

import "math"

// profileVote is one engager's trust weight and which discovery windows it
// falls in. Rising 30d is every loaded vote: the query is already cut at 30d.
type profileVote struct {
	Pubkey      string
	Content     string
	Weight      float64
	EventAt     int64
	Trending24h bool
	Trending7d  bool
	Trending30d bool
	Rising24h   bool
	Rising7d    bool
}

// profileDiscountedEngagement is the farm-discounted numerator for each
// profile score. Display counters stay on the raw path.
type profileDiscountedEngagement struct {
	trending24h float64
	trending7d  float64
	trending30d float64
	rising24h   float64
	rising7d    float64
	rising30d   float64
}

// discountProfileVotes applies the note farm discount to profile votes.
// Trending keeps one vote per signal per person. Rising keeps one person,
// classified by their strongest signal. A copied phrase collapses to one
// vote and does not open the like budget. A media-only reply counts as
// that file. A greeting pile counts
// sqrt(people) times its strongest weight. Zaps open likes and repost
// slots even when a pile is most of the replies. Surplus reactions count
// at the cheap floor, and surplus reposts at the lower reach floor.
func discountProfileVotes(replies, reactions, reposts, zaps []profileVote) profileDiscountedEngagement {
	trending := func(keep func(profileVote) bool) float64 {
		return profileTrendingEngagement(votesIn(replies, keep), votesIn(reactions, keep), votesIn(reposts, keep), votesIn(zaps, keep))
	}
	rising := func(keep func(profileVote) bool) float64 {
		return profileRisingEngagement(votesIn(replies, keep), votesIn(reactions, keep), votesIn(reposts, keep), votesIn(zaps, keep))
	}
	return profileDiscountedEngagement{
		trending24h: trending(trending24h),
		trending7d:  trending(trending7d),
		trending30d: trending(trending30d),
		rising24h:   rising(rising24h),
		rising7d:    rising(rising7d),
		rising30d:   rising(rising30d),
	}
}

func trending24h(v profileVote) bool { return v.Trending24h }
func trending7d(v profileVote) bool  { return v.Trending7d }
func trending30d(v profileVote) bool { return v.Trending30d }
func rising24h(v profileVote) bool   { return v.Rising24h }
func rising7d(v profileVote) bool    { return v.Rising7d }
func rising30d(profileVote) bool     { return true }

func votesIn(rows []profileVote, keep func(profileVote) bool) []weightedReply {
	out := make([]weightedReply, 0, len(rows))
	for _, row := range rows {
		if row.Pubkey == "" || !keep(row) {
			continue
		}
		out = append(out, weightedReply{Pubkey: row.Pubkey, Content: row.Content, Weight: row.Weight})
	}
	return out
}

// profileTrendingEngagement scores one vote per signal per person. Replies
// pass through the phrase pile. A repost from someone in that pile is the
// same vote. Reactions and reposts use the note unlock: the reply share
// scales only the reply-derived part, and a zap still opens both.
func profileTrendingEngagement(replies, reactions, reposts, zaps []weightedReply) float64 {
	folded := sumDiscountedReplyWeights(replies)
	zapWeight := sumDistinctWeights(zaps)
	reactionWeight := sumDistinctWeights(reactions)
	repostWeight := repostWeightOutsidePile(reposts, folded.DiscountedPubkeys)
	unlockUnits := folded.Substantive*folded.UnlockShare + zapWeight
	total := folded.Total + zapWeight + reactionSignal(reactionWeight, unlockUnits) + repostReachSignal(repostWeight, unlockUnits)
	return roundProfileEngagement(total)
}

// profileRisingEngagement scores one person. A reply or a zap beats a like
// or a repost. Someone whose only reply is a copied phrase stays in that
// pile, unless they also zapped: the zap is then their vote, and the pile
// keeps one vote for whoever is left. Likes and reposts from people already
// counted are not a second vote.
func profileRisingEngagement(replies, reactions, reposts, zaps []weightedReply) float64 {
	original := sumDiscountedReplyWeights(replies)
	replyW := sumDistinctWeightsMap(replies)
	zapW := sumDistinctWeightsMap(zaps)
	repostW := sumDistinctWeightsMap(reposts)
	reactW := sumDistinctWeightsMap(reactions)

	dropped := map[string]struct{}{}
	for pubkey := range original.DiscountedPubkeys {
		if _, zapped := zapW[pubkey]; zapped {
			dropped[pubkey] = struct{}{}
		}
	}
	folded := original
	if len(dropped) > 0 {
		kept := make([]weightedReply, 0, len(replies))
		for _, row := range replies {
			if _, ok := dropped[row.Pubkey]; ok {
				continue
			}
			kept = append(kept, row)
		}
		folded = sumDiscountedReplyWeights(kept)
	}

	// A pile member left alone after a zapper is removed would otherwise
	// become a substantive reply and open likes from the reply side. They
	// stay a pile vote. A zap still opens its own reaction points and
	// repost allowance.
	var leftover float64
	for pubkey, weight := range replyW {
		if _, ok := original.DiscountedPubkeys[pubkey]; !ok {
			continue
		}
		if _, ok := dropped[pubkey]; ok {
			continue
		}
		if _, stillPiled := folded.DiscountedPubkeys[pubkey]; stillPiled {
			continue
		}
		leftover += weight
	}
	if leftover > folded.Substantive {
		leftover = folded.Substantive
	}
	substantiveReplies := folded.Substantive - leftover
	pileVote := math.Max(0, folded.Total-folded.Substantive) + leftover

	var zapWeight, zapHeadcount float64
	for pubkey, weight := range zapW {
		zapWeight += weight
		if _, ok := dropped[pubkey]; ok {
			zapHeadcount += weight
			continue
		}
		if _, replied := replyW[pubkey]; replied {
			continue
		}
		zapHeadcount += weight
	}

	var reactionWeight, repostWeight float64
	for pubkey, weight := range repostW {
		if _, ok := replyW[pubkey]; ok {
			continue
		}
		if _, ok := zapW[pubkey]; ok {
			continue
		}
		repostWeight += weight
	}
	for pubkey, weight := range reactW {
		if _, ok := replyW[pubkey]; ok {
			continue
		}
		if _, ok := zapW[pubkey]; ok {
			continue
		}
		if _, ok := repostW[pubkey]; ok {
			continue
		}
		reactionWeight += weight
	}

	unlockUnits := substantiveReplies*original.UnlockShare + zapWeight
	total := pileVote + substantiveReplies + zapHeadcount + reactionSignal(reactionWeight, unlockUnits) + repostReachSignal(repostWeight, unlockUnits)
	return roundProfileEngagement(total)
}

func sumDistinctWeights(rows []weightedReply) float64 {
	total := 0.0
	for _, weight := range sumDistinctWeightsMap(rows) {
		total += weight
	}
	return total
}

func sumDistinctWeightsMap(rows []weightedReply) map[string]float64 {
	out := make(map[string]float64, len(rows))
	for _, row := range rows {
		if row.Pubkey == "" {
			continue
		}
		if weight, ok := out[row.Pubkey]; !ok || row.Weight > weight {
			out[row.Pubkey] = row.Weight
		}
	}
	return out
}

func roundProfileEngagement(v float64) float64 {
	if v <= 0 || math.IsNaN(v) {
		return 0
	}
	return math.Round(v*1000) / 1000
}
