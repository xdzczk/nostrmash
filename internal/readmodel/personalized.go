package readmodel

// CohortNote is one note a viewer's follows engaged with, plus the weights
// needed to score that cohort. Note.Score is the global ranking score.
type CohortNote struct {
	Note           TrendingNote
	CohortScore    float64
	CohortEvidence float64
	ReplyWeight    float64
	RepostWeight   float64
	ReactionWeight float64
	ZapWeight      float64
	ZapMSats       float64
}

// CohortProfile is one profile a viewer's follows engaged with or followed.
// Profile.Score is the global ranking score.
type CohortProfile struct {
	Profile        TrendingProfile
	CohortScore    float64
	CohortEvidence float64
	ViewerFollows  bool
}

// CohortConversation is one thread the viewer's follows replied in.
// Conversation.VelocityScore is the global hot-conversation score.
type CohortConversation struct {
	Conversation       HotConversation
	CohortReplies      float64
	CohortParticipants float64
}

// CohortDomain is one link domain the viewer's follows posted in the window.
type CohortDomain struct {
	Domain        string
	LatestEventAt *int64
	LinkCount     int64
	NoteCount     int64
	UniqueAuthors int64
}
