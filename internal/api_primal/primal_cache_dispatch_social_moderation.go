package api_primal

import "context"

func (g WSGateway) cacheDispatchUserMentions(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	return rawMessagesToAnyMust(g.query.GetMentions(ctx, pubkey, limit))
}

func (g WSGateway) cacheDispatchUserFollowers(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	return rawMessagesToAnyMust(g.query.GetFollowers(ctx, pubkey, limit))
}

func (g WSGateway) cacheDispatchMutelist(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	return g.buildModerationListResponse(ctx, pubkey, moderationListMute)
}

func (g WSGateway) cacheDispatchMutelists(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	return g.buildModerationListResponse(ctx, pubkey, moderationListMutelists)
}

func (g WSGateway) cacheDispatchAllowlist(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	return g.buildModerationListResponse(ctx, pubkey, moderationListAllowlist)
}
