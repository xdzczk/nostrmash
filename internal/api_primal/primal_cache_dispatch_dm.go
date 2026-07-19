package api_primal

import (
	"context"
	"strings"
	"time"
)

func (g WSGateway) cacheDispatchDirectMessageContacts(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, err
	}
	relation, err := parseDirectMessageContactsRelation(kwargs["relation"])
	if err != nil {
		return nil, err
	}
	limit := toInt(kwargs["limit"], 20)
	// Bound offset to the same ceiling as the feed/thread paths so a hostile or
	// buggy client cannot force pathological deep-OFFSET scans on the DM tables.
	offset := toBoundedNonNegativeInt(kwargs["offset"], 0, 10000)
	since := toInt64(kwargs["since"], 0)
	until := toInt64(kwargs["until"], time.Now().Unix())
	values, err := g.query.GetDirectMessageContactsDetailed(ctx, pubkey, limit, offset, since, until)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return g.buildDirectMessageContactsPayload(ctx, pubkey, relation, values)
}

func (g WSGateway) cacheDispatchDirectMessages(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, err
	}
	peer, _ := kwargs["peer_pubkey"].(string)
	if strings.TrimSpace(peer) == "" {
		peer, _ = kwargs["sender"].(string)
	}
	if err := validatePubkeyHex(peer); err != nil {
		return nil, err
	}
	since := toInt64(kwargs["since"], 0)
	until := toInt64(kwargs["until"], time.Now().Unix())
	limit := toInt(kwargs["limit"], 20)
	offset := toBoundedNonNegativeInt(kwargs["offset"], 0, 10000)
	values, err := g.query.GetDirectMessagesWithRange(ctx, pubkey, peer, since, until, limit, offset)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return g.buildDirectMessagesPayload(ctx, pubkey, peer, values), nil
}

func (g WSGateway) cacheDispatchDirectMessageCount(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, err
	}
	sender, _ := kwargs["sender"].(string)
	if sender != "" {
		if err := validatePubkeyHex(sender); err != nil {
			return nil, err
		}
	}
	count, err := g.query.GetDirectMessageCount(ctx, pubkey, sender)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{buildDirectMessageCountEvent(count)}, nil
}

func (g WSGateway) cacheDispatchDirectMessageCount2(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, err
	}
	sender, _ := kwargs["sender"].(string)
	if sender != "" {
		if err := validatePubkeyHex(sender); err != nil {
			return nil, err
		}
	}
	count, err := g.query.GetDirectMessageCount(ctx, pubkey, sender)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{buildDirectMessageCount2Event(count)}, nil
}

func (g WSGateway) cacheDispatchResetDirectMessageCount(ctx context.Context, kwargs map[string]any) ([]any, error) {
	receiver, sender, err := parseAndValidateDMResetAuth(kwargs)
	if err != nil {
		return nil, err
	}
	if err := g.query.ResetDirectMessageCount(ctx, receiver, sender); err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	if err := g.query.ResetDirectMessageUnread(ctx, receiver, sender); err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{}, nil
}

func (g WSGateway) cacheDispatchResetDirectMessageCounts(ctx context.Context, kwargs map[string]any) ([]any, error) {
	receiver, err := parseAndValidateDMResetAllAuth(kwargs)
	if err != nil {
		return nil, err
	}
	if err := g.query.ResetDirectMessageCounts(ctx, receiver); err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{}, nil
}
